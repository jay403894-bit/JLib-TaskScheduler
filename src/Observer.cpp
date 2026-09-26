// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Observer.h"
#include "../include/WaitRecord.h"
#include "../include/Fiber.h"
#include "../include/Thread.h"
#include "../include/TaskScheduler.h"
#include "../include/Clock.h"

#include <chrono>
#include <thread>
#include <vector>
#include <cstdio>

namespace JLib {
namespace {

	// Many producers -- any thread may register -- and exactly ONE consumer, the observer loop.
	// The single consumer is required by the algorithm (tail_ is plain consumer-owned state) and
	// is also what lets the active set below be unshared and lock-free.
	ObserverQueue         g_inbox;

	// A POINTER, never a global std::thread object. A joinable std::thread whose destructor runs
	// calls std::terminate, and a program is free to exit without calling Join() -- at which point
	// a global would abort the process during teardown for no reason. Leaking the handle lets the
	// OS reap the thread at exit; Stop() joins it properly when teardown is orderly.
	std::thread*          g_thread = nullptr;
	std::atomic<bool>     g_running{ false };
	std::atomic<uint64_t> g_fired{ 0 };
	std::atomic<size_t>   g_active{ 0 };

	std::atomic<uint64_t> g_genDrop{ 0 };   // flag-only watches retired by the generation check

	// Pacing. The observer parks when it has nothing to watch and is woken by Register -- the
	// registration is the event, so it produces a wake rather than being discovered by a poll.
	std::atomic<int>      g_wake{ kWaitRunning };
	std::atomic<uint64_t> g_lastRegisterMs{ 0 };

	// Stay hot this long after the last registration before parking: a burst must not park and
	// wake on every item. Same hysteresis as the worker's kHotPasses.
	constexpr uint64_t kCooldownMs  = 500;
	// A deadline this close is polled rather than slept on. Polling the clock is what beats the
	// 1 ms wheel, and this is the only place that accuracy is spent.
	constexpr uint64_t kSpinWithinMs = 2;

	// The clock deadlines are armed with: Thread::GetCurrentTimeMs computes now + delay from
	// CoarseMs. Millisecond deadlines and the timer wheel's MonotonicNs are separate epochs and are
	// never compared.
	uint64_t NowMs() noexcept { return (uint64_t)JLib::CoarseMs(); }

	void Loop() {
		// The observer's own array: nothing else touches it, so no locks and no atomics. The
		// records themselves live on their fibers; this holds a pointer plus the seq it was
		// registered with, which is how a cancelled or re-registered watch is recognised.
		struct Entry { WaitRecord* r; uint64_t seq; };
		std::vector<Entry> active;
		active.reserve(256);

		// Handles that fired this pass, split by reason because a batch shares one flag value.
		// Reused across passes so a firing burst never allocates on this thread.
		std::vector<TaskHandle> flagFired, deadlineFired;
		flagFired.reserve(64);
		deadlineFired.reserve(64);

		while (g_running.load(std::memory_order_acquire)) {
			// 1. Drain the inbox. A false from pop can mean "empty" OR "a producer is mid-push",
			//    which is fine here: the next pass picks it up, and nothing has a deadline so tight
			//    that one pass matters.
			WaitRecord* r = nullptr;
			while (g_inbox.pop(r)) active.push_back(Entry{ r, r->seq.load(std::memory_order_acquire) });

			// 2. One clock read for the whole pass.
			const uint64_t now = NowMs();

			// 3. Anything due goes back to its OWNER. The observer never delivers, so it cannot
			//    race the owner's PollTasks into a second wake.
			for (size_t i = 0; i < active.size(); ) {
				Entry&      en = active[i];
				WaitRecord* e  = en.r;

				// Cancelled or re-registered: drop it without waking anything and without touching
				// e->flag, which an unregistered watch may already have destroyed.
				if (e->seq.load(std::memory_order_acquire) != en.seq) {
					active[i] = active.back();
					active.pop_back();
					continue;
				}

				// Flag-only watches only: a generation that no longer matches means the park ended,
				// so the watch drops with nothing woken. A timed watch leaves by expiring, and
				// asking about those costs more than it retires.
				if (e->deadlineMs == 0 && !Thread::ParkLive(e->handle)) {
					g_genDrop.fetch_add(1, std::memory_order_relaxed);
					active[i] = active.back();
					active.pop_back();
					continue;
				}

				// The flag is read ONLY here, after the stale check above -- so a cancelled watch's
				// address is never touched, which is the whole basis of the outlive-the-watch rule.
				const bool flagged = e->flag
				                  && (e->flag->load(std::memory_order_acquire) & e->mask) != 0;
				const bool expired = e->deadlineMs != 0 && now >= e->deadlineMs;

				if (flagged || expired) {
					// COLLECTED, NOT KICKED YET. Several watches on one worker routinely fire in
					// the same pass, and each Kick would wake that worker again -- so they are
					// grouped and sent once below. Kicks are split by reason because flags say WHY
					// a task woke and a batch shares one value.
					(flagged ? flagFired : deadlineFired).push_back(e->handle);
					g_fired.fetch_add(1, std::memory_order_relaxed);
					active[i] = active.back();
					active.pop_back();
					continue;
				}
				++i;
			}
			// ONE WAKE PER WORKER, not one per watch. Sent after the scan so every handle that
			// fired this pass is in hand and can be grouped by the worker that owns it.
			if (!deadlineFired.empty()) {
				Thread::KickBatch(deadlineFired.data(), deadlineFired.size(), Observer::kDeadlineFired);
				deadlineFired.clear();
			}
			if (!flagFired.empty()) {
				Thread::KickBatch(flagFired.data(), flagFired.size(), Observer::kFlagFired);
				flagFired.clear();
			}

			g_active.store(active.size(), std::memory_order_relaxed);

			// 4. Pace. Three states, and the cost is spent only where accuracy is actually needed.
			uint64_t nearest = 0;         // 0 = no deadline among the active set
			bool     holdsFlag = false;
			for (const Entry& en : active) {
				if (en.r->flag) holdsFlag = true;
				const uint64_t d = en.r->deadlineMs;
				if (d != 0 && (nearest == 0 || d < nearest)) nearest = d;
			}

			if (active.empty()) {
				// Nothing to watch. Stay hot briefly in case another registration is right behind
				// this one -- same hysteresis as the worker's kHotPasses, and for the same reason:
				// a burst must not park and wake on every item.
				if (NowMs() - g_lastRegisterMs.load(std::memory_order_relaxed) < kCooldownMs) {
					std::this_thread::yield();
				} else {
					// PARK. Register kicks this word, so a new watch wakes it immediately.
					int r = kWaitRunning;
					if (g_wake.compare_exchange_strong(r, kWaitWaiting,
							std::memory_order_seq_cst, std::memory_order_seq_cst)) {
						if (g_inbox.empty() && g_running.load(std::memory_order_acquire))
							BlockOnWaitWord(&g_wake);          // recheck before blocking
						else
							g_wake.store(kWaitRunning, std::memory_order_seq_cst);
					}
					g_wake.store(kWaitRunning, std::memory_order_seq_cst);
				}
			}
			else if (holdsFlag) {
				// A flag change is not an event this thread can be woken by -- nothing kicks the
				// word when a bit flips. While any flag watch is held the observer must stay awake
				// to see it, whatever the deadlines say.
				std::this_thread::yield();
			}
			else if (nearest != 0 && nearest - now > kSpinWithinMs) {
				// Deadlines, but none imminent: sleep, bounded by the nearest one.
				const uint64_t ms = nearest - now - kSpinWithinMs;
				std::this_thread::sleep_for(std::chrono::milliseconds(ms > 50 ? 50 : ms));
			}
			else {
				std::this_thread::yield();   // imminent: poll the clock, this is the accuracy case
			}
		}
	}
}   // namespace

bool Observer::Register(Task* task, uint64_t deadlineMs,
                        const std::atomic<uint64_t>* flag, uint64_t mask) noexcept {
	if (!task || !task->record || !g_running.load(std::memory_order_acquire)) return false;
	Fiber* f = task->record->fiber;
	if (!f) return false;

	// The record lives on the fiber and is reused: a fiber is parked at most once at a time, so
	// filling it in here cannot disturb a registration that is still live. Bumping seq LAST also
	// cancels any previous watch on this fiber, so a stale entry cannot resurrect.
	WaitRecord& r = f->waitRecord;
	r.handle     = task->record->handle.load(std::memory_order_acquire);
	r.flag       = flag;
	r.mask       = mask;
	// Already published by the park; passed in so the two cannot disagree.
	r.deadlineMs = deadlineMs;
	r.seq.fetch_add(1, std::memory_order_acq_rel);
	g_inbox.push(&r);

	// The registration IS the wake. Without this the observer would have to discover new work by
	// polling, which is the thing it exists to stop everyone else doing.
	g_lastRegisterMs.store(NowMs(), std::memory_order_relaxed);
	KickWaitWord(&g_wake);
	return true;
}

void Observer::Cancel(Task* task) noexcept {
	if (!task || !task->record) return;
	Fiber* f = task->record->fiber;
	if (!f) return;
	// One relaxed bump, callable from any thread. The observer drops the entry on its next pass
	// without reading the flag -- which is what makes "unregister before you destroy it" a rule a
	// caller can actually keep.
	f->waitRecord.seq.fetch_add(1, std::memory_order_acq_rel);
}


void Observer::Start() {
	if (g_running.exchange(true, std::memory_order_acq_rel)) return;
	g_thread = new std::thread(&Loop);
}

void Observer::Stop() {
	if (!g_running.exchange(false, std::memory_order_acq_rel)) return;
	KickWaitWord(&g_wake);   // it may be parked; the flag alone would not reach it
	if (g_thread) {
		if (g_thread->joinable()) g_thread->join();
		delete g_thread;
		g_thread = nullptr;
	}
	// Drain what is left: the pool is going down, so every remaining wait is either already
	// delivered or about to be torn down with its task.
	WaitRecord* drop = nullptr;
	while (g_inbox.pop(drop)) {}
	g_active.store(0, std::memory_order_relaxed);
}

// (moved above Stop)

bool     Observer::Running() noexcept { return g_running.load(std::memory_order_acquire); }
uint64_t Observer::Fired()   noexcept { return g_fired.load(std::memory_order_relaxed); }
size_t   Observer::Active()  noexcept { return g_active.load(std::memory_order_relaxed); }
uint64_t Observer::GenDrops()         noexcept { return g_genDrop.load(std::memory_order_relaxed); }


// ---------------------------------------------------------------------------------------------
// THE WATCHDOG. A second thread, started only when Config::watchdog is set. See Observer.h for
// why it is not the observer itself, and why it touches nothing in the pool.
// ---------------------------------------------------------------------------------------------
namespace {
	MPSCQueue<Watchdog::HookEntry> g_hooks;
	std::thread*          g_wdThread = nullptr;
	std::atomic<bool>     g_wdRunning{ false };
	std::atomic<uint64_t> g_wdRan{ 0 };
	std::atomic<int>      g_wdWake{ kWaitRunning };

	void WatchdogLoop() {
		while (g_wdRunning.load(std::memory_order_acquire)) {
			Watchdog::HookEntry* e = nullptr;
			bool ranAny = false;
			while (g_hooks.pop(e)) {
				if (e && e->fn) {
					// The hook owns this call entirely. Nothing here holds a lock, so a hook that
					// hangs stalls only the watchdog -- which is the correct blast radius for a
					// diagnostic, and why the pool must never wait on this thread.
					e->fn(e->ctx);
					g_wdRan.fetch_add(1, std::memory_order_relaxed);
				}
				ranAny = true;
			}
			if (ranAny) continue;

			// PARK. Push kicks this word. No cooldown spin like the observer's: hooks arrive when
			// a human or a fault handler asks for them, not in bursts, and nothing here is
			// latency-critical -- it is already too late to be fast.
			int r = kWaitRunning;
			if (g_wdWake.compare_exchange_strong(r, kWaitWaiting,
					std::memory_order_seq_cst, std::memory_order_seq_cst)) {
				if (g_hooks.empty() && g_wdRunning.load(std::memory_order_acquire))
					BlockOnWaitWord(&g_wdWake);
				else
					g_wdWake.store(kWaitRunning, std::memory_order_seq_cst);
			}
			g_wdWake.store(kWaitRunning, std::memory_order_seq_cst);
		}
	}
}

bool Watchdog::Push(HookEntry* e) noexcept {
	if (!e || !e->fn) return false;
	if (!g_wdRunning.load(std::memory_order_acquire)) return false;
	g_hooks.push(e);
	KickWaitWord(&g_wdWake);
	return true;
}

void Watchdog::Start() {
	if (g_wdRunning.exchange(true, std::memory_order_acq_rel)) return;
	g_wdThread = new std::thread(&WatchdogLoop);   // leaked handle, same reason as the observer's
}

void Watchdog::Stop() {
	if (!g_wdRunning.exchange(false, std::memory_order_acq_rel)) return;
	KickWaitWord(&g_wdWake);
	if (g_wdThread) {
		if (g_wdThread->joinable()) g_wdThread->join();
		delete g_wdThread;
		g_wdThread = nullptr;
	}
	// Anything still queued is dropped, not run: the pool is going down, so a hook that walks it
	// would be reading structures mid-teardown.
	HookEntry* drop = nullptr;
	while (g_hooks.pop(drop)) {}
}

bool     Watchdog::Running() noexcept { return g_wdRunning.load(std::memory_order_acquire); }
uint64_t Watchdog::Ran()     noexcept { return g_wdRan.load(std::memory_order_relaxed); }

}   // namespace JLib
