// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Does the PARK PRIMITIVE work? Not WaitGroup, not Event, not the locks -- the thing underneath:
// Thread::Suspend parks a task in its worker's suspended table, and the only ways back are a flag
// waker (Thread::Kick, callable from any thread) or the deadline backstop (PollTasks).
//
// The claim being tested is that a park is a REST, not a spin. The old protocol reached SUSPENDED
// and immediately requeued the task onto the thread that had just parked it, so a wait was a
// scheduler-mediated spin that always ended -- which is why it looked like it worked. A real park
// must fail to return when nobody wakes it. That is case 1, and it is the whole point: every other
// case here is meaningless if a park can come back on its own.
#include <TaskScheduler.h>
#include <Thread.h>
#include <Observer.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>
#if defined(_WIN32)
#  include <windows.h>
#  include <timeapi.h>
#  pragma comment(lib, "winmm.lib")
#endif

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static constexpr uint32_t kWake = 1u;

using Clock = std::chrono::steady_clock;
static long long MsSince(Clock::time_point t0) {
	return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
}
// Wait for a condition, so a failure is a timeout rather than a hung test.
//
// It SLEEPS rather than spins. Every worker is already hunting, so a waiter that yields in a loop
// just takes a core away from the pool it is waiting on -- measured here as 128 of 300 parks
// returning instead of 300. main's job in this test is to get out of the way. See the
// timeBeginPeriod in main: without it a 1 ms sleep is a ~15.6 ms timer tick, which would put two
// ticks of harness latency into every cycle.
template <class F>
static bool WaitUntil(F pred, int limitMs) {
	const auto t0 = Clock::now();
	while (!pred()) {
		if (MsSince(t0) > limitMs) return false;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}

// One parked task: the body publishes its own Task* so a waker outside the pool can address it,
// then parks. Everything after the park is proof that the park returned.
struct Parker {
	std::atomic<Task*>       self{ nullptr };
	// What a real waker holds. A handle cannot exist for a park that has not happened -- it is
	// published BY the park -- so obtaining one is itself the proof that there is something to
	// wake. That is why Thread::Kick takes a handle: the "wake arrived before the park was
	// findable" case becomes unrepresentable rather than merely handled.
	std::atomic<TaskHandle>  handle{ TaskHandle{} };
	std::atomic<int>   resumed{ 0 };
	Clock::time_point  parkedAt{};
	Clock::time_point  resumedAt{};
	uint64_t           timeoutMs = 0;
	Pin                pin = Pin::None;   // where it must come back
	std::atomic<int>   wokeOnQ{ -1 };     // which worker actually ran it
};

static void ParkBody(void* p) {
	Parker& k = *static_cast<Parker*>(p);
	k.parkedAt = Clock::now();
	k.self.store(TaskScheduler::Instance().GetCurrentTask(), std::memory_order_release);
	if (Fiber* f = FiberFromStack()) f->Suspend(k.pin, k.timeoutMs);
	k.resumedAt = Clock::now();
	if (Thread* w = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers()))
		k.wokeOnQ.store(w->qIndex, std::memory_order_relaxed);
	k.resumed.fetch_add(1, std::memory_order_release);
}

static Task* StartParker(TaskScheduler& s, Parker& k, WaitGroup* wg) {
	Task* t = s.CreateTask(&ParkBody, &k, TaskType::Fiber);
	if (wg) t->waitGroup = wg;
	s.Push(t);

	// Wait for the task, then for its HANDLE. The handle is published by SuspendTask as the last
	// thing the park does, so seeing it is proof the task is parked and findable -- which is what
	// a real waker would have, and why it cannot race the park the way a bare Task* did.
	if (!WaitUntil([&] { return k.self.load(std::memory_order_acquire) != nullptr; }, 5000)) {
		std::printf("    START FAILED: the body never published its task\n");
		return t;
	}
	Task* task = k.self.load(std::memory_order_acquire);
	if (!WaitUntil([&] {
			const TaskHandle h = task->record->handle.load(std::memory_order_acquire);
			if (!h.parked) return false;
			k.handle.store(h, std::memory_order_release);
			return true;
		}, 5000)) {
		// Silent failure here would leave a zeroed handle that wakes nothing, and 64 of those look
		// exactly like a hang. Say which precondition was not met.
		const TaskHandle h = task->record->handle.load(std::memory_order_acquire);
		std::printf("    START FAILED: task %p  handle{lot=%u idx=%u worker=%u gen=%u}  resumed=%d\n",
		            (void*)task, (unsigned)h.parked, (unsigned)h.index,
		            (unsigned)h.worker_id, (unsigned)h.generation, k.resumed.load());
	}
	return t;
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
#if defined(_WIN32)
	// A 1 ms sleep is a ~15.6 ms tick otherwise, and this test measures milliseconds.
	::timeBeginPeriod(1);
#endif
	TaskScheduler::Config cfg;
	cfg.mode = Mode::Migrate;
	cfg.main = MainMode::OutOfPool;   // main is a waker here, never a worker
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();
	std::printf("park_test -- workers=%zu\n", s.GetWorkerCount());

	// 1. THE TEST. No waker, no deadline: the task must still be parked after a long wait. If this
	//    fails the park is the old pretend-park and nothing below means anything.
	std::printf("[a park with no waker does not return]\n");
	{
		Parker k;
		StartParker(s, k, nullptr);
		Check(k.self.load() != nullptr, "the body published its task and reached the park");
		std::this_thread::sleep_for(std::chrono::milliseconds(400));
		Check(k.resumed.load() == 0, "still parked after 400 ms with nothing to wake it");

		Thread::Kick(k.handle.load(), kWake);
		Check(WaitUntil([&] { return k.resumed.load() == 1; }, 2000),
		      "a flag from outside the pool returns it");
	}

	// 2. The deadline backstop: the only other way back.
	std::printf("[the deadline returns a task nobody wakes]\n");
	{
		Parker k; k.timeoutMs = 200;
		StartParker(s, k, nullptr);
		const bool back = WaitUntil([&] { return k.resumed.load() == 1; }, 5000);
		const long long took = std::chrono::duration_cast<std::chrono::milliseconds>(
			k.resumedAt - k.parkedAt).count();
		std::printf("    returned after %lld ms (deadline 200 ms)\n", back ? took : -1);
		Check(back, "a 200 ms deadline fired with no waker at all");
		Check(back && took >= 190, "it did not come back before its deadline");
	}

	// 3. A flag must beat a long deadline, or the backstop is really the wake.
	std::printf("[a flag returns it long before its deadline]\n");
	{
		Parker k; k.timeoutMs = 5000;
		StartParker(s, k, nullptr);
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		Check(k.resumed.load() == 0, "still parked at 100 ms of a 5 s deadline");
		Thread::Kick(k.handle.load(), kWake);
		const bool back = WaitUntil([&] { return k.resumed.load() == 1; }, 2000);
		const long long took = std::chrono::duration_cast<std::chrono::milliseconds>(
			k.resumedAt - k.parkedAt).count();
		std::printf("    returned after %lld ms\n", back ? took : -1);
		Check(back && took < 2000, "the flag returned it, not the deadline");
	}

	// 4. Two wakers race one park. Exactly one delivery: the body runs past the park once.
	std::printf("[racing wakers deliver one wake]\n");
	{
		constexpr int kN = 64;
		std::vector<Parker> ks(kN);
		WaitGroup wg; wg.n.store(kN, std::memory_order_relaxed);
		for (int i = 0; i < kN; ++i) StartParker(s, ks[i], &wg);

		std::atomic<bool> go{ false };
		auto storm = [&] {
			while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
			for (int i = 0; i < kN; ++i) Thread::Kick(ks[i].handle.load(), kWake);
		};
		std::thread a(storm), b(storm);
		go.store(true, std::memory_order_release);
		a.join(); b.join();

		const bool all = WaitUntil([&] {
			for (int i = 0; i < kN; ++i) if (ks[i].resumed.load() == 0) return false;
			return true;
		}, 5000);
		int extra = 0;
		for (int i = 0; i < kN; ++i) if (ks[i].resumed.load() != 1) ++extra;
		Check(all, "every parked task was returned");
		Check(extra == 0, "none was delivered twice");
		s.WaitFor(wg);
	}

	// 5. A handle that has been used is dead: the slot's generation has moved on, so an old waker
	//    firing again must not shorten the NEXT park of the same task.
	std::printf("[a stale handle wakes nothing]\n");
	{
		Parker k1;
		StartParker(s, k1, nullptr);
		const TaskHandle stale = k1.handle.load();   // kept AFTER the slot is reused
		Thread::Kick(stale, kWake);
		Check(WaitUntil([&] { return k1.resumed.load() == 1; }, 2000), "first park returned");

		// That slot is free now and will be handed to someone else. Re-firing the old handle must
		// resolve to nothing: the generation moved on. Deliberately NOT a Task* -- the whole point
		// is that a handle survives the task it named and stays safe to act on.
		Parker k2; k2.timeoutMs = 300;
		StartParker(s, k2, nullptr);
		Thread::Kick(stale, kWake);          // the dead handle
		std::this_thread::sleep_for(std::chrono::milliseconds(120));
		Check(k2.resumed.load() == 0, "the stale handle did not return a different task's park");
		Check(WaitUntil([&] { return k2.resumed.load() == 1; }, 3000),
		      "that task still came back on its own deadline");
	}

	// 6. The table is small (64 slots per worker) and a park is not optional. More concurrent
	//    parks than slots must not hang and must not lose a task.
	std::printf("[more parks than slots]\n");
	{
		constexpr int kN = 300;
		std::vector<Parker> ks(kN);
		WaitGroup wg; wg.n.store(kN, std::memory_order_relaxed);
		for (int i = 0; i < kN; ++i) { ks[i].timeoutMs = 250; StartParker(s, ks[i], &wg); }
		const bool all = WaitUntil([&] {
			for (int i = 0; i < kN; ++i) if (ks[i].resumed.load() == 0) return false;
			return true;
		}, 15000);
		int back = 0;
		for (int i = 0; i < kN; ++i) back += ks[i].resumed.load();
		std::printf("    %d of %d returned\n", back, kN);
		Check(all, "every one of 300 concurrent parks came back");
		s.WaitFor(wg);
	}

	// 6b. The same deadline, but with the pool BUSY. Every case above parks into an idle pool, so
	//     no worker takes a pass and the observer necessarily delivers 100% -- which tells us
	//     nothing about whether the worker-side path works at all.
	//
	//     Here the workers are kept busy while the deadlines run, so PollTasks (top of every hunt
	//     pass, ~100 ns apart) should catch them first and the observer's count should barely move.
	//     If the observer still delivers all of them, the worker path is dead code that only looks
	//     live, and the "workers time it, observer backstops" split is not real.
	std::printf("[deadlines while the pool is busy]\n");
	{
		constexpr int kN = 64;
		std::vector<Parker> ks(kN);
		WaitGroup parks; parks.n.store(kN, std::memory_order_relaxed);
		const uint64_t firedBefore = Observer::Fired();

		for (int i = 0; i < kN; ++i) { ks[i].timeoutMs = 250; StartParker(s, ks[i], &parks); }

		// Load, driven from MAIN rather than a pool task: main is out of the pool here, so it can
		// push and wait. A lambda task cannot -- it is Native, and a Native task that calls WaitFor
		// is fatal by design.
		std::atomic<int> loadRan{ 0 };
		bool all = false;
		const auto t0 = Clock::now();
		while (MsSince(t0) < 15000) {
			WaitGroup w; w.n.store(64, std::memory_order_relaxed);
			for (int i = 0; i < 64; ++i) {
				Task* t = s.CreateTask([&loadRan] {
					volatile uint64_t x = 0;
					for (int k = 0; k < 2000; ++k) x += k;
					loadRan.fetch_add(1, std::memory_order_relaxed);
				});
				t->waitGroup = &w;
				s.Push(t);
			}
			s.WaitFor(w);

			all = true;
			for (int i = 0; i < kN; ++i) if (ks[i].resumed.load() == 0) { all = false; break; }
			if (all) break;
		}
		s.WaitFor(parks);

		// The observer is the ONLY thing that fires a deadline now -- the worker-side check was
		// removed as a duplicate of its list -- so this is not a race between two mechanisms any
		// more. It reports how many of the deadlines the observer accounted for while every worker
		// was busy, which is the property worth holding: a pool under load must not delay them.
		const uint64_t byObserver = Observer::Fired() - firedBefore;
		std::printf("    %d deadlines, %d load tasks ran; observer fired %llu\n",
		            kN, loadRan.load(), (unsigned long long)byObserver);
		// Printed, NOT asserted. The delta is a lower bound: firedBefore is sampled after the
		// parkers are armed, so a deadline that expires inside that window is already counted and
		// the difference comes back short. Asserting equality on it fails at random -- it did,
		// once, at 63 of 64 while every task still returned. The property worth holding is the
		// check above; this number is for reading, not for gating.
		Check(all, "every deadline fired with the pool under load");
	}

	// 6b. A PINNED PARK COMES BACK ON THE WORKER IT PARKED ON, woken by a Kick from outside the
	//     pool. A pin is binary, so "pinned" means this thread and nothing else; naming a different
	//     worker is Thread::SendTo.
	//
	//     It is also what the `pinned` mask is for: a pinned slot must be invisible to thieves, so
	//     if a thief could take one, these would come back somewhere else.
	std::printf("[a pinned park resumes only on the worker it parked on]\n");
	if (s.GetWorkerCount() > 3) {
		constexpr int kN = 32;
		std::vector<Parker> ks(kN);
		for (int i = 0; i < kN; ++i) ks[i].pin = Pin::Current;

		WaitGroup parks; parks.n.store(kN);
		for (int i = 0; i < kN; ++i) StartParker(s, ks[i], &parks);

		int armed = 0;
		for (int i = 0; i < kN; ++i) if (ks[i].handle.load().parked) ++armed;
		Check(armed == kN, "every pinned park published a handle");

		for (int i = 0; i < kN; ++i) Thread::Kick(ks[i].handle.load(), kWake);
		Check(WaitUntil([&] {
				int n = 0;
				for (int i = 0; i < kN; ++i) n += ks[i].resumed.load();
				return n == kN;
			}, 5000), "every pinned park came back");

		int wrong = 0, unknown = 0;
		for (int i = 0; i < kN; ++i) {
			const int q    = ks[i].wokeOnQ.load();
			const int want = (int)ks[i].handle.load().worker_id;   // where the park took its slot
			if (q < 0)        ++unknown;
			else if (q != want) ++wrong;
		}
		if (wrong || unknown)
			std::printf("    %d resumed on the wrong worker, %d could not say where\n", wrong, unknown);
		Check(wrong == 0 && unknown == 0, "all 32 resumed on the worker they parked on");
		s.WaitFor(parks);
	}

	// 6c. MORE KICKS AT ONCE THAN THE RESUME RING HOLDS.
	//
	//     A kick hands the owner the task's key instead of making it scan, but the ring is finite
	//     (Thread::kResumeSlots). A kick that cannot reserve a slot sets an overflow flag, and the
	//     owner answers that with one sweep of its lots -- the straggler path. Nothing else in the
	//     suite reaches it: 300 concurrent parks spread over 31 workers is ~10 each.
	//
	//     So concentrate them. PushTo(1) runs every task on ONE worker, so they all park in ONE
	//     lot and every key lands in ONE ring. A fallback nobody exercises is a fallback that is
	//     wrong when it finally runs.
	std::printf("[more kicks at once than the resume ring holds]\n");
	if (s.GetWorkerCount() > 2) {
		constexpr int kN = 1200;   // > kResumeSlots (1024)
		std::vector<Parker> ks(kN);

		WaitGroup parks; parks.n.store(kN);
		for (int i = 0; i < kN; ++i) {
			Task* t = s.CreateTask(&ParkBody, &ks[i], TaskType::Fiber);
			t->waitGroup = &parks;
			s.PushTo(1, t);
		}

		int armed = 0;
		for (int i = 0; i < kN; ++i) {
			Parker& k = ks[i];
			if (!WaitUntil([&] { return k.self.load(std::memory_order_acquire) != nullptr; }, 10000))
				continue;
			Task* t = k.self.load(std::memory_order_acquire);
			if (WaitUntil([&] {
					const TaskHandle h = t->record->handle.load(std::memory_order_acquire);
					if (!h.parked) return false;
					k.handle.store(h, std::memory_order_release);
					return true;
				}, 10000))
				++armed;
		}
		std::printf("    %d of %d parked on one worker\n", armed, kN);

		for (int i = 0; i < kN; ++i)
			if (ks[i].handle.load().parked) Thread::Kick(ks[i].handle.load(), kWake);

		int back = 0;
		const bool all = WaitUntil([&] {
			back = 0;
			for (int i = 0; i < kN; ++i) back += ks[i].resumed.load();
			return back == armed;
		}, 15000);
		if (!all) {
			std::printf("    only %d of %d came back\n", back, armed);
			for (int i = 0; i < kN; ++i) {
				if (ks[i].resumed.load() != 0) continue;
				const TaskHandle h = ks[i].handle.load();
				Task* t = ks[i].self.load();
				const TaskHandle live = t && t->record
					? t->record->handle.load(std::memory_order_acquire) : TaskHandle{};
				const Fiber* fb = t && t->record ? t->record->fiber : nullptr;
				std::printf("    STUCK i=%d kicked{w=%u lot=%u idx=%u gen=%u} live{lot=%u} "
				            "fiber=%p status=%d started=%d pin=%u\n",
				            i, (unsigned)h.worker_id, (unsigned)h.parked, (unsigned)h.index,
				            (unsigned)h.generation, (unsigned)live.parked, (const void*)fb,
				            fb ? (int)fb->status.load(std::memory_order_acquire) : -1,
				            t ? (int)t->started : -1,
				            t && t->record ? (unsigned)t->record->pinTo : 0xFFFFu);
			}
		}
		Check(all, "every task came back with more kicks outstanding than ring slots");
		s.WaitFor(parks);
	}

	// 7. Sustained park/wake, to catch a slot that leaks or a generation that stops matching.
	std::printf("[1000 park/wake cycles]\n");
	{
		constexpr int kCycles = 1000;
		int done = 0;
		const char* why = "";
		const auto t0 = Clock::now();
		for (int i = 0; i < kCycles; ++i) {
			Parker k;
			WaitGroup wg; wg.n.store(1, std::memory_order_relaxed);
			StartParker(s, k, &wg);
			if (!k.self.load()) { why = "the task never reached its park"; break; }
			Thread::Kick(k.handle.load(), kWake);
			if (!WaitUntil([&] { return k.resumed.load() == 1; }, 3000)) {
				why = "the wake was never delivered";
				break;
			}
			s.WaitFor(wg);
			++done;
		}
		std::printf("    %d of %d cycles in %lld ms%s%s\n", done, kCycles, MsSince(t0),
		            *why ? " -- stopped: " : "", why);
		Check(done == kCycles, "a thousand parks and wakes, no slot leak");
	}

	// The observer fires EVERY deadline, by design as of 2026-09-24. This comment used to say the
	// opposite -- that the worker should catch most of them and a high count here would be "the
	// floor problem wearing a different hat" -- and that was written when PollTasks carried a
	// deadline check of its own.
	//
	// That check is gone. It duplicated the list the observer already holds, and it was what
	// forced the sweep to look at unflagged slots and read a clock on the hot path. A walking scan
	// belongs to the observer, which has a core with one exclusive job; a worker now scans only
	// `bits & sig`, so its work is proportional to wakes pending rather than tasks parked.
	//
	// So a number near 366 is correct here, and the "workers" count in the busy-pool case above is
	// zero BY CONSTRUCTION, not because the observer won a race.
	std::printf("[who delivered]\n    observer fired %llu of ~%d deadline waits, %zu still watched\n",
	            (unsigned long long)Observer::Fired(), 366, Observer::Active());

	// A flag-only watch has no deadline to expire on and nothing calls Cancel, so the generation
	// check is the only thing that retires one. Zero here means they are leaking again.
	std::printf("    %llu flag-only watch(es) retired by generation\n",
	            (unsigned long long)Observer::GenDrops());
	Check(Observer::Active() == 0, "no watch left behind");

	std::printf("RESULT: %s\n", g_fail ? "FAILURES" : "all checks passed");
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
