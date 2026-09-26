// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Hazard.h"  
#include "../include/Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include "../include/Timer.h"
#include "../include/Clock.h"
#include "../include/TaskLocal.h"
#include "../include/Observer.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <cstring>   
#include <cstdlib>
#include <utility>   

#if JLIB_PLATFORM_WINDOWS
// WaitOnAddress/WakeByAddress live here. Named in the source because a static lib does not
// carry link dependencies: without it the first EXE that links Scheduler.lib fails.
#pragma comment(lib, "Synchronization.lib")
#endif
#if !JLIB_PLATFORM_WINDOWS
#include <sys/resource.h>
#endif
#if JLIB_PLATFORM_LINUX
#include <sys/syscall.h>        
#include <unistd.h>
#include <linux/futex.h>        
#include <cerrno>

namespace {
	inline void FutexWait(std::atomic<int>* addr, int expected) noexcept {
		::syscall(SYS_futex, reinterpret_cast<int*>(addr), FUTEX_WAIT_PRIVATE, expected,
			nullptr, nullptr, 0);
	}
	inline void FutexWakeOne(std::atomic<int>* addr) noexcept {

		::syscall(SYS_futex, reinterpret_cast<int*>(addr), FUTEX_WAKE_PRIVATE, 1,
			nullptr, nullptr, 0);
	}
}
#endif

using namespace JLib;
thread_local Thread* Thread::instance = nullptr;

namespace {
	// Only main pumps, so a plain flag. Set while the app's pump runs: a window procedure that
	// calls WaitFor lands back in Worker(), and pumping again from in there would re-enter the
	// app's message loop from inside its own dispatch.
	bool g_inPump = false;

	void PumpMain(void (*pump)(std::uint32_t), std::uint32_t budgetUs) {
		if (g_inPump) return;
		struct Reset { ~Reset() { g_inPump = false; } } reset;   // a pump that throws still clears it
		g_inPump = true;
		pump(budgetUs);
	}
}

#if defined(JLIBSCHED_STATS)
namespace {
	// A task is about to run: its first-run time, and how long it was off-CPU since it suspended.
	std::uint64_t StatRunBegin(Task* t) noexcept {
		const std::uint64_t now = JLIB_STAT_TICKS();
		if (TaskRecord* r = t->record) {
			if (!r->statFirstRun) r->statFirstRun = now;
			if (r->statSuspendAt) { JLIB_STAT_HIST(Suspended, now - r->statSuspendAt); r->statSuspendAt = 0; }
		}
		return now;
	}
	void StatRunEnd(std::uint64_t t0) noexcept { JLIB_STAT_HIST(TaskSegment, JLIB_STAT_TICKS() - t0); }
	void StatTaskDone(TaskRecord* r) noexcept {
		if (r && r->statFirstRun) JLIB_STAT_HIST(TaskLife, JLIB_STAT_TICKS() - r->statFirstRun);
	}
}
#endif

namespace {

	enum class WorkerPrio : int { Normal = 0, High = 1, Critical = 2 };

	void ApplyWorkerPriority(WorkerPrio p) noexcept {
#if JLIB_PLATFORM_WINDOWS

		::SetThreadPriority(::GetCurrentThread(),
			p == WorkerPrio::Critical ? THREAD_PRIORITY_TIME_CRITICAL
			: p == WorkerPrio::High ? THREAD_PRIORITY_HIGHEST
			: THREAD_PRIORITY_NORMAL);

#elif JLIB_PLATFORM_LINUX

		(void)syscall(SYS_setpriority, PRIO_PROCESS, (int)syscall(SYS_gettid),
			p == WorkerPrio::Critical ? -10 : p == WorkerPrio::High ? -5 : 0);
#else
		(void)p;
#endif
	}

	void ApplyPowerThrottling(TaskScheduler::PowerThrottling p) noexcept {
#if JLIB_PLATFORM_WINDOWS
#ifdef THREAD_POWER_THROTTLING_CURRENT_VERSION
		THREAD_POWER_THROTTLING_STATE s{};
		s.Version = THREAD_POWER_THROTTLING_CURRENT_VERSION;

		switch (p) {
		case TaskScheduler::PowerThrottling::OptOut:
			s.ControlMask = THREAD_POWER_THROTTLING_EXECUTION_SPEED;
			s.StateMask = 0;
			break;
		case TaskScheduler::PowerThrottling::Force:
			s.ControlMask = THREAD_POWER_THROTTLING_EXECUTION_SPEED;
			s.StateMask = THREAD_POWER_THROTTLING_EXECUTION_SPEED;
			break;

		case TaskScheduler::PowerThrottling::Topology:
		case TaskScheduler::PowerThrottling::SystemManaged:
		default:
			s.ControlMask = 0;
			s.StateMask = 0;
			break;
		}
		(void)::SetThreadInformation(::GetCurrentThread(), ThreadPowerThrottling, &s, sizeof(s));
#else

		(void)p;
#endif
#else
		(void)p;
#endif
	}
}

#if !JLIB_PLATFORM_WINDOWS
#include <sched.h>
#include <pthread.h>

static inline void BindThreadToMask(pthread_t handle, const topology::CpuMask & mask)
{
	cpu_set_t set;
	CPU_ZERO(&set);
	for (unsigned c = 0; c < topology::CpuMask::kMaxCpus; ++c)
		if (mask.Test(c) && c < CPU_SETSIZE) CPU_SET(c, &set);
#if defined(__BIONIC__)
	(void)sched_setaffinity(pthread_gettid_np(handle), sizeof(cpu_set_t), &set);
#else
	(void)pthread_setaffinity_np(handle, sizeof(cpu_set_t), &set);
#endif
}
#endif

namespace {
	// One lot, block allocated and never moved. Never moved is the point: a live TaskHandle names
	// {worker, lot, index, generation} and is read concurrently from any thread, so relocating a
	// lot would leave every resumer and the observer pointing at freed memory -- and the generation
	// check could not notice, because the memory it would read is gone.
	JLib::ParkingLot* NewLot(JLib::Thread* owner) noexcept {
		void* mem = owner->AllocAligned(sizeof(JLib::ParkingLot), kAlign);
		if (!mem) return nullptr;
		return ::new (mem) JLib::ParkingLot();   // bits = 0: every slot free, nothing else to seed
	}

	// Lowest set bit, and popcount. One instruction each; the fallbacks keep the portable path.
	inline int LowestBit64(uint64_t v) noexcept {
#if defined(_MSC_VER) && !defined(__clang__)
		unsigned long i; _BitScanForward64(&i, v); return (int)i;
#else
		return __builtin_ctzll(v);
#endif
	}
	inline int PopCount64(uint64_t v) noexcept {
#if defined(_MSC_VER) && !defined(__clang__)
		return (int)__popcnt64(v);
#else
		return __builtin_popcountll(v);
#endif
	}
}

Thread::Thread(TaskScheduler & scheduler) : scheduler(&scheduler) {
	std::memset(&schedulerCtx, 0, sizeof(Context));
	static_assert(kAlign <= 64, "bits is a single uint64_t: one bit per slot");
	// Lot 0 is a member, so it needs no allocation and cannot fail. Publish it the same way
	// OpenLot does -- lot before count -- so a resumer resolving lots[0] sees a built lot.
	lots[0].store(&lot0, std::memory_order_release);
	lotCount.store(1, std::memory_order_release);
}
Thread::~Thread() {
	const size_t n = lotCount.load(std::memory_order_acquire);
	// From 1: lot 0 is a member and is destroyed with this object.
	for (size_t i = 1; i < n; ++i) {
		if (ParkingLot* lot = lots[i].load(std::memory_order_acquire)) {
			lot->~ParkingLot();
			mi_free(lot);
		}
	}
}

// Owner only. Opens one more lot and marks it as having room.
bool Thread::OpenLot() noexcept {
	const size_t n = lotCount.load(std::memory_order_relaxed);
	if (n >= kMaxLots) return false;
	ParkingLot* next = NewLot(this);
	if (!next) return false;
	// Release, and the lot BEFORE the count: a resumer resolving lots[n] must see a built lot.
	lots[n].store(next, std::memory_order_release);
	lotCount.store(n + 1, std::memory_order_release);
	return true;
}

// Owner only. Reserves a slot in the earliest lot with room and returns the 1-based lot number the
// handle carries; setting the bit is the reservation. Grows while a lot still has
// kGrowWhenFreeBelow free, so the allocation never lands on the park that needs it -- a park
// cannot fail once its caller is committed to switching out.
size_t Thread::TakeSlot(ParkingLot * &outLot, int& outIndex) noexcept {
	const size_t n = lotCount.load(std::memory_order_relaxed);

	for (size_t i = 0; i < n; ++i) {
		ParkingLot* lot = lots[i].load(std::memory_order_relaxed);
		if (!lot) continue;
		// The OWNER is the only one that picks a slot, so choosing si needs no RMW -- but a RESUMER
		// frees one (Thread::ResumeTask), so setting it must not clobber a concurrent clear of a
		// different bit. fetch_or touches only our bit. Uncontended, on this thread's own line.
		const uint64_t b = lot->bits.load(std::memory_order_relaxed);
		if (b == ~0ULL) continue;                       // full

		const int si = LowestBit64(~b);                 // the free slots ARE ~bits
		lot->bits.fetch_or(1ULL << si, std::memory_order_relaxed);
		outIndex = si;
		outLot = lot;

		if (i + 1 == n && (64 - PopCount64(b | (1ULL << si))) <= kGrowWhenFreeBelow) (void)OpenLot();
		return i + 1;                                   // 1-based: 0 means "not parked"
	}

	if (!OpenLot()) return 0;                           // every lot full and at kMaxLots
	ParkingLot* lot = lots[n].load(std::memory_order_relaxed);
	if (!lot) return 0;
	const uint64_t b = lot->bits.load(std::memory_order_relaxed);
	if (b == ~0ULL) return 0;
	const int si = LowestBit64(~b);
	lot->bits.fetch_or(1ULL << si, std::memory_order_relaxed);
	outIndex = si;
	outLot = lot;
	return n + 1;
}
// The kernel's coarse clock, 1 ms granularity (Clock.h). Deadlines are armed in ms.
uint64_t Thread::GetCurrentTimeMs() { return (uint64_t)JLib::CoarseMs(); }
// Owner only. Takes a slot, publishes the handle, and arms the deadline if the wait named one.
static inline void ReleaseSig(JLib::ParkingLot * lot, int index, JLib::Thread * owner = nullptr) noexcept {
	const uint64_t bit = 1ULL << index;
	const uint64_t was = lot->sig.fetch_and(~bit, std::memory_order_relaxed);
	// Only a slot that WAS signalled and takeable comes off the thief counters.
	if ((was & bit) && !(lot->pinned.load(std::memory_order_relaxed) & bit)) {
		lot->stealableSize.fetch_sub(1, std::memory_order_relaxed);
		if (owner) owner->stealableTotal.fetch_sub(1, std::memory_order_relaxed);
	}
}

bool Thread::SuspendTask(Task * task, uint64_t timeoutMs) {
	if (!task || !task->record) return false;

	// Only a pool worker can hold a park: the handle's worker_id indexes `workers`, and main's
	// helper (qIndex -1) is not in it, so every waker would bounce off the bounds check and the task
	// would never come back. Refusing is supported -- OnFiberReturned's !took path requeues.
	if (!IsPoolWorker()) return false;

	// One call gives the lot, the slot and the 1-based lot number the handle carries -- no search
	// on the way in, and no second pass to recover the number.
	ParkingLot* lot = nullptr;
	int         index = 0;
	const size_t lotNo = TakeSlot(lot, index);
	if (lotNo == 0) return false;

	lot->flags[index].store(0, std::memory_order_relaxed);
	// Cleared on EVERY park, so a bit left by the slot's previous occupant can never be read as a
	// wake for this one -- and a fresh park is NOT ready, it is waiting for consent.
	ReleaseSig(lot, (int)index, this);

	// Unconditional, so the slot's previous occupant cannot leave a pin behind. Owner-only, so
	// load/set/store with no RMW.
	{
		const uint64_t bit = 1ULL << index;
		const uint64_t p = lot->pinned.load(std::memory_order_relaxed);
		lot->pinned.store(task->record->pinTo != TaskRecord::kNoPin ? (p | bit) : (p & ~bit),
			std::memory_order_relaxed);
	}

	lot->parked[index].store(task, std::memory_order_release);
	++parkedCount;   // the bit was set by TakeSlot: that IS the reservation

	// The handle goes out LAST: until it does, no resumer can find this task, which is what keeps
	// the slot's contents from being read half-written.
	TaskHandle h;
	h.worker_id = (uint16_t)qIndex;
	h.index = (uint8_t)index;
	h.parked = (uint8_t)lotNo;
	h.generation = lot->gen[index].load(std::memory_order_relaxed);
	task->record->handle.store(h, std::memory_order_release);
	return true;
}
// Direct delivery bypasses ResumeFiber, so the pin is checked here, after the claim, against the
// live record->pinTo. ParkingLot::pinned is a snapshot taken at park time and can be stale; this is
// what makes a stale bit route correctly rather than run on the wrong thread.
bool Thread::CanRunHere(const Task * task) const {
	const uint16_t pin = task->record->pinTo;
	if (pin == TaskRecord::kNoPin) return true;
	if (pin == Pin::kMain)         return isMain;
	return qIndex >= 0 && pin == (uint16_t)qIndex;
}

// The owner's path: pop a key it was handed, claim through the lot CAS, run it. No scan. Losing the
// CAS means a thief got there first; the bit is released and the next key is tried. The slot is
// reclaimed either way. Returns the first task this thread may run; others claimed are queued.
Task* Thread::DrainResumes() {
	Task* direct = nullptr;
	uint32_t r = resumeRead.load(std::memory_order_relaxed);
	bool stalled = false;

	for (;;) {
		const uint32_t w = resumeWrite.load(std::memory_order_acquire);

		while (r != w) {
			// Masked, like PushResume: both counters free-run and the ring wraps under them.
			std::atomic<TaskHandle>* c =
				resumeChunks[(r & kResumeIndexMask) >> kResumeChunkShift].load(std::memory_order_acquire);
			if (!c) { stalled = true; break; }

			const TaskHandle h = c[r & kResumeSlotMask].load(std::memory_order_acquire);
			if (!h.parked) { stalled = true; break; }

			// PIN IS READABLE BEFORE THE CLAIM, off the lot rather than the task. A pinned extra
			// has nowhere to go once claimed -- ClaimByHandle bumps the generation, so the handle
			// cannot be put back -- so decide here, while the entry is still live in the ring.
			if (direct && h.parked <= kMaxLots && h.index < kAlign) {
				if (ParkingLot* pl = lots[h.parked - 1].load(std::memory_order_acquire)) {
					if (pl->pinned.load(std::memory_order_relaxed) & (1ULL << h.index)) {
						stalled = true;          // leave it: the next pass takes it as its direct
						break;
					}
				}
			}

			c[r & kResumeSlotMask].store(TaskHandle{}, std::memory_order_relaxed);
			++r;

			Task* t = ClaimByHandle(h);
			if (!t) continue;

			if (t->type == TaskType::Coroutine) {
				scheduler->WakeTask(t);
				continue;
			}

			Fiber* f = t->record ? t->record->fiber : nullptr;
			if (!f || f->ClaimForWake() != Fiber::ClaimResult::Claimed)
				continue;

			// A RESUME DOES NOT ROUTE. It lands on the thread the task parked on, which is the only
			// thread its handle can name. Coming back somewhere else is Thread::SendTo/SendToMain --
			// the task suspends, wakes here, and kicks itself across. So a resume this worker may not
			// run means a suspend carried a pin that can switch threads, which is not a thing.
			if (!CanRunHere(t)) {
				std::fprintf(stderr,
					"[JLib::Scheduler] FATAL: worker %d was handed a resume it may not run\n",
					qIndex);
				std::abort();
			}

			if (!direct) {
				direct = t;
				continue;
			}

			// Second and later: unpinned by construction, because a pinned extra was left in the
			// ring above rather than claimed. This worker owns the deque, so push_bottom is legal
			// and the task stays stealable.
			if (!scheduler->deques[qIndex]->push_bottom(t))
				TaskDeque::FatalPushRefused();
		}

		if (stalled) break;

		// No reset to zero any more. That was the bug: it only fired when a drain caught up exactly
		// and won the CAS, and every stalled pass skipped it, so the write index climbed forever.
		// The counters wrap on their own now, and `r != w` stays correct across the wrap because
		// the ring holds far fewer than 2^31 entries.
		if (resumeWrite.load(std::memory_order_acquire) == w) break;
	}

	resumeRead.store(r, std::memory_order_release);
	return direct;
}
// Lot stealing removed: a kick hands the owner the key (PushResume/DrainResumes), so nothing
// else ever searched a lot. The scan cost every hunter atomics across other workers' lots to
// shave latency off work that was already addressed to one thread.


// Slot back. Owner only -- a resumer claims the task and leaves the slot, which is what keeps the
// allocation side free of atomics entirely.
void Thread::release(ParkingLot * lot, int index) {
	if (!lot || index < 0 || index >= (int)kAlign) return;
	// Clearing the bit IS the free. RMW, not load/clear/store: a resumer frees its own slot from
	// another thread, and a plain store here would put back a bit it had just cleared.
	lot->bits.fetch_and(~(1ULL << index), std::memory_order_relaxed);
	if (parkedCount > 0) --parkedCount;
}

// Resumer side: take the task out of its slot, and say whether THIS caller is the one that got it.
// Exactly one caller can win the exchange, so exactly one wake is delivered; the slot index is
// reclaimed by its owner on the next poll. Returns false if the task was not parked, or someone
// else already claimed it.
bool Thread::ResumeTask(Task * task) {
	if (!task || !task->record) return false;
	const TaskHandle h = task->record->handle.load(std::memory_order_acquire);
	if (!h.parked || h.parked > kMaxLots || h.index >= kAlign) return false;

	ParkingLot* lot = lots[h.parked - 1].load(std::memory_order_acquire);
	if (!lot) return false;

	Task* expected = task;
	if (!lot->parked[h.index].compare_exchange_strong(expected, nullptr,
		std::memory_order_acq_rel))
		return false;

	lot->gen[h.index].fetch_add(1, std::memory_order_acq_rel);   // stale handles stop resolving
	lot->flags[h.index].store(0, std::memory_order_relaxed);
	ReleaseSig(lot, (int)h.index);   // claimed: the consent is spent

	// THE SLOT GOES BACK HERE, not on the owner's next sweep. It used to be left for PollTasks to
	// reclaim, and with no sweeper left that is a leak: every unpinned wake burned a slot until the
	// lots filled, StoreSuspended started refusing, and the waits fell back to spinning.
	// fetch_and rather than release(): parkedCount is the owner's own plain counter and this is not
	// the owner. Losing a count is a stat; putting back a freed bit is a lost slot.
	lot->bits.fetch_and(~(1ULL << h.index), std::memory_order_relaxed);
	task->record->handle.store(TaskHandle{}, std::memory_order_release);
	// No Observer::Cancel here either -- see the note in PollTasks: measured worse on both axes.
	return true;
}

// Sweep one worker's lots for slots that have been kicked. victim == this is the owner sweep;
// anything else is a thief. Returns one task for the caller to run, or null.
//
// Work here is proportional to wakes PENDING, not to tasks parked: `sig` says which slots have one.
// Nothing dereferences the task until the slot CAS claims it -- a resumer can finish and recycle it
// between any load and any use, so every word the decision needs lives in the lot, which never
// moves and is never freed.
Task* Thread::PollTasks(Thread * victim) {
	if (!victim) return nullptr;
	const bool   owner = (victim == this);
	const size_t nLots = victim->lotCount.load(std::memory_order_acquire);
	Task* direct = nullptr;

	for (size_t li = 0; li < nLots && !direct; ++li) {
		ParkingLot* lot = victim->lots[li].load(std::memory_order_acquire);
		if (!lot) continue;

		// One load skips a whole lot. The owner does not consult it: it drains its own sig, pinned
		// slots included, since routing those is its job.
		if (!owner && lot->stealableSize.load(std::memory_order_relaxed) <= 0) continue;

		uint64_t active_bits = lot->bits.load(std::memory_order_relaxed)
			& lot->sig.load(std::memory_order_acquire);

		// NOBODY sweeps a pinned slot, owner included. A pinned wake goes through Kick, which hands
		// the key to the ring, so finding it here as well makes one wake discoverable twice -- two
		// claims racing, and the loser wasting a CAS on a slot that is already gone. `sig` means
		// "available to whoever is hunting", and a pinned task is never that.
		active_bits &= ~lot->pinned.load(std::memory_order_relaxed);

		while (active_bits != 0) {
#if defined(_MSC_VER) && !defined(__clang__)
			unsigned long index;
			_BitScanForward64(&index, active_bits);
#else
			int index = __builtin_ctzll(active_bits);
#endif
			active_bits &= ~(1ULL << index);

			Task* task = lot->parked[index].load(std::memory_order_acquire);
			if (!task) { if (owner) release(lot, (int)index); continue; }   // a resumer took it

			const uint32_t flags = lot->flags[index].load(std::memory_order_acquire);
			if (flags == 0) continue;   // sig said otherwise; the wake was taken between the loads

			// CLAIM ONLY WHAT WE WILL RUN. A fiber is switched into, never queued, so a second one
			// taken in the same pass would have nowhere to go: leave it parked and addressable.
			if (direct || !CanRunHere(task)) continue;

			Task* expected = task;
			if (!lot->parked[index].compare_exchange_strong(expected, nullptr,
				std::memory_order_acq_rel)) {
				if (owner) release(lot, (int)index);
				continue;
			}
			lot->gen[index].fetch_add(1, std::memory_order_acq_rel);   // stale handles stop resolving
			lot->flags[index].store(0, std::memory_order_relaxed);
			ReleaseSig(lot, (int)index);   // claimed: the consent is spent
			task->record->handle.store(TaskHandle{}, std::memory_order_release);

			// A thief leaves the bit set: the owner reclaims it on its next sweep, reading a null slot.
			if (owner) release(lot, (int)index);

			if (task->type == TaskType::Fiber) {
				Fiber* f = task->record->fiber;
				// The slot CAS settled WHO owns it. The status settles WHETHER its context is saved:
				// the task enters the lot before the switch, so it can still be mid-switch here.
				// Signaled means the landing will deal with it -- do not run a live stack.
				if (f && f->ClaimForWake() == Fiber::ClaimResult::Claimed) {
					direct = task;
					if (!owner) return direct;   // a thief takes ONE and leaves
					break;
				}
			}
			else if (task->type == TaskType::Coroutine) {
				scheduler->WakeTask(task);
			}
		}
	}
	return direct;
}

// Hunt someone else's parked work. The dirty word says which shards hold a task that has been
// flagged but not yet swept -- normally because its owner is busy running something long. Without
// this, that task waits on one specific thread no matter how many are idle.
//
// Rotation, not random: random victim selection was measured to collapse balancing on the deque
// path (28 active workers down to 16).
/*
Task* Thread::StealParked() {
	if (!scheduler || !IsPoolWorker()) return nullptr;

	const size_t n = scheduler->workers.size();
	if (n <= 1) return nullptr;

	uint64_t dirty = scheduler->dirtyShards.load(std::memory_order_acquire);
	dirty &= ~(1ULL << (qIndex & 63));   // own shard is the owner sweep's job, not a steal
	if (dirty == 0) return nullptr;

	// Rotate the MASK, not the index: with 31 workers only bits 0..30 are ever set, so a rotated
	// index lands in 31..63 half the time, wraps, and converges on the lowest dirty shard.
	const unsigned r = (stealRotor += 1u + (unsigned)qIndex) & 63u;
	const uint64_t rot = r ? ((dirty >> r) | (dirty << (64 - r))) : dirty;

	for (uint64_t m = rot; m != 0; m &= m - 1) {
		const unsigned b = ((unsigned)LowestBit64(m) + r) & 63u;

		// Over 64 workers the bit aliases, so every worker congruent to b is a candidate.
		for (size_t w = b; w < n; w += 64) {
			if ((int)w == qIndex) continue;
			Thread* victim = scheduler->workers[w];
			if (!victim) continue;
			// One load skips a whole worker. Exact, unlike the dirty bit, which over-approximates.
			if (victim->stealableTotal.load(std::memory_order_relaxed) <= 0) continue;
			// Main is not hunted: a task main suspended is one it expected to keep.
			if (victim->isMain) continue;
			if (Task* t = PollTasks(victim)) return t;
		}
	}
	return nullptr;
}*/

// Resume's resolution, stopping at the generation check and touching nothing. A claim bumps
// lot->gen[index], so a mismatch is proof the park ended.
bool Thread::ParkLive(TaskHandle h) noexcept {
	if (!h.parked || h.parked > kMaxLots || h.index >= kAlign) return false;
	if (!TaskScheduler::IsInitialized()) return false;
	TaskScheduler& sched = TaskScheduler::Instance();
	if (h.worker_id >= sched.workers.size()) return false;
	Thread* worker = sched.workers[h.worker_id];
	if (!worker) return false;
	ParkingLot* lot = worker->lots[h.parked - 1].load(std::memory_order_acquire);
	if (!lot) return false;
	return lot->gen[h.index].load(std::memory_order_acquire) == h.generation;
}

// Several handles at once, grouped by (worker, lot) so the non-per-slot work is paid once each.
void Thread::KickBatch(const TaskHandle * handles, size_t n, uint32_t FLAG_RESUMED) {
	if (!handles || n == 0 || !TaskScheduler::IsInitialized()) return;
	TaskScheduler& sched = TaskScheduler::Instance();

	// One entry per distinct (worker, lot). Linear search is right here: a pass fires a handful,
	// and a small scan of a stack array beats anything with an allocation in it.
	struct Group { Thread* worker; ParkingLot* lot; uint16_t worker_id; uint8_t lotNo;
	               uint64_t mask; uint64_t newly; };
	constexpr size_t kMaxGroups = 16;
	Group  groups[kMaxGroups];
	size_t nGroups = 0;

	for (size_t i = 0; i < n; ++i) {
		const TaskHandle h = handles[i];
		if (!h.parked || h.parked > kMaxLots || h.index >= kAlign) continue;
		if (h.worker_id >= sched.workers.size()) continue;
		Thread* worker = sched.workers[h.worker_id];
		if (!worker) continue;
		ParkingLot* lot = worker->lots[h.parked - 1].load(std::memory_order_acquire);
		if (!lot) continue;
		if (lot->gen[h.index].load(std::memory_order_acquire) != h.generation) continue;

		// Per-slot and unavoidable: flags say WHY this task woke, and that is per task.
		lot->flags[h.index].fetch_or(FLAG_RESUMED, std::memory_order_release);

		// The key is NOT pushed here. It goes below, once the group's sig RMW has said which bits
		// this call actually turned on -- one entry per sig, as in Kick, or the ring can hold more
		// entries than there are parks and wrap onto unread ones.
		size_t g = 0;
		for (; g < nGroups; ++g) if (groups[g].lot == lot) break;
		if (g == nGroups) {
			// More distinct lots than the array holds: fall back to one kick each rather than
			// dropping any. Correct, just unbatched.
			if (nGroups == kMaxGroups) { Kick(h, FLAG_RESUMED); continue; }
			groups[nGroups++] = Group{ worker, lot, h.worker_id, h.parked, 0, 0 };
		}
		groups[g].mask |= 1ULL << h.index;
	}

	for (size_t g = 0; g < nGroups; ++g) {
		Group& gr = groups[g];

		// CONSENT FOR THE WHOLE GROUP IN ONE RMW. Only bits this call actually turned on, and that
		// a thief could take, may move the counters -- same rule as the single Kick.
		const uint64_t wasSig = gr.lot->sig.fetch_or(gr.mask, std::memory_order_release);
		gr.newly = gr.mask & ~wasSig;
		const uint64_t takeable = gr.newly & ~gr.lot->pinned.load(std::memory_order_relaxed);
		if (takeable) {
			const int32_t add = (int32_t)PopCount64(takeable);
			gr.lot->stealableSize.fetch_add(add, std::memory_order_relaxed);
			gr.worker->stealableTotal.fetch_add(add, std::memory_order_relaxed);
		}
	}

	// THE KEYS, now that every group's sig RMW has said which slots this call is responsible for.
	// One entry per sig, as in Kick: a slot whose sig was already up has a key in the ring already,
	// and a second is a duplicate entry for a claim that admits one winner.
	for (size_t i = 0; i < n; ++i) {
		const TaskHandle h = handles[i];
		if (!h.parked) continue;
		for (size_t g = 0; g < nGroups; ++g) {
			if (groups[g].worker_id != h.worker_id || groups[g].lotNo != h.parked) continue;
			if (groups[g].newly & (1ULL << h.index)) groups[g].worker->PushResume(h);
			break;
		}
	}

	// AFTER the keys, not per group as before. Waking a worker that then finds an empty ring lets
	// it park again before the key it was woken for arrives.
	for (size_t g = 0; g < nGroups; ++g) {
		groups[g].worker->MarkQueuedWork();
		groups[g].worker->NotifyWorker();   // ONCE for the group -- the reason this function exists
	}
}

// RESERVE THEN WRITE. The CAS wins an index nobody else can have, and only then is the slot
Task* Thread::ClaimByHandle(TaskHandle h) {
	if (!h.parked || h.parked > kMaxLots || h.index >= kAlign) return nullptr;
	ParkingLot* lot = lots[h.parked - 1].load(std::memory_order_acquire);
	if (!lot) return nullptr;
	if (lot->gen[h.index].load(std::memory_order_acquire) != h.generation) return nullptr;

	Task* task = lot->parked[h.index].load(std::memory_order_acquire);
	if (!task) { release(lot, (int)h.index); return nullptr; }   // a thief took it; reclaim the bit

	Task* expected = task;
	if (!lot->parked[h.index].compare_exchange_strong(expected, nullptr,
		std::memory_order_acq_rel)) {
		release(lot, (int)h.index);   // lost the race -- still our slot to reclaim
		return nullptr;
	}
	lot->gen[h.index].fetch_add(1, std::memory_order_acq_rel);
	lot->flags[h.index].store(0, std::memory_order_relaxed);
	ReleaseSig(lot, (int)h.index);
	task->record->handle.store(TaskHandle{}, std::memory_order_release);
	release(lot, (int)h.index);
	return task;
}

// First use of a chunk publishes it. A loser of that race frees its spare and takes the winner's --
// a chunk is never replaced once published, so any pointer read from here stays valid forever, and
// there is nothing to retire at a quiescent point.
std::atomic<TaskHandle>* Thread::ResumeChunk(size_t k) noexcept {
	if (k >= kResumeChunks) return nullptr;
	if (std::atomic<TaskHandle>* c = resumeChunks[k].load(std::memory_order_acquire)) return c;

	void* mem = Alloc(sizeof(std::atomic<TaskHandle>) * kResumeChunk);
	if (!mem) return nullptr;
	auto* fresh = static_cast<std::atomic<TaskHandle>*>(mem);
	for (size_t i = 0; i < kResumeChunk; ++i)
		::new (static_cast<void*>(&fresh[i])) std::atomic<TaskHandle>(TaskHandle{});

	std::atomic<TaskHandle>* expected = nullptr;
	if (resumeChunks[k].compare_exchange_strong(expected, fresh,
		std::memory_order_acq_rel, std::memory_order_acquire))
		return fresh;
	mi_free(fresh);
	return expected;
}

// Reserve then write: the CAS wins an index nobody else can have, and only then is the slot
// written. The owner reads in order and stops at the first empty slot. No full case -- see the
// bound on kResumeChunks.
void Thread::PushResume(TaskHandle h) noexcept {
	uint32_t w = resumeWrite.load(std::memory_order_relaxed);
	for (;;) {
		uint32_t expected = w;
		if (resumeWrite.compare_exchange_weak(expected, w + 1,
			std::memory_order_acq_rel, std::memory_order_relaxed))
			break;
		w = expected;
	}
	const uint32_t idx = w & kResumeIndexMask;   // WRAP: the counter free-runs, the ring does not
	std::atomic<TaskHandle>* c = ResumeChunk((size_t)(idx >> kResumeChunkShift));
	if (!c) {
		// Now genuinely unreachable: every chunk is allocated at thread start (AdoptCurrentThread)
		// and the index is masked into the table above. Fatal rather than a return, because the CAS
		// already consumed this index -- dropping the handle leaves the consumer stopped at an empty
		// slot forever, which costs every later resume on this worker, not one wake.
		std::fprintf(stderr, "[JLib::Scheduler] FATAL: resume ring index %u has no chunk on worker %d\n",
			w, qIndex);
		std::fflush(stderr);
		std::abort();
	}
	c[w & kResumeSlotMask].store(h, std::memory_order_release);
}

void Thread::Kick(TaskHandle h, uint32_t FLAG_RESUMED) {
	if (!TaskScheduler::IsInitialized()) return;
	TaskScheduler& sched = TaskScheduler::Instance();

	// By value, never a Task*: nothing here dereferences the task, so a handle for one that has
	// since finished fails the generation check below instead of touching freed memory.
	if (!h.parked || h.parked > kMaxLots || h.index >= kAlign) return;
	if (h.worker_id >= sched.workers.size()) return;

	Thread* worker = sched.workers[h.worker_id];
	if (!worker) return;

	ParkingLot* lot = worker->lots[h.parked - 1].load(std::memory_order_acquire);
	if (!lot) return;
	if (lot->gen[h.index].load(std::memory_order_acquire) != h.generation) return;

	// Consent is granted here and only here: flags say why, sig says ready (ParkingLot::sig).
	const uint64_t bit = 1ULL << h.index;
	lot->flags[h.index].fetch_or(FLAG_RESUMED, std::memory_order_release);
	const uint64_t wasSig = lot->sig.fetch_or(bit, std::memory_order_release);

	// Takeable only if THIS kick made it ready and a thief could have it. Double kicks are legal,
	// so the fetch_or's old value is what keeps the count from drifting on a repeat.
	if (!(wasSig & bit) && !(lot->pinned.load(std::memory_order_relaxed) & bit)) {
		lot->stealableSize.fetch_add(1, std::memory_order_relaxed);
		worker->stealableTotal.fetch_add(1, std::memory_order_relaxed);
	}


	// HAND THE OWNER THE KEY. Kick is the PINNED delivery: this worker is the only candidate, so it
	// gets the handle and the wake.
	//
	// ONE ENTRY PER SIG, not one per kick. Double kicks are legal and the claim admits exactly one
	// winner, so a repeat needs no second key -- and pushing one anyway is what let the ring hold
	// more entries than there are parks, which is the bound its size is chosen from (kResumeChunks).
	// The flags above are OR-ed either way, so a repeat still reports its reason.
	if (!(wasSig & bit)) worker->PushResume(h);

	worker->MarkQueuedWork();
	worker->NotifyWorker();
}

// DIRECT SIGNAL. For a caller that is holding the waiter itself -- a primitive with the node on
// that fiber's own frame, unlinked under the primitive's own lock. The pop IS the arbitration, so
// this skips everything Signal does to make a bare handle safe: the bounds checks, the workers
// lookup, and the generation compare that exists so a STRANGER cannot signal a recycled slot.
//
// UNPINNED IS AVAILABLE, NOT ADDRESSED. The claim here is the whole wake: it takes the task out of
// its lot and hands it to Push, which leaves it on the waker's own deque when the waker is a worker
// and on the shared intake otherwise -- reachable by every hunter either way. This is what the
// coroutine branch of WakeWaiter has always done.
//
// Addressing it to `home` instead (a key in that thread's resume ring) costs one thread's wake
// latency on every handoff, because only `home` can drain that ring: measured 500 ms against 12 ms
// for the coroutine path on the same lock. Availability beats locality here and it is not close.
//
// Returns false if there was nothing to wake: not a fiber, not parked, or already awake.
bool Thread::SignalDirect(Task * t, uint32_t /*FLAG_RESUMED*/) {
	if (!t || !t->record) return false;
	Fiber* f = t->record->fiber;
	if (!f) return false;

	switch (f->ClaimForWake()) {
	case Fiber::ClaimResult::Claimed:
		// Out of the lot and off any other waker's reach. Ours to place.
		TaskScheduler::Instance().Push(t);
		return true;
	case Fiber::ClaimResult::Signaled:
		// Still switching out. A wake places nothing: the landing sees SUSPEND_SIGNALED and keeps
		// the task, so placing it here would run a live stack.
		return true;
	default:
		return false;
	}
}

// The UNPINNED wake: consent only. The task stays in the lot, where its owner finds it on its own
// sweep and any hunter finds it through StealParked. Naming one thread here is what makes every
// wake wait on that thread. Nobody is woken while someone is already searching.
void Thread::Signal(TaskHandle h, uint32_t FLAG_RESUMED) {
	if (!TaskScheduler::IsInitialized()) return;
	TaskScheduler& sched = TaskScheduler::Instance();

	if (!h.parked || h.parked > kMaxLots || h.index >= kAlign) return;
	if (h.worker_id >= sched.workers.size()) return;
	Thread* worker = sched.workers[h.worker_id];
	if (!worker) return;

	ParkingLot* lot = worker->lots[h.parked - 1].load(std::memory_order_acquire);
	if (!lot) return;
	if (lot->gen[h.index].load(std::memory_order_acquire) != h.generation) return;

	lot->flags[h.index].fetch_or(FLAG_RESUMED, std::memory_order_release);
	lot->sig.fetch_or(1ULL << h.index, std::memory_order_release);

	// Same as SignalDirect: the key goes to the owner, which runs one and pushes the rest to its
	// deque. Waking an arbitrary idle worker instead was right when the task stayed in the lot for
	// anyone to find; it cannot drain another thread's ring.
	worker->PushResume(h);
	worker->MarkQueuedWork();
	worker->NotifyWorker();
}

// Hand the running task to worker N. A YIELD, not a park: the task stays runnable, so it needs no
// lot slot, no handle and nothing to claim -- the landing just places it on N instead of by pin.
bool Thread::SendTo(uint16_t worker) {
	if (!TaskScheduler::IsInitialized()) return false;
	if (worker != TaskRecord::kSendMain && worker >= TaskScheduler::Instance().workers.size())
		return false;

	// Fiber only. A coroutine can suspend only at a co_await, so stamping here and hoping for one
	// would be a silent no-op when none comes: the coroutine form is `co_await SendTo{n}`.
	Fiber* f = FiberFromStack();
	if (!f || !f->CanSuspend()) return false;
	Task* task = f->owningTask;
	if (!task || !task->record) return false;

	task->record->sendTo = worker;   // consumed by YieldFiber, after the switch
	f->Yield(Pin::None);
	return true;
}

// Main, whether it is a worker or its own thread. Out of pool it has no index to name.
bool Thread::SendToMain() { return SendTo(TaskRecord::kSendMain); }
void Thread::AdoptCurrentThread(size_t fiberCacheCapacity, size_t deepCacheCapacity)
{
	instance = this;
	thread_id = epochId;   // assigned by StartPool
	JLIB_STAT_ONLY(detail::StatLabelThread(qIndex, isMain ? "main (in pool)" : "worker");)
		// Set here, not in Worker(): main enters Worker() more than once, and a stop requested
		// between those entries must not be undone.
		running.store(true, std::memory_order_release);

	if (!isMain) {   // main is the application's thread; leave its power policy alone
		TaskScheduler::PowerThrottling pt = TaskScheduler::GetWorkerPowerThrottling();
		if (pt == TaskScheduler::PowerThrottling::Topology) {
			const bool onECore = ((size_t)qIndex < scheduler->isPCore.size())
				&& !scheduler->isPCore[(size_t)qIndex];
			pt = onECore ? TaskScheduler::PowerThrottling::Force
				: TaskScheduler::PowerThrottling::OptOut;
		}
		ApplyPowerThrottling(pt);
	}

	// This thread's mimalloc heap, before anything allocates from it below.
	detail::EnsureThreadHeap(this);

	localCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity, StackClass::Standard);
	deepCache.Initialize(&scheduler->GetGlobalPool(), deepCacheCapacity, StackClass::Deep);

	// The whole resume ring, here on its owner's thread: 16 chunks, 128 KB, from this worker's own
	// heap. A kick must not allocate -- the observer walks that path -- and a chunk that could not
	// be allocated would strand an index the producer's CAS already consumed, which stops the
	// consumer at that slot for the life of the thread (PushResume, PollTasks).
	for (size_t k = 0; k < kResumeChunks; ++k) {
		if (!ResumeChunk(k)) {
			std::fprintf(stderr, "[JLib::Scheduler] FATAL: resume ring chunk %zu would not allocate "
				"for worker %d\n", k, qIndex);
			std::fflush(stderr);
			std::abort();
		}
	}

	ready.store(true, std::memory_order_release);
}

void Thread::StartWorker(size_t cpu_affinity, size_t fiberCacheCapacity, size_t deepCacheCapacity)
{
	auto ready = std::make_shared<std::atomic<bool>>(false);
	thread = std::thread([this, ready, fiberCacheCapacity, deepCacheCapacity]() {
		while (!ready->load(std::memory_order_acquire)) std::this_thread::yield();
		AdoptCurrentThread(fiberCacheCapacity, deepCacheCapacity);
		this->Worker();
		});
	nativeHandle = thread.native_handle();

	auto affinityPolicy = scheduler->GetAffinityPolicy();

	if (cpu_affinity >= topology::CpuMask::kMaxCpus &&
		affinityPolicy != TaskScheduler::AffinityPolicy::None) {
		affinityPolicy = TaskScheduler::AffinityPolicy::None;
		// Exactly kMaxCpus is StartPool's "no CPU for this worker" sentinel -- a pool with more
		// workers than the topology hands out CPUs, which is a request, not a fault. Only a value
		// PAST the sentinel means a real CPU id the mask cannot hold.
		static std::atomic<bool> warned{ false };
		if (cpu_affinity > topology::CpuMask::kMaxCpus
			&& !warned.exchange(true, std::memory_order_relaxed)) {
			std::cerr << "[JLib::Scheduler] logical CPU " << cpu_affinity
				<< " is past CpuMask::kMaxCpus (" << topology::CpuMask::kMaxCpus
				<< ") -- that worker runs unbound. Raise CpuMask::kWords in Topology.h.\n";
		}
	}

#if JLIB_PLATFORM_WINDOWS

	const WORD  cpuGroup = (WORD)topology::CpuMask::GroupOf((topology::CpuId)cpu_affinity);
	const BYTE  cpuNumber = (BYTE)topology::CpuMask::BitOf((topology::CpuId)cpu_affinity);

	switch (affinityPolicy) {
	case TaskScheduler::AffinityPolicy::PhysicalOnly:
	case TaskScheduler::AffinityPolicy::Hard: {
		GROUP_AFFINITY ga{};
		ga.Mask = (KAFFINITY)(1ULL << cpuNumber);
		ga.Group = cpuGroup;
		SetThreadGroupAffinity(nativeHandle, &ga, nullptr);
		break;
	}
	case TaskScheduler::AffinityPolicy::Ideal: {
		PROCESSOR_NUMBER pn{};
		pn.Group = cpuGroup;
		pn.Number = cpuNumber;
		SetThreadIdealProcessorEx(nativeHandle, &pn, nullptr);
		break;
	}
	case TaskScheduler::AffinityPolicy::None:
		break;
	}
#else

	switch (affinityPolicy) {
	case TaskScheduler::AffinityPolicy::PhysicalOnly:
	case TaskScheduler::AffinityPolicy::Hard: {
		topology::CpuMask one;
		one.Set((topology::CpuId)cpu_affinity);
		BindThreadToMask(nativeHandle, one);
		break;
	}
	case TaskScheduler::AffinityPolicy::Ideal: {
		const size_t qi = (size_t)qIndex;
		if (qi < scheduler->llcMaskOfWorker.size()) {
			const topology::CpuMask& llc = scheduler->llcMaskOfWorker[qi];
			if (llc.Any()) BindThreadToMask(nativeHandle, llc);
		}
		break;
	}
	case TaskScheduler::AffinityPolicy::None:
		break;
	}
#endif
	ready->store(true, std::memory_order_release);
};
std::thread::id Thread::GetID() {
	return thread.get_id();
}
void Thread::SetQueueIndex(size_t index)
{
	qIndex = index;
};

bool Thread::DrainOwnInboxesToDeques() {

	const size_t BATCH = 64;
	Task* batch[BATCH];
	bool moved = false;

	auto drain = [&](TaskMPSCQueue* inbox, TaskDeque* deque, bool hiPriLane) {
		for (;;) {
			size_t count = 0;
			while (count < BATCH && inbox->pop(batch[count])) count++;
			if (count == 0) break;

			size_t n = 0;
			for (size_t i = 0; i < count; ++i)
				if (batch[i]) batch[n++] = batch[i];
			if (n) {

				if (!deque->push_bottom_batch(batch, n)) TaskDeque::FatalPushRefused();
				moved = true;
			}
		}
		};

	const size_t nAll = scheduler->workers.size();
	drain(scheduler->normalInboxes[qIndex].get(), scheduler->deques[qIndex].get(), false);
	return moved;
}

void Thread::Join() {
	bool expected = false;
	if (!joining.compare_exchange_strong(expected, true)) return;

	running.store(false, std::memory_order_release);
	NotifyWorker(true);

	if (thread.joinable())
		thread.join();

	joining.store(false, std::memory_order_release);
}
// The raw slot. SetCurrentFiber and tests only -- see the declaration in Thread.h.
JLib::Thread* JLib::detail::TlsThreadRaw() noexcept { return JLib::Thread::GetCurrent(); }

Thread* Thread::GetCurrent() {
	// Opaque on purpose (see JLIB_NOINLINE in Thread.h): with only `return instance;` visible, GCC
	// can prove this pure and reuse one result across a suspension in the caller -- the old
	// thread's value. The empty asm stops that; MSVC does not do it to a noinline call.
#if defined(__GNUC__) || defined(__clang__)
	__asm__ __volatile__("" ::: "memory");
#endif
	return instance;
}
void Thread::ReleaseCurrentThread() noexcept {
	if (instance == this) instance = nullptr;
}

// The park itself: the holder IS the storage, so this queues nothing. A wake is the only thing
// that ever puts the task back on a run queue. Returns false when no slot was free -- the caller
// must not switch out, or the task would be parked with nobody holding it.
bool Thread::StoreSuspended(Task * task, uint64_t timeoutMs)
{
	if (!SuspendTask(task, timeoutMs)) return false;
	if (task == currentRunningTask)
		currentRunningTask = nullptr;
	return true;
}



void Thread::NotifyWorker(bool force) {

	(void)force;

	Wake();
}

void JLib::KickWaitWord(std::atomic<int>*word) noexcept {
	const int prev = word->exchange(kWaitKicked, std::memory_order_seq_cst);
	if (prev != kWaitWaiting) return;
	JLIB_STAT(WakesSent);
#if defined(JLIB_PLATFORM_WINDOWS)
	::WakeByAddressSingle(word);
#elif JLIB_PLATFORM_LINUX
	FutexWakeOne(word);
#endif
}

void JLib::BlockOnWaitWord(std::atomic<int>*word) noexcept {
	int waiting = kWaitWaiting;
	while (word->load(std::memory_order_seq_cst) == kWaitWaiting) {
#if defined(JLIB_PLATFORM_WINDOWS)
		::WaitOnAddress(word, &waiting, sizeof(int), INFINITE);
#elif JLIB_PLATFORM_LINUX
		FutexWait(word, waiting);
#endif
	}
}

void Thread::Wake() noexcept {
	// Main never parks on workerState; its wake is a kick of its wait word.
	if (isMain) {
		KickWaitWord(&mainWait);
	}
	else if (workerState.exchange(WS_NOTIFIED, std::memory_order_seq_cst) == WS_PARKED) {
		JLIB_STAT(WakesSent);
#if defined(JLIB_PLATFORM_WINDOWS)
		::WakeByAddressSingle(&workerState);
#elif JLIB_PLATFORM_LINUX
		FutexWakeOne(&workerState);
#else
#error "JLib::Scheduler needs WaitOnAddress (Windows) or futex (Linux) -- no other park is supported"
#endif
	}

}

bool Thread::Ready() {
	return ready.load(std::memory_order_acquire);
}

Fiber* Thread::AcquireFiber(Task * task) {
	// Lambda tasks never get here: they run directly on the worker stack and may not suspend.
	ThreadLocalCache<>& cache = CacheFor(task ? task->stackClass : StackClass::Standard);
	// One pop: an empty cache already refills from the global pool inside Pop. A miss means the pool
	// is past its memory limit, and the caller requeues the task for later.
	Fiber* f = cache.Pop();
	if (!f) return nullptr;
	f->ResetForReuse();
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
	fiberAcquires.fetch_add(1, std::memory_order_relaxed);
#endif
	return f;
}

void Thread::ReleaseFiber(Fiber * f) {
	if (isHelper) {   // main's helper keeps no cache: fibers go straight back to the pool
		scheduler->GetGlobalPool().ReturnBatch(&f, 1);
		return;
	}
	CacheFor(f->stackClass).Push(f);
}

// ---- main's helper (MainMode::OutOfPool) ----

void Thread::AdoptAsHelper() {
	instance = this;
	thread_id = epochId;   // 0: main's epoch slot
	detail::EnsureThreadHeap(this);
	JLIB_STAT_ONLY(detail::StatLabelThread(-1, "main");)
		running.store(true, std::memory_order_release);
	ready.store(true, std::memory_order_release);
}

// UNUSED since main stopped stealing out of the pool (see OutOfPoolMainWait). Kept out of the
// build rather than deleted: RunHelped below is still the path that resumes a Pin::Main task on
// main, and this is the only other caller shape it ever had.
#if 0
// One steal from one victim per call (round-robin), any stealable task. Bounded by the caller.
bool Thread::HelpSteal() {
	const int nq = (int)scheduler->deques.size();
	if (nq == 0) return false;
	stealCursor = (stealCursor + 1) % nq;
	if (!scheduler->DequeHasWork((size_t)stealCursor)) return false;   // flag first, deque second
	// Mode::Pinned: every suspension must resume on its own thread, and nothing can resume on
	// main's helper -- so it only takes work that can never suspend (native tasks).
	const bool pinned = TaskScheduler::PinForced();
	auto ok = [pinned](StealBits sb) {
		return !pinned || sb.native();
		};
	auto s = scheduler->deques[stealCursor]->steal_if(ok);
	if (!s || !*s) return false;
	RunHelped(*s);
	return true;
}
#endif

// Runs a task ON MAIN: a Pin::Main resume out of the pool (RunMainTask hands it here). Same dispatch as a worker: coroutines and native tasks directly, fibers
// on a fiber from the global pool. A task that suspends here resumes elsewhere: nothing is pinned
// to the helper (Pin::Current resolves to no pin off the pool).
void Thread::RunHelped(Task * t) {
	if (scheduler->DiscardIfCancelled(t)) return;
	JLIB_STAT(RunHelper);
	t->record->home = this;   // before the hand-over: see TaskRecord::home

	if (t->type == TaskType::Coroutine) {
		JLIB_STAT(RunCoroutine);
		JLIB_STAT_ONLY(const std::uint64_t t0 = StatRunBegin(t);)
			t->started = 1;
		currentRunningTask = t;
		t->Execute();              // the frame owns the task from here
		currentRunningTask = nullptr;
		JLIB_STAT_ONLY(StatRunEnd(t0);)
			return;
	}
	if (t->type == TaskType::Native) {
		JLIB_STAT(RunLambda);
		JLIB_STAT_ONLY(const std::uint64_t t0 = StatRunBegin(t);)
			t->started = 1;
		currentRunningTask = t;
		t->Execute();
		currentRunningTask = nullptr;
		JLIB_STAT_ONLY(StatRunEnd(t0); StatTaskDone(t->record);)
			if (t->waitGroup) {
				t->waitGroup->Done();
			}
		scheduler->FreeTask(t);
		return;
	}

	Fiber* f = t->record->fiber;
	if (!f) {
		if (scheduler->GetGlobalPool().StealInto(&f, 1, t->stackClass) != 1 || !f) {
			JLIB_STAT(NoFiberRequeue);
			scheduler->Requeue(t);   // pool exhausted: give it back
			return;
		}
		f->ResetForReuse();
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
		fiberAcquires.fetch_add(1, std::memory_order_relaxed);
#endif
		t->record->fiber = f;
		f->owningTask = t;
		f->Init(GlobalFiberPool::FiberEntryWrapper);
	}
	JLIB_STAT(RunFiber);
	JLIB_STAT_ONLY(const std::uint64_t t0 = StatRunBegin(t);)
		t->started = 1;
	f->status.store(FiberStatus::RUNNING, std::memory_order_release);
	f->homeCtx = &this->schedulerCtx;
	currentRunningTask = t;
	currentFiber = f;
	busy.store(true, std::memory_order_relaxed);
	ContextSwitch(&this->schedulerCtx, &f->ctx);
	busy.store(false, std::memory_order_relaxed);
	JLIB_STAT_ONLY(StatRunEnd(t0);)

		Fiber* back = currentFiber ? currentFiber : f;
	OnFiberReturned(back, back->owningTask ? back->owningTask : t);
}

uint32_t Thread::FastRand() {
	static thread_local uint32_t x = []() {
		auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
		uint32_t seed = static_cast<uint32_t>(now);
		seed ^= (std::hash<std::thread::id>{}(std::this_thread::get_id()) << 1);
		return seed == 0 ? 1 : seed;
		}();
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
	return x;
}

void Thread::OnFiberReturned(Fiber * f, Task * task) noexcept {
	Task* task_to_run = task;
	FiberStatus fs = f->status.load(std::memory_order_acquire);
	if (fs == FiberStatus::DEAD) {
		JLIB_STAT_ONLY(StatTaskDone(task_to_run->record);)
			if (task_to_run->waitGroup) {
				task_to_run->waitGroup->Done();
			}
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
		fiberRecycles.fetch_add(1, std::memory_order_relaxed);
#endif
		TaskRecord* rec = task_to_run->record;
		if (rec) rec->fiber = nullptr;
		ReleaseFiber(f);
		scheduler->FreeTask(task_to_run);

		currentFiber = nullptr;
		currentRunningTask = nullptr;
	}
	else if (fs == FiberStatus::WANTS_YIELD) {

		f->status.store(FiberStatus::READY, std::memory_order_release);

		// yieldedLastPass lets the next pass look at the inbox before the yielder again. A SendTo
		// target rides on the record and is consumed inside YieldFiber, which both a fiber yield
		// and a coroutine's Reschedule reach.
		if (scheduler->YieldFiber(task_to_run)) yieldedLastPass = true;
		currentFiber = nullptr;
		currentRunningTask = nullptr;
	}
	else if (fs == FiberStatus::WANTS_SUSPEND || fs == FiberStatus::SUSPEND_SIGNALED) {
		// Off its stack, context saved. The waiter was published BEFORE the switch, so a waker can
		// already have marked this fiber: the CAS below is the arbitration, and a plain store here
		// would drop that wake.
		currentFiber = nullptr;
		currentRunningTask = nullptr;

		const uint64_t deadline = f->parkTimeoutMs;

		// Written on every park, including when there is no deadline: nothing else clears it, so a
		// wait without one would inherit the recycled fiber's last value.
		f->waitRecord.deadlineMs = (deadline > 0) ? GetCurrentTimeMs() + deadline : 0;
		f->parkTimeoutMs = 0;

		FiberStatus exp = FiberStatus::WANTS_SUSPEND;
		if (!f->status.compare_exchange_strong(exp, FiberStatus::SUSPENDED,
			std::memory_order_acq_rel)) {
			// SUSPEND_SIGNALED: a waker won while this fiber was switching out, and a wake places
			// nothing -- so it is runnable right here. Leave it CURRENT and the worker loop runs it
			// next pass. Re-Kicking cannot work: ReleaseTask below clears record->handle, so the
			// Kick would resolve an empty handle and the task would be lost.
			f->status.store(FiberStatus::READY, std::memory_order_release);
			f->parkFlag = nullptr;
			f->parkMask = 0;
			// Out of the lot, so nobody else can claim it while we hold it.
			if (task_to_run->record
				&& task_to_run->record->handle.load(std::memory_order_acquire).parked)
				f->ReleaseTask(task_to_run);
			currentFiber = f;
			currentRunningTask = task_to_run;
			return;
		}

		// Registered after SUSPENDED, so the observer can never hold a task that is still running.
		// It wakes the owner rather than delivering.
		const std::atomic<uint64_t>* pFlag = f->parkFlag;
		const uint64_t               pMask = f->parkMask;
		f->parkFlag = nullptr;
		f->parkMask = 0;
		if (deadline > 0 || pFlag) {
			const bool watched = Observer::Register(
				task_to_run, deadline > 0 ? GetCurrentTimeMs() + deadline : 0, pFlag, pMask);

			// A refused registration is a wait with no watcher. Nothing else watches deadlines or
			// flags -- PollTasks reads flags only -- so such a wait returns only if a waker kicked
			// it. Said once; not fatal, because the wait is still correct if a waker comes.
			if (!watched) {
				static std::atomic<unsigned> warned{ 0 };
				if (warned.fetch_add(1, std::memory_order_relaxed) == 0)
					std::fprintf(stderr,
						"[JLib::Scheduler] a timed wait registered no watcher -- the observer is not\n"
						"  running, and nothing else watches deadlines or flags. THIS WAIT WILL NOT\n"
						"  RETURN unless something kicks it. Observer::Start() runs from StartPool;\n"
						"  if you see this, it did not.\n");
			}
		}

	}
	else {
		assert(false && "OnFiberReturned: fiber came back in a state nobody handles; its task is lost");
		currentFiber = nullptr;
		currentRunningTask = nullptr;
	}
}

bool Thread::MainWorker(WaitCtx * ctx) {
	if (!isMain || instance != this || TaskScheduler::GetMainMode() != MainMode::InPool) return false;
	return Worker(ctx);
}

bool Thread::Worker(WaitCtx * ctx) {
	assert((ctx == nullptr || isMain) && "only main's slot waits inside Worker()");
	const bool countsAsHunter = scheduler->workers.size();

	tsanSchedulerFiber = tsan::CurrentFiber();

	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];
	// Locals, not thread_local: they only need to survive `continue`, and a thread_local would be
	// shared by a nested Worker() on the same thread.
	Task* task_to_run = nullptr;

	// Reserved workers (K) are not hunters: they may be forbidden to take compute work, so one of
	// them being the last hunter would not cover anything. K is fixed before Init.
	unsigned orphanSweep = 0;
	// Pool threads keep a retire bag and clear it at the idle gate below. (Main's is marked at
	// Init; it also clears its bag in its own wait loop.)
	detail::RunsGates() = true;

	// Hunting means scanning other deques. Nothing caps how many workers do it; leaving the hunt to
	// own queues and parks; enough others are already searching. Leaving the hunt to run a body
	// hands off: if it was the last hunter, an idle worker is woken (LeaveHunt).
	bool isSearching = false;
	auto enterHunt = [&]() { if (countsAsHunter && !isSearching) { TaskScheduler::EnterHunt(); isSearching = true; } };
	auto leaveHunt = [&]() { if (isSearching) { isSearching = false; TaskScheduler::LeaveHunt(); } };

	// Leaving to park wakes nobody. Refused for the last hunter, which stays up (see TryLeaveHuntForPark).
	auto tryLeaveHuntForPark = [&]() -> bool {
		if (!countsAsHunter || !isSearching) return true;
		if (!TaskScheduler::TryLeaveHuntForPark()) return false;
		isSearching = false;
		return true;
		};

	int stealVictim = qIndex;
	int stickyLeft = 0;   // passes left on the victim of the last steal (sticky steal cap)
	unsigned fairPass = 0;   // passes since start: every kFairTickEvery-th drains the inbox regardless
	size_t   l3Local = 0, l3Remote = 0;   // cursors into this worker's L3 group / the other groups
	unsigned scanRot = 0;   // seek: this thread's rotation over the flag mask

	// Consecutive passes that found nothing, for the last hunter's backoff (see the park gate).
	unsigned idleSpins = 0;

	// Main leaves Worker() only between tasks. Always out of the hunt, and never leaving its
	// wait word registered on a group that may outlive this call.
	auto mainExit = [&](bool result) -> bool {
		leaveHunt();
		if (ctx && ctx->wg) {
			std::atomic<int>* me = &mainWait;
			ctx->wg->threadWaiter.compare_exchange_strong(me, nullptr, std::memory_order_acq_rel);
		}
		return result;
		};

	// The app's message pump (Config::pumpMain). isMain is only ever set with main in the pool, so
	// out of the pool this stays null and main is left to pump itself.
	const TaskScheduler::Config& cfg = TaskScheduler::CurrentConfig();
	void (* const pump)(std::uint32_t) = isMain ? cfg.pumpMain : nullptr;
	const std::uint32_t pumpEvery = cfg.pumpMainEvery ? cfg.pumpMainEvery : 1;
	std::uint32_t pumpPass = 0;

	enterHunt();

	while (running.load(std::memory_order_acquire)) {

		if (ctx && !task_to_run && ctx->Done()) return mainExit(true);

		const size_t nAll = scheduler->workers.size();

		if (!task_to_run) {
#if !defined(JLIB_TIMER_CTL_NO_WORKER_POLL)
			// Any worker between tasks fires what the timer wheel owes. The gate is read before the
			// clock -- it holds INT64_MAX when nothing is armed -- so the clock is only read when
			// something could be due. TimerPollFire try-locks and never blocks.
			const std::int64_t gate = detail::g_timerGateNs.load(std::memory_order_relaxed);
			if (gate != INT64_MAX && MonotonicNs() >= gate) TimerPollFire();
#endif
			// No I/O poll here. Workers never touch the port: on Windows every look is a kernel call,
			// and under a steady I/O load a gated poll still fires often. The reactor thread is the
			// port's only reader and pumps completions into the injector, which this loop already
			// takes from every pass with one load (tests/verify/kport_model.c).
		}

		auto drainOwnInbox = [&]() -> bool {
			if (task_to_run) return false;
			size_t count = 0;
			while (count < BATCH_SIZE && scheduler->normalInboxes[qIndex]->pop(batch[count]))
				++count;
			if (count == 0) return false;

			// push_bottom_batch grows rather than refusing (a failed grow is itself fatal), so a
			// false here is a broken invariant -- same handling as DrainOwnInboxesToDeques.
			const int keep = (int)count - 1;
			if (keep > 0 && !scheduler->deques[qIndex]->push_bottom_batch(batch, (size_t)keep))
				TaskDeque::FatalPushRefused();
			JLIB_STAT_N(InboxStaged, keep);
			JLIB_STAT(RunInbox);
			task_to_run = batch[count - 1];
			return true;
			};

		ready.store(true, std::memory_order_release);

		if (task_to_run) {

			leaveHunt();
			idleSpins = 0;   // work exists: the next empty pass starts scanning at full speed

			// No publish of the inbox here. Moving it onto the deque just before running this task

			if (scheduler->DiscardIfCancelled(task_to_run)) {
				task_to_run = nullptr;
				ready.store(true, std::memory_order_release);
				continue;
			}

			task_to_run->started = 1;
#if !defined(JLIB_HOME_CTL_NO_UPDATE)   // negative control for tests/home_alloc_test.cpp
			task_to_run->record->home = this;   // before the hand-over: see TaskRecord::home
#endif

			// Coroutine: resumed by a plain call on this stack. The frame owns the task from here:
			// on suspend an awaiter has already handed it to a waiter (it may be running elsewhere
			// by the time resume() returns), and on completion the promise frees it. So this
			// thread must not touch the task after the call.
			if (task_to_run->type == TaskType::Coroutine) {
				Task* const coro = task_to_run;
				task_to_run = nullptr;
				currentRunningTask = coro;
				JLIB_STAT(RunCoroutine);
				JLIB_STAT_ONLY(const std::uint64_t t0 = StatRunBegin(coro);)
					busy.store(true, std::memory_order_relaxed);
				coro->Execute();
				busy.store(false, std::memory_order_relaxed);
				JLIB_STAT_ONLY(StatRunEnd(t0);)
					currentRunningTask = nullptr;
				ready.store(true, std::memory_order_release);
				continue;
			}

			// Native (lambda or CreateNativeTask): direct call on this stack, no fiber. It may never
			// suspend (fatal at every wait point). Everything else runs on a fiber.
			if (task_to_run->type == TaskType::Native) {
				currentRunningTask = task_to_run;
				JLIB_STAT(RunLambda);
				JLIB_STAT_ONLY(const std::uint64_t t0 = StatRunBegin(task_to_run);)
					busy.store(true, std::memory_order_relaxed);
				task_to_run->Execute();
				JLIB_STAT_ONLY(StatRunEnd(t0); StatTaskDone(task_to_run->record);)

					if (task_to_run->waitGroup) {
						task_to_run->waitGroup->Done();
					}
				busy.store(false, std::memory_order_relaxed);
				currentRunningTask = nullptr;

				scheduler->FreeTask(task_to_run);

				task_to_run = nullptr;
				ready.store(true, std::memory_order_release);
				continue;
			}

			Fiber* existingFiber = task_to_run->record->fiber;

			Fiber* f;
			if (existingFiber) {
				f = existingFiber;
			}
			else {
				f = AcquireFiber(task_to_run);
				if (!f) {
					JLIB_STAT(NoFiberRequeue);
					// Unstarted, so its only placement is where it was queued. Main gives it back to
					// itself: it may have come in through PushMain, and nothing else records that.
					if (isMain) scheduler->PushMainQueue(task_to_run);
					else        scheduler->Requeue(task_to_run);

					task_to_run = nullptr;
					std::this_thread::yield();
					continue;
				}
				task_to_run->record->fiber = f;
				f->owningTask = task_to_run;
				f->Init(GlobalFiberPool::FiberEntryWrapper);
			}

			f->status.store(FiberStatus::RUNNING, std::memory_order_release);
			f->homeCtx = &this->schedulerCtx;
			currentRunningTask = task_to_run;
			currentFiber = f;
			JLIB_STAT(RunFiber);
			JLIB_STAT_ONLY(const std::uint64_t t0 = StatRunBegin(task_to_run);)
				busy.store(true, std::memory_order_relaxed);
			{

				ContextSwitch(&this->schedulerCtx, &f->ctx);

			}
			// A fiber that handed this thread over and never reached a fiber landing point leaves
			// its pending hand-off here. Cheap: a thread-local load that is null on every ordinary
			// pass. See detail::SetPendingHandoff.
			detail::PlacePendingHandoff();
			busy.store(false, std::memory_order_relaxed);
			JLIB_STAT_ONLY(StatRunEnd(t0);)

				Fiber* back = currentFiber ? currentFiber : f;
			OnFiberReturned(back, back->owningTask ? back->owningTask : task_to_run);

			// A waker that won during the switch leaves the fiber runnable and CURRENT rather than
			// parked. Clearing task_to_run unconditionally would drop it here and send this worker
			// off to hunt, so the wake would be lost with the task still marked running.
			if (currentFiber && currentRunningTask) {
				task_to_run = currentRunningTask;
				continue;
			}

			task_to_run = nullptr;
		}

		// Every pass reaches here empty-handed: just after running a task, or having come in with
		if (pump && ++pumpPass >= pumpEvery) {
			pumpPass = 0;
			PumpMain(pump, cfg.pumpMainBudgetUs);
		}

		{

			enterHunt();

			// Counted HERE, acted on below. A pass that delivers a resume is still a pass, so the
			// tick advances whatever this worker ends up taking.
			const bool fairTick = (++fairPass % TaskScheduler::kFairTickEvery == 0);

			// This thread's own ready resumes come first: warm stack on this core, and only this
			// thread was obliged to deliver them. No scan -- the kicker left the handle in the ring,
			// so the gate is "was I handed anything", not "do I have anything parked".
			if (resumeRead.load(std::memory_order_relaxed)
				!= resumeWrite.load(std::memory_order_acquire)) {
				if (Task* woke = DrainResumes()) { task_to_run = woke; continue; }
			}


			// Fairness tick: the inbox is otherwise taken only when the deque empties, so a worker
#if !defined(JLIB_INBOX_CTL_NO_TICK)
			if (fairTick) drainOwnInbox();
			if (task_to_run) continue;
#endif

			if (!task_to_run) {
				Task* hp = nullptr;
				if (scheduler->hiPriInboxes[qIndex]->pop(hp) && hp) {
					task_to_run = hp;
					JLIB_STAT(RunHiPri);
					continue;   // took a task: run it before anything below can overwrite task_to_run
				}
			}

			// Guarded: a task already taken this pass (the lane, the fairness tick) must not be
			// overwritten -- it would be lost, never run and never requeued.
			if (yieldedLastPass && !task_to_run) {
				yieldedLastPass = false;

				if (!scheduler->normalInboxes[qIndex]->quiescent()) {
					Task* fromInbox = nullptr;
					if (scheduler->normalInboxes[qIndex]->pop(fromInbox) && fromInbox) {
						task_to_run = fromInbox;
						JLIB_STAT(RunInbox);
						continue;
					}
				}
				// No deque pop here: the one below is this pass's, and popping twice costs a second
				// bottom read on the miss that reaches it.
			}

			// The injector: work any worker may run, submitted from outside the pool. Ahead of the
			// deque, so a busy worker pays a contended load every pass rather than letting outside
			// work wait out everything already queued here. Moving it behind the deque changed no
			// measured case either way (latency, latency_hot, mutex, lock_s0..s100), so the order is
			// the intended one and not a measured one -- a case that separates them would decide it.
			if (!task_to_run) {
				if (Task* inj = TaskScheduler::TakeInjector()) {
					JLIB_STAT(RunInjector);
					task_to_run = inj;
					continue;
				}
			}

			// Every worker pops its OWN deque, reserved included: the park gate waits for it to be
			// empty, so a worker that may not pop it would spin forever. A reserved worker's deque
			// only holds what it pushed there itself (resumes and yields of unpinned tasks).
			if (!task_to_run) {
				auto opt = scheduler->deques[qIndex]->pop_bottom();
				if (opt) {
					Task* task = *opt;
					if (!task) {
						std::cerr << "[worker " << qIndex << "] Null task from pop_bottom!" << std::endl;
					}
					else {
						task_to_run = task;
						JLIB_STAT(RunOwnDeque);
						continue;
					}
				}
			}

		}

		drainOwnInbox();

		{

			if (!task_to_run && (isSearching)) {
				const bool timeScan = JLIB_STAT_SAMPLE();
				const std::uint64_t scanT0 = timeScan ? JLIB_STAT_TICKS() : 0;

				// Nothing vets a steal by core class. The check that used to sit here always
				// returned true, and it could not do otherwise: a thief that refuses work by the
				// class of the core it is on either starves the task or starves itself. Targeting
				// a core class is a PUSH-side decision, and only Windows classifies reliably.
				auto classOK = [](StealBits) { return true; };
				auto tryStealFrom = [&](int target) -> bool {
					// The flag array first: one relaxed byte read, in memory no deque owns. Only
					// when it says there may be work does this touch the victim's deque.
					if (!scheduler->DequeHasWork((size_t)target)) return false;

					JLIB_STAT(StealProbes);

					auto s = scheduler->deques[target]->steal_if(classOK);
					if (!s) return false;


					JLIB_STAT(StealHits);
					JLIB_STAT(RunStolen);
					task_to_run = *s;
					return true;
					};

			

				const int nq = (int)scheduler->deques.size();
				if (nq > 1) {

					// Steal order, one pass:
					const bool grouped = scheduler->l3Groups > 1;
					const size_t myG = grouped ? scheduler->l3GroupOf[(size_t)qIndex] : 0;
					const bool sticking = stickyLeft > 0;
					if (sticking) {
						JLIB_STAT(StickyProbes);
					}
					else if (grouped) {
						const std::vector<int>& mine = scheduler->l3Members[myG];
						const size_t m = mine.size();
						for (size_t i = 1; i <= m; ++i) {
							l3Local = (l3Local + 1) % m;
							if (mine[l3Local] != qIndex) break;
						}
						stealVictim = mine[l3Local];
					}
					else {
						stealVictim = (stealVictim + 1) % nq;
						if (stealVictim == qIndex) stealVictim = (stealVictim + 1) % nq;
					}

					bool hit = stealVictim != qIndex && tryStealFrom(stealVictim);
					if (hit) {
						if (sticking) { JLIB_STAT(StickyHits); --stickyLeft; }
						else          stickyLeft = (int)scheduler->GetStickyStealCap() - 1;
					}
					else {
						stickyLeft = 0;
					}

					// 2. Seek (own group).
					if (!hit && nq <= 64 && scheduler->SeekOnMiss()) {
						uint64_t mask = 0;
						for (int v = 0; v < nq; ++v)
							if (v != qIndex && v != stealVictim
								&& (!grouped || scheduler->l3GroupOf[(size_t)v] == myG)
								&& scheduler->DequeHasWork((size_t)v))
								mask |= uint64_t(1) << v;
						if (mask) {
							scanRot = (scanRot + 1 + (unsigned)qIndex) & 63u;
							const unsigned r = scanRot;
							const uint64_t rot = r ? ((mask >> r) | (mask << (64 - r))) : mask;
							unsigned b = 0;
							while (!((rot >> b) & 1u)) ++b;
							JLIB_STAT(SeekProbes);
							if (tryStealFrom((int)((b + r) & 63u))) { JLIB_STAT(SeekHits); hit = true; }
						}
					}

					// 3. The SMT sibling, only if fat.
					if (!hit) {
						const int sib = ((size_t)qIndex < scheduler->siblingQIndex.size())
							? scheduler->siblingQIndex[qIndex] : -1;
						if (sib >= 0 && sib != stealVictim && sib != qIndex && sib < nq
							&& scheduler->deques[sib]->size_approx() > TaskScheduler::kFatDeque)
							hit = tryStealFrom(sib);
					}

					// 4. Another L3: last, and only when nothing in the own group is flagged.
					if (!hit && grouped) {
						bool localWork = false;
						for (int v : scheduler->l3Members[myG])
							if (v != qIndex && scheduler->DequeHasWork((size_t)v)) { localWork = true; break; }
						if (!localWork) {
							const std::vector<int>& others = scheduler->l3Others[myG];
							l3Remote = (l3Remote + 1) % others.size();
							JLIB_STAT(StealRemoteProbes);
							hit = tryStealFrom(others[l3Remote]);
						}
					}
				}

				if (timeScan) JLIB_STAT_HIST(StealScan, JLIB_STAT_TICKS() - scanT0);
				if (task_to_run) continue;
			}

		}

		drainOwnInbox();

		if (task_to_run) {
			continue;
		}
		else {
			// The push flag is cleared here, on the way to idle, and only when it is set -- not at
			// the top of every pass, where it cost a seq_cst store (a full fence) per task. A push
			// that set it before this clear is found by the rescan; one after it, by the recheck
			// that follows publishing PARKED. A flag left over from a busy stretch costs one rescan.
			if (hasQueuedWork.load(std::memory_order_seq_cst)) {
				hasQueuedWork.store(false, std::memory_order_seq_cst);
				continue;
			}

			if (!running.load(std::memory_order_acquire)
				|| (!scheduler->paused.load(std::memory_order_seq_cst)
					&& (hasQueuedWork.load(std::memory_order_seq_cst)
						|| !scheduler->hiPriInboxes[qIndex]->quiescent()

						|| (!scheduler->normalInboxes[qIndex]->quiescent())
						|| !TaskScheduler::InjectorIdle()))) {   // every worker reads the injector

				if (!running.load(std::memory_order_acquire)) break;

				platform::CpuRelax();
				continue;
			}

			{

				if (!running.load(std::memory_order_acquire)) {

					break;
				}

				if (scheduler->deques[qIndex]->size_approx() == 0) {

					// MainWorker() with nothing to wait for: main is done here and goes back to its
					// caller. Leaving to run the caller's code is like leaving to run a body.
					if (isMain && !ctx) return mainExit(true);

					// Reclaim gate: an idle thread reclaims its own bags and hands what is still
					// protected to the orphan store, so nothing is stranded while it sleeps. Orphans
					// are swept now and then (the listener passes here constantly). Main (InPool) clears
					// only its own bag here; the orphans belong to pool workers.
					if (Reclaimer::GatePending())
						Reclaimer::Gate(true, !isMain);
					else if (!isMain && (++orphanSweep & 0x3FFu) == 0 && Reclaimer::OrphansPending())
						Reclaimer::Gate(false, true);

					// The last hunter stays up and keeps working the find loop (searching never 0).
					if (!tryLeaveHuntForPark()) {
						constexpr unsigned kHotPasses = 2000;
						if (idleSpins < kHotPasses) {
							++idleSpins;
							platform::CpuRelax();
							continue;
						}
						// Past the window: grow the pause to a cap of 32 relaxes (~1.5 us), which
						// is the longest a resume can wait to be stolen.
						const unsigned steps = idleSpins - kHotPasses;
						const unsigned shift = steps < 40 ? (steps / 8) : 5;
						const unsigned relaxes = 1u << shift;
						for (unsigned i = 0; i < relaxes; ++i) platform::CpuRelax();
						if (steps < 40) ++idleSpins;
						continue;
					}

					// ---- MAIN BLOCKS ON ITS WAIT WORD, NEVER ON workerState ----
					// Same handshake as the park below: publish WAITING, recheck, block. Kicked by a
					// push or resume aimed at slot 0, PushMain, the group reaching 0, WakeMain, or stop.
					if (isMain) {
						int r = kWaitRunning;
						if (!mainWait.compare_exchange_strong(r, kWaitWaiting,
							std::memory_order_seq_cst, std::memory_order_seq_cst)) {
							mainWait.store(kWaitRunning, std::memory_order_seq_cst);   // consume the kick
							enterHunt();
							continue;
						}
						if (ctx->wg) {
							ctx->wg->threadWaiter.store(&mainWait, std::memory_order_seq_cst);
							ctx->wg->n.fetch_or(WaitGroup::WAITER_BIT, std::memory_order_acq_rel);
						}
						if (!running.load(std::memory_order_seq_cst)
							|| ctx->Done()
							|| scheduler->deques[qIndex]->size_approx() != 0
							|| hasQueuedWork.load(std::memory_order_seq_cst)
							|| !scheduler->hiPriInboxes[qIndex]->quiescent()
							|| !scheduler->normalInboxes[qIndex]->quiescent()
							|| !TaskScheduler::InjectorIdle()) {   // main helps with injected work too
							mainWait.store(kWaitRunning, std::memory_order_seq_cst);
							enterHunt();
							if (!running.load(std::memory_order_acquire)) break;
							continue;
						}
						JLIB_EPOCH_CHECK_NO_GUARD("main wait word");
						BlockOnWaitWord(&mainWait);
						mainWait.store(kWaitRunning, std::memory_order_seq_cst);
						enterHunt();
						if (!running.load(std::memory_order_acquire)) break;
						continue;
					}
					assert(!isMain && "main must never reach the workerState park");

					{
						int e = WS_NOTIFIED;
						if (workerState.compare_exchange_strong(e, WS_EMPTY,
							std::memory_order_seq_cst, std::memory_order_relaxed)) {
							enterHunt();
							continue;
						}
					}

					int expectedEmpty = WS_EMPTY;
					if (!workerState.compare_exchange_strong(expectedEmpty, WS_PARKED,
						std::memory_order_seq_cst, std::memory_order_relaxed)) {

						if (expectedEmpty == WS_NOTIFIED) {
							int e2 = WS_NOTIFIED;
							workerState.compare_exchange_strong(e2, WS_EMPTY,
								std::memory_order_seq_cst, std::memory_order_relaxed);
						}
						enterHunt();
						continue;
					}
					int sleeping = WS_PARKED;

					parkCount.fetch_add(1, std::memory_order_relaxed);
					JLIB_STAT(Parks);

					// Only compute workers go on the idle stack; K is woken directly by lane pushes.
					TaskScheduler::RegisterIdleWorker((size_t)qIndex);

					assert(currentFiber == nullptr &&
						"worker about to park while still owning a fiber -- that frame can never "
						"be resumed by anyone");
					assert(currentRunningTask == nullptr &&
						"worker about to park while still holding a running task");

					if (!running.load(std::memory_order_acquire)
						|| scheduler->deques[qIndex]->size_approx() != 0
						|| hasQueuedWork.load(std::memory_order_seq_cst)
						|| !scheduler->hiPriInboxes[qIndex]->quiescent()

						|| (!scheduler->normalInboxes[qIndex]->quiescent())
						|| (!TaskScheduler::InjectorIdle())   // compute workers read it; K does not
						|| (TaskScheduler::SearchingCount() == 0)) {

						TaskScheduler::UnregisterIdleWorker((size_t)qIndex);

						int parked = WS_PARKED;
						if (!workerState.compare_exchange_strong(parked, WS_EMPTY,
							std::memory_order_seq_cst, std::memory_order_relaxed)) {
							int n = WS_NOTIFIED;
							workerState.compare_exchange_strong(n, WS_EMPTY,
								std::memory_order_seq_cst, std::memory_order_relaxed);
						}

						enterHunt();

						if (!running.load(std::memory_order_acquire)) break;
						continue;
					}

					// A parker parks. No timeout: a worker that wakes itself on a schedule to go
					while (workerState.load(std::memory_order_seq_cst) == WS_PARKED
						&& running.load(std::memory_order_acquire)) {
#if defined(JLIB_PLATFORM_WINDOWS)

						::WaitOnAddress(&workerState, &sleeping, sizeof(int), INFINITE);
#elif JLIB_PLATFORM_LINUX

						FutexWait(&workerState, sleeping);
#else

#error "JLib::Scheduler needs WaitOnAddress (Windows) or futex (Linux)"
#endif
					}

					{
						int got = WS_NOTIFIED;
						workerState.compare_exchange_strong(got, WS_EMPTY,
							std::memory_order_seq_cst, std::memory_order_relaxed);
					}

					TaskScheduler::UnregisterIdleWorker((size_t)qIndex);

					enterHunt();

					if (!running.load(std::memory_order_acquire)) break;
					continue;
				}

			}
		}
	}
	if (!isMain) running.store(false, std::memory_order_release);
	return mainExit(false);
}

namespace JLib {

	void Thread::PushPathFieldOffsets(size_t& hasQueuedWorkOff,
		size_t& workerStateOff) noexcept {

		hasQueuedWorkOff = offsetof(Thread, hasQueuedWork);
		workerStateOff = offsetof(Thread, workerState);
	}
}
