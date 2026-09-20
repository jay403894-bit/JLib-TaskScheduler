// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Hazard.h"  
#include "../include/Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include "../include/Timer.h"   
#include "../include/TaskLocal.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <cstring>   
#include <cstdlib>
#include <utility>   

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
                      : p == WorkerPrio::High     ? THREAD_PRIORITY_HIGHEST
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

static inline void BindThreadToMask(pthread_t handle, const topology::CpuMask& mask)
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

Thread::Thread(TaskScheduler& scheduler) : scheduler(&scheduler) {
	std::memset(&schedulerCtx, 0, sizeof(Context));
}
Thread::~Thread() {
}

void Thread::AdoptCurrentThread(size_t fiberCacheCapacity)
{
	instance  = this;
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
		// K runs above the compute workers. It never parks, and a spinning thread at normal priority
		// takes turns with every busy thread once the machine is saturated: measured p99 latency of
		// 0.2-3 ms and p99.9 up to 28 ms at normal, 4-104 us at high (medians unchanged). Best effort
		// on Linux, where a negative nice needs privileges.
		if (qIndex >= 0 && TaskScheduler::IsReservedIndex((size_t)qIndex, scheduler->workers.size()))
			ApplyWorkerPriority(WorkerPrio::High);
	}

	localCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity, StackClass::Standard);
	tinyCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity, StackClass::Tiny);
	deepCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity, StackClass::Deep);

	ready.store(true, std::memory_order_release);
}

void Thread::StartWorker(size_t cpu_affinity, size_t fiberCacheCapacity)
{
	auto ready = std::make_shared<std::atomic<bool>>(false);
	thread = std::thread([this, ready, fiberCacheCapacity]() {
		while (!ready->load(std::memory_order_acquire)) std::this_thread::yield();
		AdoptCurrentThread(fiberCacheCapacity);   
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
	
	const WORD  cpuGroup  = (WORD)topology::CpuMask::GroupOf((topology::CpuId)cpu_affinity);
	const BYTE  cpuNumber = (BYTE)topology::CpuMask::BitOf((topology::CpuId)cpu_affinity);

	switch (affinityPolicy) {
	case TaskScheduler::AffinityPolicy::PhysicalOnly:   
	case TaskScheduler::AffinityPolicy::Hard: {
		GROUP_AFFINITY ga{};
		ga.Mask  = (KAFFINITY)(1ULL << cpuNumber);
		ga.Group = cpuGroup;
		SetThreadGroupAffinity(nativeHandle, &ga, nullptr);
		break;
	}
	case TaskScheduler::AffinityPolicy::Ideal: {
		PROCESSOR_NUMBER pn{};
		pn.Group  = cpuGroup;
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
	if (!TaskScheduler::IsReservedIndex((size_t)qIndex, nAll))
		drain(scheduler->normalInboxes[qIndex].get(), scheduler->deques[qIndex].get(), false);
	return moved;
}

void Thread::Join() {
	bool expected = false;
	if (!joining.compare_exchange_strong(expected, true)) return;

	running.store(false, std::memory_order_release);
	NotifyWorker( true);   

	if (thread.joinable())
		thread.join();

	joining.store(false, std::memory_order_release);
}
Thread* Thread::GetCurrent() {
	return instance;
}
void Thread::ReleaseCurrentThread() noexcept {
	if (instance == this) instance = nullptr;
}

void Thread::CoYield(Fiber* targetFiber, Pin pin){
	if (targetFiber) {
		targetFiber->CoYield(pin);
	}
}
void Thread::Suspend(Fiber* targetFiber, Pin pin){
	if (targetFiber) {
		targetFiber->Suspend(pin);
	}
}
 void Thread::Resume(Fiber* targetFiber) {
	 if (targetFiber) {
		 targetFiber->Resume(); 
	 }
}

 void JLib::Thread::CoYield(Pin pin)
 {
	 GetCurrent()->currentFiber->CoYield(pin);
 }

 void JLib::Thread::Suspend(Pin pin)
 {
	 GetCurrent()->currentFiber->Suspend(pin);
 }

void Thread::NotifyWorker(bool force){

	(void)force;

	Wake();
}

void JLib::KickWaitWord(std::atomic<int>* word) noexcept {
	const int prev = word->exchange(kWaitKicked, std::memory_order_seq_cst);
	if (prev != kWaitWaiting) return;
	JLIB_STAT(WakesSent);
#if defined(JLIB_PLATFORM_WINDOWS)
	::WakeByAddressSingle(word);
#elif JLIB_PLATFORM_LINUX
	FutexWakeOne(word);
#endif
}

void JLib::BlockOnWaitWord(std::atomic<int>* word) noexcept {
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
	} else if (workerState.exchange(WS_NOTIFIED, std::memory_order_seq_cst) == WS_PARKED) {
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

bool Thread::Ready(){
	return ready.load(std::memory_order_acquire);
}
       
Fiber* Thread::AcquireFiber(Task* task) {
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

void Thread::ReleaseFiber(Fiber* f) {
	if (isHelper) {   // main's helper keeps no cache: fibers go straight back to the pool
		scheduler->GetGlobalPool().ReturnBatch(&f, 1);
		return;
	}
	CacheFor(f->stackClass).Push(f);
}

// ---- main's helper (MainMode::OutOfPool) ----

void Thread::AdoptAsHelper() {
	instance  = this;
	thread_id = epochId;   // 0: main's epoch slot
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
		if (sb.type == TaskType::Main) return false;
		return !pinned || sb.native;
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
void Thread::RunHelped(Task* t) {
	if (scheduler->DiscardIfCancelled(t)) return;
	JLIB_STAT(RunHelper);

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
	if (t->native) {
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
	tsan::SwitchTo(f->tsanFiber);
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

void Thread::OnFiberReturned(Fiber* f, Task* task) noexcept {
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

		// yieldedLastPass lets the next pass look at the inbox before the yielder again.
		if (scheduler->YieldFiber(task_to_run)) yieldedLastPass = true;
		currentFiber = nullptr;
		currentRunningTask = nullptr;
	}
	else {
		
		FiberStatus exp = FiberStatus::WANTS_SUSPEND;
		if (f->status.compare_exchange_strong(exp, FiberStatus::SUSPENDED, std::memory_order_acq_rel)) {
			
		}
		else if (exp == FiberStatus::SUSPEND_SIGNALED) {
			
			f->status.store(FiberStatus::READY, std::memory_order_release);
			scheduler->ResumeFiber(task_to_run);
		}
		currentFiber = nullptr;
		currentRunningTask = nullptr;
	}
}

bool Thread::MainWorker(WaitCtx* ctx) {
	if (!isMain || instance != this || TaskScheduler::GetMainMode() != MainMode::InPool) return false;
	return Worker(ctx);
}

bool Thread::Worker(WaitCtx* ctx) {
	assert((ctx == nullptr || isMain) && "only main's slot waits inside Worker()");

	tsanSchedulerFiber = tsan::CurrentFiber();

	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];
	// Locals, not thread_local: they only need to survive `continue`, and a thread_local would be
	// shared by a nested Worker() on the same thread.
	Task* task_to_run = nullptr;

	// Reserved workers (K) are not hunters: they may be forbidden to take compute work, so one of
	// them being the last hunter would not cover anything. K is fixed before Init.
	const bool countsAsHunter =
		!TaskScheduler::IsReservedIndex((size_t)qIndex, scheduler->workers.size());

	unsigned orphanSweep = 0;
	// Pool threads keep a retire bag and clear it at the idle gate below. (Main's is marked at
	// Init; it also clears its bag in its own wait loop.)
	detail::RunsGates() = true;

	// Hunting means scanning other deques. Nothing caps how many workers do it; leaving the hunt to
	// own queues and parks; enough others are already searching. Leaving the hunt to run a body
	// hands off: if it was the last hunter, an idle worker is woken (LeaveHunt).
	bool isSearching = false;
	auto enterHunt =[&]() { if (countsAsHunter && !isSearching) { TaskScheduler::EnterHunt(); isSearching = true; } };
	auto leaveHunt = [&]() { if (isSearching) { isSearching = false; TaskScheduler::LeaveHunt(); } };

	// Leaving to park wakes nobody. Refused for the last hunter, which stays up (see TryLeaveHuntForPark).
	auto tryLeaveHuntForPark = [&]() -> bool {
		if (!countsAsHunter || !isSearching) return true;
		if (!TaskScheduler::TryLeaveHuntForPark()) return false;
		isSearching = false;
		return true;
	};

	int stealVictim = qIndex;
	int stickyLeft  = 0;   // passes left on the victim of the last steal (sticky steal cap)
	unsigned fairPass = 0;   // passes since start: every kFairTickEvery-th drains the inbox regardless
	size_t   l3Local = 0, l3Remote = 0;   // cursors into this worker's L3 group / the other groups
	unsigned scanRot = 0;   // seek: this thread's rotation over the flag mask
	// A reserved worker (K) holding a task it stole. Checked once at dispatch, then cleared.
	bool stolenOnK = false;

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

	enterHunt();

	while (running.load(std::memory_order_acquire)) {

		if (ctx && !task_to_run && ctx->Done()) return mainExit(true);

#if !defined(JLIB_FIBERHOLDER_CTL_NO_WORKER_DRAIN)
#endif  

		const size_t kNow = TaskScheduler::GetHotWorkers();
		
		const size_t nAll = scheduler->workers.size();
		const bool isReservedWorker = TaskScheduler::IsReservedIndex((size_t)qIndex, nAll);

		auto drainOwnInbox = [&]() -> bool {
			if (task_to_run || isReservedWorker) return false;
			size_t count = 0;
			while (count < BATCH_SIZE && scheduler->normalInboxes[qIndex]->pop(batch[count]))
				++count;
			if (count == 0) return false;

			const int keep = (int)count - 1;
			if (keep == 0 || scheduler->deques[qIndex]->push_bottom_batch(batch, (size_t)keep)) {
				JLIB_STAT_N(InboxStaged, keep);
				JLIB_STAT(RunInbox);
				task_to_run = batch[count - 1];
				return true;
			}
			
			for (size_t i = 0; i < count; ++i)
				if (batch[i]) scheduler->Requeue(batch[i]);
			return false;
		};

		ready.store(true, std::memory_order_release);

		if (task_to_run) {

			leaveHunt();
			idleSpins = 0;   // work exists: the next empty pass starts scanning at full speed

			// No publish of the inbox here. Moving it onto the deque just before running this task
			// put it UNDER whatever the task pushes next, and a task that keeps pushing its successor
			// then buried it for good (tests/inbox_fairness_test.cpp: 0 of 62 ran; 62 of 62 without).
			// The inbox goes onto the deque when the deque runs dry and on the fairness tick -- at
			// the bottom, so the next pops take it.

			// K runs a stolen task as a compute worker until it returns (or yields). The steal gate
			// allowed the pickup; this checks whether I/O reached K after it. Only K's own I/O
			// sources are read -- never the victim deque. If I/O is there, give the task back.
			const bool stolenHere = stolenOnK;
			stolenOnK = false;

			if (scheduler->DiscardIfCancelled(task_to_run)) {
				task_to_run = nullptr;
				ready.store(true, std::memory_order_release);
				continue;
			}

			// Main-thread work that reached any other thread goes back to main.
			if (task_to_run->type == TaskType::Main && !isMain) {
				assert(false && "a TaskType::Main task reached a non-main worker");
				scheduler->PushMainQueue(task_to_run);
				task_to_run = nullptr;
				continue;
			}

			if (stolenHere
			    && (!TaskScheduler::IoLaneQuiet()
			        || !TaskScheduler::LaneIntakeIdle()
			        || !scheduler->hiPriInboxes[qIndex]->quiescent())) {
				TaskScheduler::NoteReservedStealReturned();
				scheduler->Requeue(task_to_run);   // unstarted: placed on a compute worker
				task_to_run = nullptr;
				continue;
			}

			task_to_run->started = 1;

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
			if (task_to_run->native) {
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
					{
						
						scheduler->Requeue(task_to_run);
					}
					
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
				
				tsan::SwitchTo(f->tsanFiber);
				ContextSwitch(&this->schedulerCtx, &f->ctx);
	
			}
			busy.store(false, std::memory_order_relaxed);
			JLIB_STAT_ONLY(StatRunEnd(t0);)

			Fiber* back = currentFiber ? currentFiber : f;
			OnFiberReturned(back, back->owningTask ? back->owningTask : task_to_run);

			task_to_run = nullptr;
		}
		
		{
			
			enterHunt();

			if (isReservedWorker && !scheduler->normalInboxes[qIndex]->quiescent()) {
				static std::atomic<unsigned> strayWarned{ 0 };
				if (strayWarned.fetch_add(1, std::memory_order_relaxed) == 0)
					std::fprintf(stderr,
						"[JLib::Scheduler] ordinary work in the loPri inbox of RESERVED worker %d"
						" (K=%zu). K never drains loPri, so this task will not run. This is a"
						" placement bug -- find the writer; there is no longer a net.\n",
						qIndex, kNow);
			}
			
			if (!task_to_run && isReservedWorker) {
				if (Task* shared = TaskScheduler::TakeLaneIntake()) {
					JLIB_STAT(RunLaneIntake);
					task_to_run = shared;
				}
			}

			// Fairness tick, every kFairTickEvery-th pass of a compute worker. Normally the inbox is
			// taken only when the worker's own deque is empty; a worker whose deque never empties
			// (work that keeps pushing its successor) would then ignore its inbox forever. On the
			// tick it drains the inbox regardless: the batch goes onto the bottom of its deque, the
			// last one runs now, and the pops that follow take the rest.
			const bool fairTick = !isReservedWorker && (++fairPass % TaskScheduler::kFairTickEvery == 0);
#if !defined(JLIB_INBOX_CTL_NO_TICK)
			if (!task_to_run && fairTick) drainOwnInbox();
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

				if (!isReservedWorker && !scheduler->normalInboxes[qIndex]->quiescent()) {
					Task* fromInbox = nullptr;
					if (scheduler->normalInboxes[qIndex]->pop(fromInbox) && fromInbox) {
						task_to_run = fromInbox;
						JLIB_STAT(RunInbox);
						continue;
					}
				}

				if (auto opt = scheduler->deques[qIndex]->pop_bottom()) {
					if (Task* t = *opt) { JLIB_STAT(RunOwnDeque); task_to_run = t; continue; }
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

			// Only hunters scan other deques (K steals under its own rule below).
			if (!task_to_run && (isSearching || isReservedWorker)) {
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

					// K steals only when pinning is not forced, and only while the I/O lane is quiet.
					// With forced pinning a stolen task that suspends would resume back onto K.
					if (isReservedWorker
					    && (TaskScheduler::PinForced() || !TaskScheduler::IoLaneQuiet()))
						return false;

					auto s = scheduler->deques[target]->steal_if(classOK);
					if (!s) return false;
					if (isReservedWorker) {
						stolenOnK = true;
						TaskScheduler::NoteReservedSteal();
					}

					JLIB_STAT(StealHits);
					JLIB_STAT(RunStolen);
					task_to_run = *s;
					return true;
				};
				
				const int nq = (int)scheduler->deques.size();
				if (nq > 1) {

					// Steal order, one pass:
					//   1. cursor: one victim, rotating (within this worker's own L3 group when there
					//      are several). The ONLY step that sticks: after a hit it stays on that
					//      victim for up to cap-1 more passes; the first miss ends it.
					//   2. seek: a deque in the own group whose flag is set, picked by a rotation
					//      that moves every pass. Not sticky -- the next miss seeks it again.
					//   3. the SMT sibling, if fat.
					//   4. another L3, last of all and only when the own group shows no work: a
					//      remote steal misses on the deque and on the task's data. Not sticky.
					const bool grouped = scheduler->l3Groups > 1;
					const size_t myG = grouped ? scheduler->l3GroupOf[(size_t)qIndex] : 0;
					const bool sticking = stickyLeft > 0;
					if (sticking) {
						JLIB_STAT(StickyProbes);
					} else if (grouped) {
						const std::vector<int>& mine = scheduler->l3Members[myG];
						const size_t m = mine.size();
						for (size_t i = 1; i <= m; ++i) {
							l3Local = (l3Local + 1) % m;
							if (mine[l3Local] != qIndex) break;
						}
						stealVictim = mine[l3Local];
					} else {
						stealVictim = (stealVictim + 1) % nq;
						if (stealVictim == qIndex) stealVictim = (stealVictim + 1) % nq;
					}

					bool hit = stealVictim != qIndex && tryStealFrom(stealVictim);
					if (hit && !isReservedWorker) {
						if (sticking) { JLIB_STAT(StickyHits); --stickyLeft; }
						else          stickyLeft = (int)scheduler->GetStickyStealCap() - 1;
					} else {
						stickyLeft = 0;
					}

					// 2. Seek (own group).
					if (!hit && !isReservedWorker && nq <= 64 && scheduler->SeekOnMiss()) {
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
					if (!hit && !isReservedWorker) {
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
						
						|| (!isReservedWorker && !scheduler->normalInboxes[qIndex]->quiescent())
						|| (isReservedWorker && !TaskScheduler::LaneIntakeIdle())))) {

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
					// The last hunter is refused the park so that work pushed straight onto a
					// deque -- a resume, which notifies nobody -- is still found without a wake.
					// It must not scan at full speed while it waits: one hunter probing every
					// other deque as fast as it can was measured at ~10,000 probes per
					// millisecond, and cost an unrelated thread on this machine ~10% (more with
					// Hard affinity, nothing with None). So empty passes back off, and anything
					// found resets it. The bound is what a resume may wait before being stolen.
					// The cap is a LATENCY budget, not a spin count: CpuRelax is ~140 cycles on
					// recent x86 (~45 ns), so 32 of them is ~1.5 us -- the most a resume waits to
					// be stolen. 256 was tried and cost 14 us of p50 wake latency, which is the
					// whole reason this hunter is kept awake.
					// HOT FOR A WHILE, THEN COOL. Measured on this machine, with the flag array in
					// place: spinning flat out holds wake latency at ~6 us p50 but costs an
					// unrelated thread ~20%; backing off immediately gives that back but moves p50
					// to ~9.5 us. Work in a frame arrives in bursts, so the useful shape is to stay
					// hot for a short window after the last task -- a burst never pays the backoff
					// -- and go quiet once the pool has genuinely been idle for a while.
					//
					// kHotPasses is a duration, not a count: a pass is ~100 ns, so ~2000 of them is
					// ~200 us of full-speed hunting after the last task ran.
					// K never parks: it stays up the same way. A parked K costs latency work a full
					// OS wake -- measured 10-85 us p50 (up to ~300 us p99) against 0.6-5 us awake --
					// and, being off the idle stack, it would never be woken to steal compute.
					if (!tryLeaveHuntForPark() || isReservedWorker) {
						constexpr unsigned kHotPasses = 2000;
						if (idleSpins < kHotPasses) {
							++idleSpins;
							platform::CpuRelax();
							continue;
						}
						// Past the window: grow the pause to a cap of 32 relaxes (~1.5 us), which
						// is the longest a resume can wait to be stolen.
						const unsigned steps   = idleSpins - kHotPasses;
						const unsigned shift   = steps < 40 ? (steps / 8) : 5;
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
							|| !scheduler->normalInboxes[qIndex]->quiescent()) {
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
					if (!isReservedWorker) TaskScheduler::RegisterIdleWorker((size_t)qIndex);

					assert(currentFiber == nullptr &&
					       "worker about to park while still owning a fiber -- that frame can never "
					       "be resumed by anyone");
					assert(currentRunningTask == nullptr &&
					       "worker about to park while still holding a running task");

					if (!running.load(std::memory_order_acquire)
						|| scheduler->deques[qIndex]->size_approx() != 0
						|| hasQueuedWork.load(std::memory_order_seq_cst)
						|| !scheduler->hiPriInboxes[qIndex]->quiescent()
						
						|| (!isReservedWorker && !scheduler->normalInboxes[qIndex]->quiescent())
						|| (isReservedWorker && !TaskScheduler::LaneIntakeIdle())
						// Nobody is hunting any more. This worker left the hunt BEFORE it was on
						// the idle stack, so a LeaveHunt that took `searching` 1->0 in that gap
						// found no one to hand off to -- and work pushed onto a busy worker's own
						// deque (a resume or a yield, which wakes nobody) would then sit with no
						// one looking. Found by tests/verify/hunt_model.c. Dekker pair: our
						// seq_cst idle-stack push, then this seq_cst read; the leaver's seq_cst
						// fetch_sub, then its seq_cst idle-stack read. One of the two sees the other.
						|| (!isReservedWorker && TaskScheduler::SearchingCount() == 0)) {

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
	workerStateOff   = offsetof(Thread, workerState);
}
}
