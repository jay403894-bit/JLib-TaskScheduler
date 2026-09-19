// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Thread.h"
#include "../include/Hazard.h"
#include "../include/RetryStats.h"   
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
#include "../include/TaskDAG.h"   
#include "../include/FiberRegistry.h"   

#include "../include/IoReactor.h" 
#include "../include/Reclaimer.h"  
#include <cstdlib>            
#include <cstring>            
                              
#include "../include/Timer.h"     
#include "../include/platform.h"
#include "../include/Topology.h"
#include <stdexcept>
#include <cstdio>      
#include <vector>
#include <chrono>
using namespace JLib;

namespace {
	// What the static getters report when no pool is running.
	const TaskScheduler::Config kNoPoolConfig{};

	thread_local uint32_t t_spinHelpDepth = 0;
	thread_local uint32_t t_heldMutexes   = 0;

	constexpr uint32_t kIdleSpinsBeforeYield = 1000;

	thread_local uint32_t t_idleSpins = 0;

	inline void ContendedSpinStep() {
		if (t_spinHelpDepth == 0 && t_heldMutexes == 0 && TaskScheduler::IsInitialized()) {
			++t_spinHelpDepth;
			const bool ranSomething = TaskScheduler::Instance().TryRunStolenNativeTask();
			--t_spinHelpDepth;
			if (ranSomething) { t_idleSpins = 0; return; }   
		}
		if (++t_idleSpins >= kIdleSpinsBeforeYield) {
			t_idleSpins = 0;
			std::this_thread::yield();
		}
		else {
			platform::CpuRelax();
		}
	}

#ifndef JLIBSCHED_FAST_SPIN_TRIES
#define JLIBSCHED_FAST_SPIN_TRIES 0
#endif
#if defined(JLIBSCHED_TUNABLE_FAST_SPIN)
	inline int FastSpinLimit() {
		return TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetFastSpinTries()
		                                      : JLIBSCHED_FAST_SPIN_TRIES;
	}
#define JLIB_FAST_SPIN_LIMIT (FastSpinLimit())
#else
	constexpr int kFastSpinTries = JLIBSCHED_FAST_SPIN_TRIES;
#define JLIB_FAST_SPIN_LIMIT (kFastSpinTries)
#endif

	template <typename TryOp>
	inline void SpinThenHelp(TryOp&& tryOp) {
		int fastSpins = 0;
		(void)fastSpins;
		while (!tryOp()) {
			if (fastSpins < JLIB_FAST_SPIN_LIMIT) {
				++fastSpins;
				platform::CpuRelax();
			}
			else {
				ContendedSpinStep();
			}
		}
	}

	inline bool OnBareThread() {
		Thread* t = Thread::GetCurrent();
		return t == nullptr || t->currentFiber == nullptr;
	}
}


static_assert(sizeof(Task) <= TaskAllocator::SLOT, "Task doesn't fit a slot");
static_assert(alignof(Task) <= 16, "Task over-aligned for a slot");

TaskScheduler* TaskScheduler::instance = nullptr;

TaskScheduler::AtExitDestroyer TaskScheduler::atExitDestroyer;

void detail::TeardownForTesting(TaskScheduler& scheduler) { scheduler.Join(); }

void detail::DestroyForTesting() {
	TaskScheduler* p = TaskScheduler::instance;
	if (!p) return;
	p->Join();
	TaskScheduler::instance = nullptr;
	delete p;
}

TaskScheduler::AtExitDestroyer::~AtExitDestroyer() {
	
	if (instance) instance->Join();
	instance = nullptr;
}

int& TaskScheduler::ConsecutiveHiPriSteals() noexcept {
	static thread_local int n = 0;
	return n;
}
GlobalFiberPool* TaskScheduler::globalPool = nullptr;

const TaskScheduler::Config& TaskScheduler::CurrentConfig() noexcept {
	return instance ? instance->cfg_ : kNoPoolConfig;
}

TaskScheduler::LiveTunables::LiveTunables(const Tunables& t) noexcept
	: minItersPerWorker(t.minItersPerWorker)
	, leavesPerWorker(t.leavesPerWorker ? t.leavesPerWorker : 1)
	, measuredWidth(t.measuredWidth)
	, rememberedCost(t.rememberedCost)
	, wakeCostNs(t.wakeCostNs < 100 ? 100 : t.wakeCostNs)
	, parallelForSerial(t.parallelForSerial)
	, pforMode((uint8_t)t.pforMode)
	, stickyStealCap(t.stickyStealCap < 1 ? 1 : (t.stickyStealCap > 8 ? 8 : t.stickyStealCap))
	, ioQuietWindowUs(t.ioQuietWindowUs)
	, reservedStealing(t.reservedStealing)
	, laneIntake(t.laneIntake)
	, bareWaitHelp(t.bareWaitHelp)
	, fastSpinTries(t.fastSpinTries < 0 ? JLIBSCHED_FAST_SPIN_TRIES : t.fastSpinTries) {}

// hotWorkers_ starts at the requested K; ClampHotWorkersToPool fits it to the pool once the
// workers exist, as the old SetHotWorkers-then-clamp sequence did.
static std::atomic<std::uint64_t> g_poolGenerations{ 0 };

TaskScheduler::TaskScheduler(const Config& cfg)
	: cfg_(cfg)
	, poolGen_(g_poolGenerations.fetch_add(1, std::memory_order_relaxed) + 1)
	, live_(cfg.tunables)
	, hotWorkers_(cfg.hotWorkers) {}

std::uint64_t TaskScheduler::PoolGeneration() noexcept {
	TaskScheduler* inst = instance;
	return inst ? inst->poolGen_ : 0;
}

bool TaskScheduler::TimersEnabled() noexcept { return CurrentConfig().timers; }
bool TaskScheduler::IoReactorEnabled() noexcept { return CurrentConfig().io; }
unsigned TaskScheduler::IoCompletionThreads() noexcept { return CurrentConfig().ioCompletionThreads; }
bool TaskScheduler::ReserveTimerCore() noexcept { return TimersEnabled(); }
bool TaskScheduler::ReserveIoCore() noexcept { return IoReactorEnabled(); }

TaskAllocator::Usage TaskScheduler::SlabUsage() {
	if (!instance) return TaskAllocator::Usage{};
	return instance->taskAllocator.UsageProfile();
}

std::uint64_t TaskScheduler::OutstandingFiberRows() noexcept {
	if (!instance) return 0;
	std::uint64_t acq = 0, rec = 0;
	for (Thread* w : instance->workers) {
		if (!w) continue;
		acq += w->FiberAcquireCount();
		rec += w->FiberRecycleCount();
	}
	if (Thread* h = instance->mainHelper) { acq += h->FiberAcquireCount(); rec += h->FiberRecycleCount(); }
	return acq > rec ? acq - rec : 0;
}

std::string TaskScheduler::SlabUsageString(const char* label) {
	char line[256];
	std::string out;

	if (!instance) {
		std::snprintf(line, sizeof(line), "[%s] scheduler not initialized\n", label);
		return std::string(line);
	}
	const auto u = instance->taskAllocator.UsageProfile();
	const TaskAllocator::ClassUsage* cs[5] = { &u.c64, &u.c80, &u.c128, &u.c256, &u.c512 };

	std::snprintf(line, sizeof(line), "\n=== %s ===\n", label);                      out += line;
	std::snprintf(line, sizeof(line),
		"  class  configured    resident   peak-live        live  grown\n");         out += line;

	std::size_t resBytes = 0, capBytes = 0;
	long long   peakBytes = 0;
	for (const auto* c : cs) {
		resBytes  += c->resident * c->slotBytes;
		capBytes  += c->capacity * c->slotBytes;
		peakBytes += c->peakLive * (long long)c->slotBytes;
		std::snprintf(line, sizeof(line), "  %4zuB  %10zu  %10zu  %10lld  %8lld  %5zu\n",
			c->slotBytes, c->capacity, c->resident, c->peakLive, c->live, c->extents);
		out += line;
	}
	std::snprintf(line, sizeof(line),
		"  reserved %.1f MB, resident %.1f MB, peak demand %.2f MB\n",
		(double)capBytes / (1024.0 * 1024.0),
		(double)resBytes / (1024.0 * 1024.0),
		(double)peakBytes / (1024.0 * 1024.0));                                      out += line;

	std::snprintf(line, sizeof(line), "  suggested (measured peak +50%%):\n");        out += line;
	std::snprintf(line, sizeof(line), "    JLib::TaskScheduler::SlabSizes s;\n");     out += line;
	std::snprintf(line, sizeof(line),
		"    s.slots64 = %zu; s.slots80 = %zu; s.slots128 = %zu; s.slots256 = %zu; s.slots512 = %zu;\n",
		(std::size_t)(u.c64.peakLive  + u.c64.peakLive  / 2) + 64,
		(std::size_t)(u.c80.peakLive  + u.c80.peakLive  / 2) + 64,
		(std::size_t)(u.c128.peakLive + u.c128.peakLive / 2) + 64,
		(std::size_t)(u.c256.peakLive + u.c256.peakLive / 2) + 64,
		(std::size_t)(u.c512.peakLive + u.c512.peakLive / 2) + 64);                  out += line;
	std::snprintf(line, sizeof(line),
		"    cfg.slab = s;   // JLib::TaskScheduler::Config, passed to Init\n");       out += line;

	for (const auto* c : cs) {
		if (c->extents) {
			std::snprintf(line, sizeof(line),
				"  NOTE: the %zuB class GREW %zu time(s) -- under-configured for this run.\n",
				c->slotBytes, c->extents);
			out += line;
		}
	}
	return out;
}

void TaskScheduler::ReportSlabUsage(const char* label) {
	const std::string s = SlabUsageString(label);
	std::printf("%s", s.c_str());
#if defined(_WIN32)
	::OutputDebugStringA(s.c_str());
#endif
}

unsigned TaskScheduler::GetReservedCores() noexcept { return CurrentConfig().reservedCores; }

size_t TaskScheduler::GetSafeTC() {
	const Config& c = CurrentConfig();
	unsigned int cores = std::thread::hardware_concurrency();
	if (cores <= 1) return 1;

	unsigned int reserved = 1;
	if (c.timers) reserved += 1;
	if (c.io)     reserved += c.ioCompletionThreads;
	reserved += c.reservedCores;
	if (cores <= reserved) return 1;
	return static_cast<size_t>(cores - reserved);
}
void TaskScheduler::Init(size_t poolSize) {
	Config cfg;
	cfg.mode    = Mode::Migrate;
	cfg.main    = MainMode::OutOfPool;   // the long-standing default
	cfg.workers = poolSize;
	Init(cfg);
}

void TaskScheduler::Init(Mode mode, MainMode main, size_t poolSize) {
	Config cfg;
	cfg.mode    = mode;
	cfg.main    = main;
	cfg.workers = poolSize;
	Init(cfg);
}

void TaskScheduler::Init(const Config& requested) {
	if (instance != nullptr)
		throw std::runtime_error("TaskScheduler already initialized!");

	Config cfg = requested;
	if (cfg.main == MainMode::Default)
		cfg.main = (cfg.mode == Mode::Migrate) ? MainMode::InPool : MainMode::OutOfPool;
	if (cfg.ioCompletionThreads == 0) cfg.ioCompletionThreads = 1;
	if (cfg.hotWorkers > kMaxReservedWorkers) cfg.hotWorkers = kMaxReservedWorkers;
	if (cfg.io) {
		cfg.timers = true;
		if (cfg.hotWorkers == 0) cfg.hotWorkers = 1;   // the reactor hands completions to K
	}
	detail::SlabGrowthEnabled().store(cfg.slabGrowth, std::memory_order_relaxed);

	instance = new TaskScheduler(cfg);
	instance->StartPool(cfg.workers);

	{
		char buf[16] = {};
#if defined(_MSC_VER)
		size_t elen = 0; char* ev = nullptr;
		if (_dupenv_s(&ev, &elen, "JLIB_WATCHDOG") == 0 && ev) { strncpy_s(buf, sizeof(buf), ev, _TRUNCATE); free(ev); }
#else
		if (const char* ev = std::getenv("JLIB_WATCHDOG")) { std::strncpy(buf, ev, sizeof(buf) - 1); }
#endif
		const int secs = buf[0] ? std::atoi(buf) : 0;
		if (secs > 0) {
			std::thread([secs] {
				std::this_thread::sleep_for(std::chrono::seconds(secs));
				TaskScheduler* inst = instance;
				if (!inst) return;
				std::fprintf(stderr, "\n[watchdog] after %ds -- workers=%zu, grows=%zu\n", 					secs, inst->workers.size(), TaskDeque::GrowCount());
				for (size_t q = 0; q < inst->workers.size(); ++q) {
					std::fprintf(stderr,
						
						"[watchdog]  q%-2zu deque size=%-7zu cap=%-7zu | inbox lo=%s hi=%s | parks=%u\n",
						q, inst->deques[q]->size_approx(), inst->deques[q]->capacity(),
						inst->normalInboxes[q]->empty()   ? "empty" : "HAS",
						inst->hiPriInboxes[q]->empty()   ? "empty" : "HAS",
						GetWorkerParkCount(q));
					
				}
				std::fflush(stderr);
				inst->DumpPoolState("watchdog");
			}).detach();
		}
	}
}
GlobalFiberPool& JLib::TaskScheduler::GetGlobalPool()
{
	if (!instance->globalPool)
		throw std::runtime_error("GlobalFiberPool not initialized!");
	return *instance->globalPool;

}

TaskScheduler::~TaskScheduler() {
	if (!stopFlag)
		Join();

	hiPriInboxes.clear();
	normalInboxes.clear();
	deques.clear();
}
bool TaskScheduler::PushMain(Task* task) {
	if (!poolActive) return false;
	if (!task) return false;
	task->type = TaskType::Main;
	PushMainQueue(task);
	return true;
}

// Main out of the pool parks on this word inside WaitFor; PushMain and group completion kick it.
static std::atomic<int> g_outOfPoolMainWait{ kWaitRunning };

// Main in the pool is slot 0: its hi-pri inbox is its one queue, and PushTo wakes it. Main out of
// the pool has no slot, so it keeps mainQ and its own wait word.
void TaskScheduler::PushMainQueue(Task* task) {
	if (GetMainMode() == MainMode::InPool) {
		PushTo(0, task);
		return;
	}
	mainQ.push(task);
	KickWaitWord(&g_outOfPoolMainWait);
}

bool TaskScheduler::RunOneMainTask() {
	Task* t = nullptr;
	if (!mainQ.pop(t) || !t) return false;
	if (DiscardIfCancelled(t)) return true;
	t->Execute();
	if (t->waitGroup) {
		t->waitGroup->Done();
	}
	FreeTask(t);
	return true;
}

// Main out of the pool, waiting on a group: take routed work, a few steals, then park on the
// group. Never a continuous scan -- that would slow the workers doing the group's work.
void TaskScheduler::OutOfPoolMainWait(WaitGroup& wg) {
	static constexpr unsigned kAttemptsBeforePark = 32;
	auto count = [&] { return wg.n.load(std::memory_order_acquire) & WaitGroup::COUNT_MASK; };
	unsigned attempts = 0;
	while (count() > 0) {
		if (const size_t h = FiberRegistry::Instance().CurrentHolder(); h != FiberRegistry::kNoHolder)
			if (FiberRegistry::Instance().HolderHasWork(h))
				FiberRegistry::Instance().DrainHolder(h);

		if (RunOneMainTask()) { attempts = 0; continue; }   // routed to main
		const bool stole = mainHelper ? mainHelper->HelpSteal() : TryRunStolenNativeTask();
		if (stole)            { attempts = 0; continue; }   // one bounded steal
		if (++attempts < kAttemptsBeforePark) { platform::CpuRelax(); continue; }
		attempts = 0;

		Reclaimer::Gate(false, false);   // main's own bag, never the orphans

		// Park: WAITING, register on the group (bit before count), recheck, block.
		std::atomic<int>* const word = &g_outOfPoolMainWait;
		word->store(kWaitWaiting, std::memory_order_seq_cst);
		bool registered = false;
		{
			std::lock_guard<std::mutex> lock(wg.mtx);
			const int old = wg.n.fetch_or(WaitGroup::WAITER_BIT, std::memory_order_acq_rel);
			if ((old & WaitGroup::COUNT_MASK) != 0) { wg.blockedThreads.push_back(word); registered = true; }
		}
		if (registered && mainQ.quiescent() && poolActive.load(std::memory_order_acquire))
			BlockOnWaitWord(word);
		word->store(kWaitRunning, std::memory_order_seq_cst);
		if (registered) {
			std::lock_guard<std::mutex> lock(wg.mtx);
			auto& v = wg.blockedThreads;
			for (size_t i = 0; i < v.size(); ++i)
				if (v[i] == word) { v[i] = v.back(); v.pop_back(); break; }
		}
	}
	// Main goes back to its own code, where no gate runs for a while: clear its bag and hand
	// whatever is still protected to the orphan store (pool workers sweep it).
	Reclaimer::Gate(true, false);
	wg.Settle();   // the group may be ours to destroy: let any wake in progress finish first
}

bool TaskScheduler::MainWorker() {
	Thread* t = Thread::GetCurrent();
	return t && t->MainWorker(nullptr);
}

void TaskScheduler::WakeMain() noexcept {
	TaskScheduler* s = instance;
	if (!s) return;
	if (GetMainMode() != MainMode::InPool) { KickWaitWord(&g_outOfPoolMainWait); return; }
	if (s->workers.empty() || !s->workers[0]) return;
	s->workers[0]->Wake();
}

bool TaskScheduler::DiscardIfCancelled(Task* task) {
	if (!task || task->started || !IsTaskCancelled(task)) return false;

	TaskDAG::OnTaskDiscarded(task);

	if (task->waitGroup) {
		task->waitGroup->Done();
	}
	FreeTask(task);
	return true;
}

void TaskScheduler::ProcessMainThread() {
	if (!poolActive) return;

	if (const size_t h = FiberRegistry::Instance().CurrentHolder(); h != FiberRegistry::kNoHolder)
		if (FiberRegistry::Instance().HolderHasWork(h))
			FiberRegistry::Instance().DrainHolder(h);

	Task* t;
	while (mainQ.pop(t)) {
		if (!t) continue;
		if (DiscardIfCancelled(t)) continue;
		t->Execute();
		if (t->waitGroup) {
			t->waitGroup->Done();
		}
		
		FreeTask(t);
	}
}

void TaskScheduler::WaitForMain(WaitGroup& wg) {
	WaitFor(wg);   // main's WaitFor already runs routed main work in both modes
}
void TaskScheduler::Join() {
	if (!poolActive) return;

	// MainMode::InPool: slot 0 is main's own Thread, so only main may tear the pool down -- otherwise its
	// Thread could be destroyed while main is still inside Worker().
	const bool mainDrives = GetMainMode() == MainMode::InPool && !workers.empty()
	                        && Thread::GetCurrent() == workers[0];
	assert((GetMainMode() != MainMode::InPool || mainDrives) && "MainMode::InPool: call Join() from main");

	stopFlag.store(true, std::memory_order_release);

	if (IoReactorEnabled() && IoReactor::IsAvailable())
		IoReactor::Instance().Stop();
	if (TimersEnabled())
		TimerQueue::Instance().Stop();

	{
		
		const bool intakeWasOn = LaneIntakeEnabled();
		SetLaneIntake(false);

		size_t rescued = 0;
		while (Task* t = TakeLaneIntake()) {
			PushTarget(t, kAnyWorker);   
			++rescued;
		}
		SetLaneIntake(intakeWasOn);
		if (rescued) {
			fprintf(stderr, "[JLib::Scheduler] teardown: moved %zu queued I/O completion(s) from the\n"
			                "  shared lane intake to the floor. Not an error -- the reserved band was\n"
			                "  stopping and the floor is the queue that is always drainable.\n",
			        rescued);
			fflush(stderr);
		}
	}

	{
		for (;;) {
			WaitPrimitive* p = nullptr;
			{
				std::lock_guard<std::mutex> lk(primitivesMtx);
				p = primitivesHead;
				if (!p) break;
				primitivesHead = p->nextPrimitive_;
				if (primitivesHead) primitivesHead->prevPrimitive_ = nullptr;
				p->nextPrimitive_ = p->prevPrimitive_ = nullptr;
			}
			p->DrainForShutdown();
		}
	}

	{
		registryMtx.lock();
		for (auto& pair : eventRegistry)
			pair.second->SignalAll();

		registryMtx.unlock();
	}
	NotifyAll();

	{
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		int cleanPasses = 0;
		bool quiesced = false;

		while (std::chrono::steady_clock::now() < deadline) {
			bool idle = true;

			for (size_t i = 0; i < workers.size() && idle; ++i) {
				if (!workers[i]) continue;
				if (workers[i]->busy.load(std::memory_order_acquire)) idle = false;
				else if (!hiPriInboxes[i]->empty() || !normalInboxes[i]->empty()) idle = false;
			}
			for (Thread* sp : spares)
				if (sp->busy.load(std::memory_order_acquire)) idle = false;
			
			for (size_t i = 0; i < deques.size() && idle; ++i)
				
				if (deques[i]->size_approx() != 0
				    || (i < hiPriInboxes.size() && !hiPriInboxes[i]->empty())) idle = false;

			if (idle) {
				if (++cleanPasses >= 2) { quiesced = true; break; }
			} else {
				cleanPasses = 0;
			}
			// Main's own queues have no other consumer: drain them instead of only sleeping.
			if (mainDrives) workers[0]->MainWorker(nullptr);
			std::this_thread::sleep_for(std::chrono::microseconds(200));
		}

		if (!quiesced) {
			std::printf("[JLib::Scheduler] Join(): the pool did not go quiet within 2s. Cancelled\n"
			            "  frames that have not unwound will NOT run their destructors, release what\n"
			            "  they hold, or return their hazard records. State follows.\n");
			DumpPoolState("Join(): quiescence timeout");
		}
	}

	for (auto& worker : workers)
		worker->RequestStop();

	ResumeAll();

	for (auto& worker : workers)
		if (worker && worker->GetThread().joinable())
			worker->GetThread().join();

	for (Thread* sp : spares) sp->RequestStop();
	for (Thread* sp : spares)
		if (sp->GetThread().joinable()) sp->GetThread().join();

	FiberRegistry::Instance().DrainAllForTeardown();

	if (!Reclaimer::Drain()) {
		std::fprintf(stderr,
			"[JLib::Scheduler] teardown: reclamation did not go quiet. Retired memory is being\n"
			"  leaked at exit. The usual cause is an EpochGuard or HazardGuard still held, which\n"
			"  keeps what it protects (and, for an epoch, everything retired after it) alive.\n");
		std::fflush(stderr);
	}

#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
	
	{
		std::uint64_t acq = 0, rec = 0;
		for (Thread* w : workers) { if (w) { acq += w->FiberAcquireCount(); rec += w->FiberRecycleCount(); } }
		if (mainHelper) { acq += mainHelper->FiberAcquireCount(); rec += mainHelper->FiberRecycleCount(); }
		for (Thread* sp : spares) { acq += sp->FiberAcquireCount(); rec += sp->FiberRecycleCount(); }
		const std::uint64_t outstanding = acq > rec ? acq - rec : 0;
		if (outstanding > 0) {
			std::printf("[JLib::Scheduler] FIBER ROW LEAK at teardown: %llu rows acquired, %llu "
			            "recycled, %llu outstanding (expected 0).\n"
			            "  Every AcquireFiber must reach FiberStatus::DEAD exactly once -- that is\n"
			            "  the only path that runs the tagged deleters, destroys the context, returns\n"
			            "  the stack to the magazine and frees the slot. %llu fiber(s) suspended and\n"
			            "  were never resumed to completion, so their stacks are gone for the life of\n"
			            "  the process and the budget they came out of never recovered.\n"
			            "  Look for a task that parks on a row nothing owns the completion of.\n",
			            (unsigned long long)acq, (unsigned long long)rec,
			            (unsigned long long)outstanding, (unsigned long long)outstanding);
		}
	}
#endif

	{
		poolMutex.lock();
		if (mainDrives) workers[0]->ReleaseCurrentThread();
		if (mainHelper) {
			mainHelper->ReleaseCurrentThread();   // only clears the binding if called on main
			delete mainHelper;
			mainHelper = nullptr;
		}
		for (Thread* w : workers) delete w;
		workers.clear();
		for (Thread* sp : spares) delete sp;
		spares.clear();
		mainQ.clear();
		poolMutex.unlock();
	}

	poolActive.store(false, std::memory_order_release);
}

static std::atomic<int> g_lastPushTarget{ -1 };

void TaskScheduler::DumpPoolState(const char* why) const {
	printf("\n=== POOL STATE (%s) ===\n", why);
	
	size_t queued = 0;
	
	for (size_t i = 0; i < deques.size(); ++i) queued += deques[i]->size_approx();
	printf("queuedTasks=%zu  paused=%d  poolActive=%d  workers=%zu\n",
		queued,
		(int)paused.load(std::memory_order_relaxed),
		(int)poolActive.load(std::memory_order_relaxed),
		workers.size());
	
	printf("  q  state           queued busy run   inbox(hi/norm) deque(hi/norm)\n");
	for (size_t i = 0; i < workers.size(); ++i) {
		const auto s = workers[i]->GetDebugState();
		
		static const char* kNames[] = { "EMPTY", "NOTIFIED", "PARKED", "YIELD" };
		
		const char* st = (s.workerState >= 0
		                  && s.workerState < (int)(sizeof(kNames) / sizeof(kNames[0])))
		               ? kNames[s.workerState] : "?";
		
		printf(" %2d  %-14s   %d      %d    %d       %d/%d           %zu/%zu%s\n",
			s.qIndex, st, (int)s.hasQueuedWork,
			(int)s.busy, (int)s.running,
			(int)!hiPriInboxes[i]->empty(), (int)!normalInboxes[i]->empty(),
			(size_t)(hiPriInboxes[i]->empty() ? 0 : 1), deques[i]->size_approx(),

			(s.workerState == 2 && (s.hasQueuedWork || !hiPriInboxes[i]->empty()
				|| !normalInboxes[i]->empty()))
					? "   <-- SLEEPING WITH WORK"
			: (Stats::Enabled() && g_lastPushTarget.load(std::memory_order_relaxed) == s.qIndex)
					? "   <-- LAST PUSH TARGET" : "");
	}
	if (nonWorkerLane < deques.size()) {
		printf(" nw  %-14s   -      -    -   -     -/-           %zu/%zu%s\n",
			nonWorkerLaneClaimed.load(std::memory_order_relaxed) ? "CLAIMED" : "free",
			(size_t)0, deques[nonWorkerLane]->size_approx(), "");   
	}
	fflush(stdout);
}

void TaskScheduler::NotifyAll() {
	for (auto& w : workers)
		w->NotifyWorker();
}

void TaskScheduler::ResumeAll() {
	for (auto& w : workers) {
		if (!w) continue;
		
		w->Wake();
	}
}

TaskScheduler::AffinityPolicy TaskScheduler::GetAffinityPolicy() { return CurrentConfig().affinity; }


MainMode TaskScheduler::GetMainMode() noexcept { return instance ? instance->cfg_.main : MainMode::OutOfPool; }


// "I am looking for work." The counter is the whole hunt protocol: LeaveHunt's 1->0 edge is what
// hands off to an idle worker, and TryLeaveHuntForPark refuses the last hunter so somebody is
// always looking. Entering is unconditional -- there is no cap on how many may search.
//
// There WAS a cap, to hold down probe traffic. It is gone because the traffic is: a scan now reads
// one byte per deque out of a packed flag array (see workFlags) and only touches a deque the flag
// says is worth touching, and an idle hunter backs off. A cap could only cost a wake, never save
// one: a worker refused a slot parks while work may be sitting on another deque with nobody
// looking for it.
void TaskScheduler::EnterHunt() noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return;
	inst->searching_.n.fetch_add(1, std::memory_order_seq_cst);
	JLIB_STAT(HuntEntered);
}

void TaskScheduler::LeaveHunt() noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return;
	const uint32_t old = inst->searching_.n.fetch_sub(1, std::memory_order_seq_cst);
	if (old != 1) return;

	const size_t q = PopIdleWorker();
	if (q == kNoIdleWorker) return;             
	if (q < inst->workers.size() && inst->workers[q]) inst->workers[q]->NotifyWorker();
}

uint32_t TaskScheduler::SearchingCount() noexcept {
	TaskScheduler* inst = instance;
	return inst ? inst->searching_.n.load(std::memory_order_seq_cst) : 0;
}

// The last hunter may not leave: `searching` never reaches 0 while the pool is live. That is the
// Dekker half that makes owner-published (unannounced) deque work safe. Returns false for the
// last hunter, which stays in the find loop as a full worker.
bool TaskScheduler::TryLeaveHuntForPark() noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return true;
	std::atomic<uint32_t>& searching = inst->searching_.n;
	uint32_t cur = searching.load(std::memory_order_seq_cst);
	for (;;) {
		if (cur <= 1) return false;   // leaving would make searching == 0: stay up
		if (searching.compare_exchange_weak(cur, cur - 1,
				std::memory_order_seq_cst, std::memory_order_seq_cst))
			return true;
	}
}

// Sized once per pool, before any worker starts; a new pool starts from a fresh instance, so there
// is nothing left over to reset.
void TaskScheduler::InitIdleStack(size_t n) {
	TaskScheduler* inst = instance;
	if (!inst) return;
	inst->idleNodes_ = std::vector<IdleNode>(n);
}

size_t TaskScheduler::IdleWorkerCount() noexcept {
	TaskScheduler* inst = instance;
	return inst ? inst->idleCount_.load(std::memory_order_relaxed) : 0;
}

void TaskScheduler::RegisterIdleWorker(size_t q) noexcept {
	TaskScheduler* inst = instance;
	if (!inst || q >= inst->idleNodes_.size() || q >= inst->workers.size() || !inst->workers[q]) return;
	std::vector<IdleNode>& nodes = inst->idleNodes_;

	// seq_cst, with the push below: the parker's half of the Dekker pair with LeaveHunt (see the
	// SearchingCount() recheck in the park path).
	nodes[q].armed.store(1, std::memory_order_seq_cst);

	if (inst->workers[q]->idleLinked.load(std::memory_order_relaxed)) return;

	// Mark linked BEFORE the push: a popper clears it after its pop, so the last write always
	// matches whether the node is on the stack.
	inst->workers[q]->idleLinked.store(true, std::memory_order_relaxed);
	uint64_t head = inst->idleTop_.load(std::memory_order_acquire);
	for (;;) {
		nodes[q].next.store((uint32_t)(head & 0xffffffffull), std::memory_order_relaxed);
		const uint64_t next = (((head >> 32) + 1ull) << 32) | (uint64_t)(q + 1);
		if (inst->idleTop_.compare_exchange_weak(head, next,
				std::memory_order_seq_cst, std::memory_order_acquire)) break;
	}
	inst->idleCount_.fetch_add(1, std::memory_order_relaxed);
}

void TaskScheduler::UnregisterIdleWorker(size_t q) noexcept {
	TaskScheduler* inst = instance;
	if (!inst || q >= inst->idleNodes_.size()) return;
	if (inst->idleNodes_[q].armed.exchange(0, std::memory_order_release) != 0)
		inst->idleCount_.fetch_sub(1, std::memory_order_relaxed);
}

size_t TaskScheduler::PopIdleWorker() noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return kNoIdleWorker;
	std::vector<IdleNode>& nodes = inst->idleNodes_;

	for (;;) {
		// seq_cst: the leaver's half of the Dekker pair (its seq_cst fetch_sub of `searching`
		// comes first). See the SearchingCount() recheck in the park path.
		uint64_t head = inst->idleTop_.load(std::memory_order_seq_cst);
		const uint32_t idx1 = (uint32_t)(head & 0xffffffffull);
		if (idx1 == 0) return kNoIdleWorker;

		const size_t q = (size_t)idx1 - 1;
		if (q >= nodes.size()) return kNoIdleWorker;

		const uint32_t nxt = nodes[q].next.load(std::memory_order_relaxed);
		const uint64_t next = (((head >> 32) + 1ull) << 32) | (uint64_t)nxt;
		if (!inst->idleTop_.compare_exchange_weak(head, next,
				std::memory_order_release, std::memory_order_acquire)) continue;

		if (q < inst->workers.size() && inst->workers[q])
			inst->workers[q]->idleLinked.store(false, std::memory_order_relaxed);

		if (nodes[q].armed.exchange(0, std::memory_order_seq_cst) == 0)
			continue;
		inst->idleCount_.fetch_sub(1, std::memory_order_relaxed);

		if (q >= inst->workers.size() || !inst->workers[q]) continue;
		if (inst->workers[q]->workerState.load(std::memory_order_seq_cst) != Thread::WS_PARKED)
			continue;                       

		return q;
	}
}

unsigned TaskScheduler::GetWorkerParkCount(size_t q) noexcept {
	TaskScheduler* inst = instance;
	if (!inst || q >= inst->workers.size() || !inst->workers[q]) return 0;
	return inst->workers[q]->parkCount.load(std::memory_order_relaxed);
}
void TaskScheduler::ResetWorkerParkCounts() noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return;
	for (auto& w : inst->workers)
		if (w) w->parkCount.store(0, std::memory_order_relaxed);
}

size_t TaskScheduler::GetHotWorkers() noexcept {
	TaskScheduler* inst = instance;
	return inst ? inst->hotWorkers_.load(std::memory_order_relaxed) : 0;
}

bool TaskScheduler::HiPriLaneActive() noexcept { return GetHotWorkers() != 0; }

void TaskScheduler::ClampHotWorkersToPool() noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return;
	const size_t n = inst->workers.size();
	size_t k = inst->cfg_.hotWorkers;
	if (n == 0)      k = 0;
	else if (k >= n) k = n - 1;
	inst->hotWorkers_.store(k, std::memory_order_relaxed);
}

TaskScheduler::PowerThrottling TaskScheduler::GetWorkerPowerThrottling() { return CurrentConfig().power; }

namespace JLib { namespace detail {
	AbiComponents JLibScheduler_STALE_LIBRARY_rebuild_the_Scheduler_for_this_configuration() { return LocalAbiComponents(); }
}}

void     TaskScheduler::SetMinItersPerWorker(size_t n) noexcept { live_.minItersPerWorker.store(n, std::memory_order_relaxed); }
size_t   TaskScheduler::GetMinItersPerWorker() const noexcept  { return live_.minItersPerWorker.load(std::memory_order_relaxed); }
void     TaskScheduler::SetLeavesPerWorker(size_t n) noexcept   { live_.leavesPerWorker.store(n ? n : 1, std::memory_order_relaxed); }
size_t   TaskScheduler::GetLeavesPerWorker() const noexcept    { return live_.leavesPerWorker.load(std::memory_order_relaxed); }
void     TaskScheduler::SetMeasuredWidth(bool on) noexcept      { live_.measuredWidth.store(on, std::memory_order_relaxed); }
bool     TaskScheduler::GetMeasuredWidth() const noexcept      { return live_.measuredWidth.load(std::memory_order_relaxed); }
void     TaskScheduler::SetRememberedCost(bool on) noexcept     { live_.rememberedCost.store(on, std::memory_order_relaxed); }
bool     TaskScheduler::GetRememberedCost() const noexcept     { return live_.rememberedCost.load(std::memory_order_relaxed); }
void     TaskScheduler::SetWakeCostNs(unsigned ns) noexcept     { live_.wakeCostNs.store(ns < 100 ? 100 : ns, std::memory_order_relaxed); }
unsigned TaskScheduler::GetWakeCostNs() const noexcept         { return live_.wakeCostNs.load(std::memory_order_relaxed); }
void     TaskScheduler::SetParallelForSerial(bool on) noexcept  { live_.parallelForSerial.store(on, std::memory_order_relaxed); }
bool     TaskScheduler::ParallelForSerial() const noexcept     { return live_.parallelForSerial.load(std::memory_order_relaxed); }
void     TaskScheduler::SetPforMode(PforMode m) noexcept        { live_.pforMode.store((uint8_t)m, std::memory_order_relaxed); }
TaskScheduler::PforMode TaskScheduler::GetPforMode() const noexcept { return (PforMode)live_.pforMode.load(std::memory_order_relaxed); }
void     TaskScheduler::SetStickyStealCap(unsigned n) noexcept  { live_.stickyStealCap.store(n < 1 ? 1 : (n > 8 ? 8 : n), std::memory_order_relaxed); }
unsigned TaskScheduler::GetStickyStealCap() const noexcept     { return live_.stickyStealCap.load(std::memory_order_relaxed); }
void     TaskScheduler::SetIoQuietWindowUs(unsigned us) noexcept { live_.ioQuietWindowUs.store(us, std::memory_order_relaxed); }
unsigned TaskScheduler::IoQuietWindowUs() const noexcept       { return live_.ioQuietWindowUs.load(std::memory_order_relaxed); }
void     TaskScheduler::SetReservedStealing(bool on) noexcept   { live_.reservedStealing.store(on, std::memory_order_relaxed); }
bool     TaskScheduler::ReservedStealing() const noexcept      { return live_.reservedStealing.load(std::memory_order_relaxed); }
void     TaskScheduler::SetLaneIntake(bool on) noexcept         { live_.laneIntake.store(on, std::memory_order_relaxed); }
bool     TaskScheduler::LaneIntakeEnabled() const noexcept     { return live_.laneIntake.load(std::memory_order_relaxed); }
void     TaskScheduler::SetBareWaitHelp(bool on) noexcept       { live_.bareWaitHelp.store(on, std::memory_order_relaxed); }
bool     TaskScheduler::BareWaitHelp() const noexcept          { return live_.bareWaitHelp.load(std::memory_order_relaxed); }
#if defined(JLIBSCHED_TUNABLE_FAST_SPIN)
void     TaskScheduler::SetFastSpinTries(int n) noexcept        { live_.fastSpinTries.store(n < 0 ? 0 : n, std::memory_order_relaxed); }
int      TaskScheduler::GetFastSpinTries() const noexcept      { return live_.fastSpinTries.load(std::memory_order_relaxed); }
#endif

void TaskScheduler::RunCursorRange(int start, int end, int grain, std::function<void(int, int)>& func) {
	if (end <= start) return;
	if (workers.empty()) { func(start, end); return; }   

	const int W = (int)(workers.size() - GetHotWorkers());

	{
		const long long total    = (long long)end - start;
		const long long blocks   = (long long)W * 4;
		long long        balanced = (total + blocks - 1) / blocks;
		if (balanced < 1)   balanced = 1;
		if (balanced > 128) balanced = 128;
		grain = std::max<int>(grain, (int)balanced);
	}

	const long long sliceCount = (((long long)end - start) + grain - 1) / grain;
	const int lanes = (int)std::min<long long>(W, std::max<long long>(1, sliceCount));

	std::atomic<int> cursor{ start };
	WaitGroup wg;
	wg.n.store(lanes, std::memory_order_relaxed);

	for (int w = 0; w < lanes; ++w) {
		Task* t = CreateInternalTask([cur = &cursor, f = &func, end, grain]() {
			for (;;) {
				const int lo = cur->fetch_add(grain, std::memory_order_relaxed);
				if (lo >= end) return;
				const int hi = (lo + grain > end) ? end : lo + grain;
				(*f)(lo, hi);
			}
		}, Lane::Normal);

		if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
		t->waitGroup = &wg;
		
		if (!Push(t)) { wg.n.fetch_sub(1, std::memory_order_acq_rel); FreeTask(t); }
	}

	for (;;) {
		const int lo = cursor.fetch_add(grain, std::memory_order_relaxed);
		if (lo >= end) break;
		func(lo, (lo + grain > end) ? end : lo + grain);
	}
	WaitFor(wg);
}

static thread_local bool t_ownsNonWorkerLane = false;

namespace {
	struct NonWorkerLaneClaim {
		std::atomic<bool>* flag = nullptr;
		
		explicit NonWorkerLaneClaim(std::atomic<bool>& f, bool attempt) {
			if (attempt && !f.exchange(true, std::memory_order_acquire)) {
				flag = &f;
				t_ownsNonWorkerLane = true;
			}
		}
		~NonWorkerLaneClaim() {
			if (flag) {
				t_ownsNonWorkerLane = false;
				flag->store(false, std::memory_order_release);
			}
		}
		bool held() const { return flag != nullptr; }
		NonWorkerLaneClaim(const NonWorkerLaneClaim&) = delete;
		NonWorkerLaneClaim& operator=(const NonWorkerLaneClaim&) = delete;
	};
}

TaskDeque* TaskScheduler::LaneForCurrentThread() {
	
	// A pool worker's own deque; otherwise the non-worker deque if this thread holds it (main out of
	// the pool has a helper Thread with no queue, so that is checked second, not skipped).
	Thread* self = Thread::GetCurrent();
	if (self && self->qIndex >= 0 && (size_t)self->qIndex < deques.size()) return deques[(size_t)self->qIndex].get();
	if (t_ownsNonWorkerLane && nonWorkerLane < deques.size()) return deques[nonWorkerLane].get();
	return nullptr;
}

size_t TaskScheduler::LaneIndexForCurrentThread() {
	Thread* self = Thread::GetCurrent();
	if (self && self->qIndex >= 0 && (size_t)self->qIndex < deques.size()) return (size_t)self->qIndex;
	if (t_ownsNonWorkerLane && nonWorkerLane < deques.size()) return nonWorkerLane;
	return SIZE_MAX;
}


static constexpr size_t kMinGrain = 16;

static constexpr long long kProbeFloorNs = 500;

namespace {
	struct BodyCost {
		std::atomic<size_t>    key{ 0 };          
		std::atomic<long long> nsPerMega{ 0 };    
		std::atomic<unsigned>  uses{ 0 };         
	};
	constexpr size_t kBodyCostSlots = 64;
	BodyCost g_bodyCost[kBodyCostSlots];

	constexpr unsigned kReprobeInterval = 64;

	BodyCost* FindBodySlot(size_t key) noexcept {
		if (key == 0) key = 1;                     
		const size_t start = key % kBodyCostSlots;
		for (size_t i = 0; i < 8; ++i) {           
			BodyCost& e = g_bodyCost[(start + i) % kBodyCostSlots];
			const size_t k = e.key.load(std::memory_order_acquire);
			if (k == key) return &e;
			if (k == 0) {
				size_t expected = 0;
				if (e.key.compare_exchange_strong(expected, key, std::memory_order_acq_rel,
				                                  std::memory_order_acquire))
					return &e;
				if (expected == key) return &e;    
			}
		}
		return nullptr;                            
	}
}

static constexpr size_t kFirstWaveThieves = 4;

// PforMode::Auto: below this many ns per piece the shared cursor contends and lazy splitting wins
// (sweep: up to ~2x at 0.1 us); above it Cursor wins or ties on compute bodies.
static constexpr long long kLazyPieceNs = 500;

void TaskScheduler::ParallelFor(int begin, int end, int grain, std::function<void(int, int)> func) {
	ParallelFor(begin, end, grain, std::move(func), GetPforMode());
}

void TaskScheduler::ParallelFor(int begin, int end, int grain, std::function<void(int, int)> func,
                                PforMode mode) {
	if (end - begin <= 0) return;
	grain = std::max(1, grain);

	if (ParallelForSerial() || !poolActive.load(std::memory_order_acquire) || workers.empty()) {
		func(begin, end);
		return;
	}

	long long nsPerMegaIdx = 0;   // measured cost of a million indices; 0 = not probed
	size_t fanWidth = workers.size();
	if (GetMeasuredWidth()) {
		const int len = end - begin;

		BodyCost* slot = GetRememberedCost() ? FindBodySlot(func.target_type().hash_code()) : nullptr;
		if (slot) {
			const long long nsPerMega = slot->nsPerMega.load(std::memory_order_relaxed);
			const unsigned  uses      = slot->uses.fetch_add(1, std::memory_order_relaxed);
			
			if (nsPerMega > 0 && (uses % kReprobeInterval) != 0) {
				const long long W = (nsPerMega * (long long)len) / 1'000'000LL;
				const long long c = (long long)GetWakeCostNs();
				size_t k = (size_t)std::sqrt((double)W / (double)(c > 0 ? c : 1));
				if (k > workers.size()) k = workers.size();
				if (k < 2) { func(begin, end); return; }
				fanWidth = k;
				nsPerMegaIdx = nsPerMega;
				goto haveWidth;
			}
		}
		
		const int probeCap = (len / 32 < 1) ? 1 : (len / 32);
		int probed = 0;
		long long probeNs = 0;      
		int probeLen = 0;           
		
		long long tPrev = MonotonicNs();
		for (int chunk = 1; probed < probeCap; ) {
			int take = chunk;
			if (take > probeCap - probed) take = probeCap - probed;
			func(begin + probed, begin + probed + take);
			const long long tNow = MonotonicNs();
			const long long p0 = tPrev, p1 = tNow;
			tPrev = tNow;
			probed += take;
			
			const long long d = (p1 > p0) ? (p1 - p0) : 1;
			long long want = ((long long)take * kProbeFloorNs) / d;
			if (want < (long long)take * 2) want = (long long)take * 2;   
			if (want > (long long)probeCap)  want = probeCap;
			chunk = (int)want;
			
			if (p1 > p0 && take >= probeLen) { probeNs = p1 - p0; probeLen = take; }
			if (probeNs >= kProbeFloorNs) break;   
		}
		if (probeLen <= 0) { probeLen = probed > 0 ? probed : 1; }
		
		begin += probed;
		if (begin >= end) return;                

		if (probeNs < kProbeFloorNs) {
			func(begin, end);
			return;
		}
		const long long W = (probeNs * (long long)len) / (probeLen > 0 ? probeLen : 1);
		nsPerMegaIdx = (probeNs * 1'000'000LL) / (probeLen > 0 ? probeLen : 1);

		if (slot) {
			const long long sample = (probeNs * 1'000'000LL) / (probeLen > 0 ? probeLen : 1);
			const long long prev = slot->nsPerMega.load(std::memory_order_relaxed);
			slot->nsPerMega.store(prev > 0 ? (prev * 3 + sample) / 4 : sample,
			                      std::memory_order_relaxed);
		}

		const long long c = (long long)GetWakeCostNs();
		size_t k = (size_t)std::sqrt((double)W / (double)(c > 0 ? c : 1));
		if (k > workers.size()) k = workers.size();
		if (k < 2) {
			
			func(begin, end);
			return;
		}
		fanWidth = k;
	} else {
		const int minIters = (int)workers.size() * (int)GetMinItersPerWorker();
		if (end - begin < minIters) {
			func(begin, end);
			return;
		}
	}
	
haveWidth:
	;

	{
		
		const size_t maxLeaves = fanWidth * GetLeavesPerWorker();
		const int floorGrain = (int)(((size_t)(end - begin) + maxLeaves - 1) / maxLeaves);
		grain = std::max(grain, floorGrain);

		grain = std::max(grain, (int)kMinGrain);
	}

	if (mode == PforMode::Auto) {
		const long long pieceNs = nsPerMegaIdx > 0 ? (nsPerMegaIdx * (long long)grain) / 1'000'000LL : -1;
		mode = (pieceNs >= 0 && pieceNs < kLazyPieceNs) ? PforMode::LazyPCore : PforMode::Cursor;
	}
	if (mode == PforMode::LazyPCore) RunLazyRange(begin, end, grain, func, fanWidth);
	else                             RunCursorRange(begin, end, grain, func);
}

// One piece of a lazily split range. Runs grain by grain; whenever this thread's own deque is empty
// (nobody is waiting on it for work) it gives the upper half away there, counted in `wg` before the
// push. The piece holds a count of its own while it runs, so the group cannot reach zero early.
// A thread with no deque (main out of the pool, a spare) just runs the range.
void TaskScheduler::RunLazyPiece(std::function<void(int, int)>* func, int lo, int hi, int grain,
                                 WaitGroup* wg) {
	TaskDeque* dq = LaneForCurrentThread();   // own deque, or the non-worker deque main holds
	while (hi - lo > grain) {
		if (dq && hi - lo >= 2 * grain && dq->empty()) {
			const int mid = lo + (hi - lo) / 2;
			Task* t = CreateInternalTask([this, func, mid, hi, grain, wg]() {
				RunLazyPiece(func, mid, hi, grain, wg);
			}, Lane::Normal);
			if (t) {
				wg->n.fetch_add(1, std::memory_order_relaxed);
				t->waitGroup = wg;
				if (!dq->push_bottom(t)) TaskDeque::FatalPushRefused();
				hi = mid;
				continue;
			}
		}
		(*func)(lo, lo + grain);
		lo += grain;
	}
	if (lo < hi) (*func)(lo, hi);
}

void TaskScheduler::RunLazyRange(int start, int end, int grain, std::function<void(int, int)>& func,
                                 size_t width) {
	if (end <= start) return;
	WaitGroup wg;
	wg.n.store(1, std::memory_order_relaxed);   // the caller's own share

	int callerHi = end;
	Thread* self = Thread::GetCurrent();
	const int myQ = (self && self->IsPoolWorker()) ? self->qIndex : -1;
	// A caller with no deque (main out of the pool) takes the non-worker deque for the call, if no
	// other outside thread holds it. Nested calls on the holder keep using it.
	NonWorkerLaneClaim claim(nonWorkerLaneClaimed, myQ < 0 && !t_ownsNonWorkerLane);
	TaskDeque* myDq = LaneForCurrentThread();
	{

		// P-core compute workers, idle ones first. With one core class every compute worker is P.
		constexpr size_t kMaxSeeds = 64;
		int idleQ[kMaxSeeds], busyQ[kMaxSeeds];
		size_t nIdle = 0, nBusy = 0;
		const size_t computeN = ReservedBase(workers.size());
		const size_t maxOthers = std::min<size_t>(kMaxSeeds, width > 0 ? width - 1 : 0);
		for (size_t q = 0; q < computeN && nIdle + nBusy < maxOthers; ++q) {
			Thread* w = workers[q];
			if (!w || (int)q == myQ || w->isMain || !isPCore[q]) continue;
			if (w->adoptSlot.load(std::memory_order_relaxed) == Thread::kAdoptAway) continue;
			if (w->busy.load(std::memory_order_relaxed)) busyQ[nBusy++] = (int)q;
			else                                         idleQ[nIdle++] = (int)q;
		}
		const long long n = (long long)end - start;
		size_t parts = 1 + nIdle + nBusy;
		const long long maxParts = n / grain;
		if ((long long)parts > maxParts) parts = (size_t)(maxParts > 1 ? maxParts : 1);

		// Contiguous ranges: the caller keeps the first, then idle targets, then busy ones.
		auto bound = [&](size_t i) { return start + (int)((n * (long long)i) / (long long)parts); };
		callerHi = bound(1);
		for (size_t i = 1; i < parts; ++i) {
			const int lo = bound(i), hi = bound(i + 1);
			Task* t = CreateInternalTask([this, f = &func, lo, hi, grain, w = &wg]() {
				RunLazyPiece(f, lo, hi, grain, w);
			}, Lane::Normal);
			if (!t) { func(lo, hi); continue; }
			wg.n.fetch_add(1, std::memory_order_relaxed);
			t->waitGroup = &wg;
			const size_t k = i - 1;
			if (k < nIdle) {
				const int q = idleQ[k];
				normalInboxes[(size_t)q]->push(t);
				workers[(size_t)q]->MarkQueuedWork();
				workers[(size_t)q]->NotifyWorker();
			} else if (myDq) {
				// That P-core is running something: its inbox would hide the range until it
				// finishes. On the caller's own deque any thief can take it now.
				if (!myDq->push_bottom(t)) TaskDeque::FatalPushRefused();
			} else {
				const int q = busyQ[k - nIdle];
				normalInboxes[(size_t)q]->push(t);
				workers[(size_t)q]->MarkQueuedWork();
				workers[(size_t)q]->NotifyWorker();
			}
		}
	}

	RunLazyPiece(&func, start, callerHi, grain, &wg);
	wg.Done();          // no waiter yet: only drops the caller's count
	WaitFor(wg);
}

void TaskScheduler::BuildTopology(unsigned int num_workers) {
	siblingQIndex.assign(num_workers, -1);
	clusterMates.assign(num_workers, {});

	auto qIndexOf = [this, num_workers](int logicalCpu) -> int {
		if (logicalCpu < 0) return -1;
		for (unsigned int q = 0; q < num_workers && q < qToCpu.size(); ++q)
			if (qToCpu[q] == logicalCpu) return (int)q;
		return -1;
	};

	isPCore.assign(num_workers, 1);
	isPCpu.assign(topology::CpuMask::kMaxCpus, 1);   

	topology::Info topo;
	topology::Query(topo);

	{
		
		for (int cpu = 0; cpu < (int)topology::CpuMask::kMaxCpus; ++cpu) {
			const int eff = topo.efficiencyClass[cpu];
			if (eff >= 0 && eff < topo.maxClass) {
				isPCpu[cpu] = 0;
				int q = qIndexOf(cpu);
				if (q >= 0) isPCore[q] = 0;
			}
		}
	}

	const std::vector<topology::CpuMask>& coreMasks  = topo.coreMasks;    
	const std::vector<topology::CpuMask>& cacheMasks = topo.cacheMasks;   
	const bool haveCores = topo.haveCores;
	const bool haveCache = topo.haveCache;

	if (haveCores) {
		for (const topology::CpuMask& mask : coreMasks) {
			std::vector<int> qsInGroup;
			for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu) {
				if (mask.Test(cpu)) {
					int q = qIndexOf((int)cpu);
					if (q >= 0) qsInGroup.push_back(q);
				}
			}
			
			if (qsInGroup.size() == 2) {
				siblingQIndex[qsInGroup[0]] = qsInGroup[1];
				siblingQIndex[qsInGroup[1]] = qsInGroup[0];
			}
		}
	}

	if (haveCache) {
		for (const topology::CpuMask& mask : cacheMasks) {
			
			std::vector<int> qsInGroup;
			for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu) {
				if (mask.Test(cpu)) {
					int q = qIndexOf((int)cpu);
					if (q >= 0) qsInGroup.push_back(q);
				}
			}
			if (qsInGroup.size() < 2) continue;
			for (int q : qsInGroup) {
				
				if (qsInGroup.size() > clusterMates[q].size() + 1) {
					clusterMates[q].clear();
					for (int other : qsInGroup) {
						if (other != q && other != siblingQIndex[q]) clusterMates[q].push_back(other);
					}
				}
			}
		}
	}
	else {
		
		for (unsigned int q = 0; q < num_workers; ++q) {
			for (unsigned int other = 0; other < num_workers; ++other) {
				if (other != q && (int)other != siblingQIndex[q]) clusterMates[q].push_back((int)other);
			}
		}
	}

	llcMaskOfWorker.assign(num_workers, topology::CpuMask{});
	if (haveCache) {
		for (unsigned int q = 0; q < num_workers; ++q) {
			// The worker's own CPU, from the same mapping the pinning uses (not q + 1: the offset
			// depends on MainMode and the list may be physical CPUs, not 0..N in order).
			const int cpuI = (q < qToCpu.size()) ? qToCpu[q] : -1;
			if (cpuI < 0) continue;
			const unsigned cpu = (unsigned)cpuI;
			if (cpu >= topology::CpuMask::kMaxCpus) continue;
			unsigned best = 0;
			for (const topology::CpuMask& mask : cacheMasks) {
				if (!mask.Test(cpu)) continue;
				const unsigned n = mask.Count();
				if (n > best) { best = n; llcMaskOfWorker[q] = mask; }
			}
		}
	}

	matesSameClass.assign(num_workers, {});
	matesOtherClass.assign(num_workers, {});
	for (unsigned int q = 0; q < num_workers; ++q)
		for (int m : clusterMates[q])
			((isPCore[m] == isPCore[q]) ? matesSameClass[q] : matesOtherClass[q]).push_back(m);
}

void TaskScheduler::StartPool(size_t poolSize) {
	poolMutex.lock();
	thread_id = 0;   // main always owns epoch slot 0; workers get 1..N below
	// Main keeps its own retire bag and clears it at its own gates (never the orphans).
	detail::RunsGates() = true;

	if (poolSize == 0)
		poolSize = GetSafeTC();
	{
		unsigned int hw = std::thread::hardware_concurrency();
		if (hw == 0) hw = 4;
		if (poolSize > hw) poolSize = hw;
	}

	std::vector<int> physicalCpus;   
	std::vector<int> logicalCpus;    
	{
		topology::Info topo;
		topology::Query(topo);

		if (topo.haveCores) {
			topology::CpuMask all;
			for (const topology::CpuMask& m : topo.coreMasks)
				for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu)
					if (m.Test(cpu)) all.Set(cpu);
			for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu)
				if (all.Test(cpu)) logicalCpus.push_back((int)cpu);
		}

		if (GetAffinityPolicy() == AffinityPolicy::PhysicalOnly) {
			if (topo.haveCores) {
				
				for (const topology::CpuMask& m : topo.coreMasks)
					for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu)
						if (m.Test(cpu)) { physicalCpus.push_back((int)cpu); break; }
			}
			if (physicalCpus.size() >= 2)
				poolSize = physicalCpus.size() - 1;   
			else
				physicalCpus.clear();                 
		}
	}

	unsigned int num_workers = static_cast<unsigned int>(poolSize);
	
	// Pool slots: main + workers, then the spares (a spare is not a queue slot but runs tasks, so
	// it needs an epoch slot of its own), then the ThreadScope slots.
	const size_t spareN = (cfg_.mode == Mode::Pinned) ? 0 : cfg_.spareThreads;
	EpochManager::Instance().Init(num_workers + 1 + spareN, cfg_.externalThreads);
	
	{
		const std::vector<int>& cpuList = !physicalCpus.empty() ? physicalCpus : logicalCpus;
		const size_t off = (GetMainMode() == MainMode::InPool) ? 0u : 1u;
		qToCpu.assign(num_workers, -1);
		for (unsigned int i = 0; i < num_workers; ++i) {
			const size_t li = (size_t)i + off;
			if (li < cpuList.size()) qToCpu[i] = cpuList[li];
			else if (cpuList.empty()) qToCpu[i] = (int)(i + off);   
			
		}
	}

	BuildTopology(num_workers);
	
	pWorkers.clear(); eWorkers.clear();
	nextPWorker.store(0); nextEWorker.store(0);
	for (unsigned int q = 0; q < num_workers; ++q)
		(isPCore[q] ? pWorkers : eWorkers).push_back((int)q);
	stopFlag.store(false, std::memory_order_release);
	nextWorker.store(0, std::memory_order_relaxed);   
	
	unsigned int coreCount = num_workers;
	if (coreCount == 0) coreCount = 4; 

	const size_t kBand   = (cfg_.hotWorkers < coreCount) ? cfg_.hotWorkers : coreCount;
	
	const size_t compute = (coreCount > kBand) ? (coreCount - kBand) : 1;

	size_t standardFiberCount = compute * NormalFibersPerComputeWorker();

	globalPool = GlobalFiberPool::Create(standardFiberCount,
	                                     kBand   * TinyFibersPerKWorker(),
	                                     compute * DeepFibersPerComputeWorker(),
	                                     FiberMemoryLimit());
	
	FiberRegistry::Instance().Build(globalPool, num_workers);

	for (Thread* w : workers) delete w;
	workers.clear();
	deques.clear();
	normalInboxes.clear();
	hiPriInboxes.clear();
	workers.reserve(num_workers);
	
	deques.reserve(num_workers + 1);
	normalInboxes.reserve(num_workers);
	hiPriInboxes.reserve(num_workers);
	
	mainQ.init(&taskAllocator);

	for (unsigned int i = 0; i < num_workers; ++i) {
		deques.push_back(std::make_unique<TaskDeque>());
		
		normalInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		hiPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		normalInboxes[i]->init(&taskAllocator);
		hiPriInboxes[i]->init(&taskAllocator);
		
		
		deques[i]->SetOwnerTag(i, "deque");
	}
	
	// MainMode::InPool: slot 0 is main, so there is no separate non-worker deque (index num_workers is out of range).
	const bool mainInPool = GetMainMode() == MainMode::InPool;
	nonWorkerLane = num_workers;
	if (!mainInPool) {
		deques.push_back(std::make_unique<TaskDeque>());
		deques[nonWorkerLane]->SetOwnerTag(nonWorkerLane, "non-worker");
		deques[nonWorkerLane]->SetPushOnly();
	}
	nonWorkerLaneClaimed.store(false, std::memory_order_relaxed);

	// One byte per deque, packed into a cache-line-aligned block so a hunter's pass reads them all
	// in one fetch, and handed to each deque now that the count is final.
	{
		const size_t n = deques.size();
		auto* mem = static_cast<std::atomic<std::uint8_t>*>(
			::operator new(n * sizeof(std::atomic<std::uint8_t>), std::align_val_t(platform::kCacheLine)));
		for (size_t i = 0; i < n; ++i) ::new (&mem[i]) std::atomic<std::uint8_t>(0);
		workFlags.reset(mem);
		for (size_t i = 0; i < n; ++i) deques[i]->SetWorkFlag(&workFlags[i]);
	}

	workers.reserve(num_workers);
	for (unsigned int i = 0; i < num_workers; ++i) {

		Thread* worker = new Thread(*this);
		worker->SetQueueIndex(i);
		// Epoch slot 0 is always main: with main in the pool main IS worker 0, otherwise workers start at 1.
		worker->epochId = mainInPool ? (size_t)i : (size_t)i + 1;
		worker->isMain  = mainInPool && i == 0;
		workers.push_back(worker);
	}

	ClampHotWorkersToPool();

	InitIdleStack(num_workers);
	
	const size_t fairShare = standardFiberCount / (num_workers ? num_workers : 1);
	const size_t fiberCacheCapacity = (fairShare / 2 < 16) ? 16 : fairShare / 2;

	if (IoReactorEnabled() && IoReactor::IsAvailable())
		IoReactor::Instance().Start();
	if (TimersEnabled())
		TimerQueue::Instance().Start();

	for (unsigned int i = mainInPool ? 1u : 0u; i < num_workers; ++i) {

		const int cpuI = ((size_t)i < qToCpu.size()) ? qToCpu[i] : -1;
		const size_t cpu = (cpuI >= 0) ? (size_t)cpuI
		                               : (size_t)topology::CpuMask::kMaxCpus;
		workers[i]->StartWorker(cpu, fiberCacheCapacity);
	}

	HazardDomain::Instance().Init();
	// MainMode::InPool: main becomes slot 0 before the readiness wait, or it would wait on itself.
	if (mainInPool && num_workers > 0) workers[0]->AdoptCurrentThread(fiberCacheCapacity);
	if (!mainInPool) {
		// Main out of the pool gets a helper so it can run stolen tasks while it waits.
		mainHelper = new Thread(*this);
		mainHelper->qIndex   = -1;
		mainHelper->epochId  = 0;
		mainHelper->isHelper = true;
		mainHelper->AdoptAsHelper();
	}
	for (size_t i = 0; i < spareN; ++i) {
		Thread* sp = new Thread(*this);
		sp->qIndex  = -1;
		sp->isSpare = true;
		sp->epochId = (size_t)num_workers + 1 + i;
		spares.push_back(sp);
		sp->StartSpare(fiberCacheCapacity);
	}
	for (auto& w : workers) {
		while (!w->Ready())
			std::this_thread::yield();
	}
	for (Thread* sp : spares)
		while (!sp->Ready()) std::this_thread::yield();

	assert((mainInPool
	          ? (deques.size() == workers.size())
	          : (deques.size() >= 1 && nonWorkerLane == deques.size() - 1))
	       && "queue layout: the non-worker deque must be last, and must not exist with main in the pool");

	poolActive.store(true, std::memory_order_release);

	poolMutex.unlock();
}

WaitResult TaskScheduler::WaitOnEventCancellable(Event& event, Pin pin) {
	
	if (IsTaskCancelled(GetCurrentTask())) return WaitResult::Cancelled;

	WaitOnEvent(event, pin);

	return IsTaskCancelled(GetCurrentTask()) ? WaitResult::Cancelled : WaitResult::Ok;
}

void TaskScheduler::WaitOnEvent(Event& event, Pin pin) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	CheckSuspendable(myTask, "TaskScheduler::WaitOnEvent");
	Fiber* myFiber = myTask->record->fiber;
	if (!myFiber) {

		throw std::runtime_error("WaitOnEvent called from a task with no assigned fiber -- "
			"only fiber tasks may suspend.");
	}

	// Checked before linking: an Event never removes a single waiter. A cancel after this point
	// arrives through the event's CancelWaiters (timer ejects are armed after the link).
	if (IsTaskCancelled(myTask)) return;

	myFiber->BeginSuspend(pin);
	event.AddWaiter(myTask);

	CheckSuspendableCurrent("TaskScheduler::WaitOnEvent");
	JLIB_EPOCH_CHECK_NO_GUARD("TaskScheduler::WaitOnEvent");
	
	Thread::TsanSwitchToScheduler();
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}

void TaskScheduler::WaitOnEvent(const std::string& eventName, Pin pin) { WaitOnEvent(GetEvent(eventName), pin); }

bool TaskScheduler::Push(Task* task) {
	JLIB_STAT(Pushes);
	// A pool thread other than main keeps its own pushes on its own deque. Main distributes.
	Thread* self = Thread::GetCurrent();
	if (task && self && self->IsPoolWorker() && !self->isMain) {
		if (!deques[(size_t)self->qIndex]->push_bottom(task)) TaskDeque::FatalPushRefused();
		return true;
	}
	return PushTarget(task);
}

void TaskScheduler::RunCounted(WaitGroup& wg, Task* t) {
	wg.n.fetch_add(1, std::memory_order_relaxed);
	t->waitGroup = &wg;

	if (!Push(t)) {
		FreeTask(t);
		wg.Done();   // others may already wait on this group
	}
}

static thread_local unsigned t_bareWaitPolls  = 0;
static thread_local unsigned t_bareWaitYields = 0;
static thread_local unsigned t_bareWaitHelped = 0;

static inline void BareWaitBackoff(unsigned& spins) noexcept {
	
	constexpr unsigned kSpinBeforeYield = 512;
	if (spins < kSpinBeforeYield) { ++spins; platform::CpuRelax(); }
	else                          { spins = 0; ++t_bareWaitYields; std::this_thread::yield(); }
}

unsigned TaskScheduler::LastBareWaitPolls()  noexcept { return t_bareWaitPolls;  }
unsigned TaskScheduler::LastBareWaitYields() noexcept { return t_bareWaitYields; }
unsigned TaskScheduler::LastBareWaitHelped() noexcept { return t_bareWaitHelped; }
bool TaskScheduler::WaitFor(WaitGroup* wg, bool (*pred)(void*), void* arg, Pin pin) {
	CheckSuspendableCurrent("TaskScheduler::WaitFor");
	Thread* thread = Thread::GetCurrent();
	Thread::WaitCtx ctx;
	ctx.wg = wg;
	ctx.pred = pred;
	ctx.arg = arg;
	if (thread && thread->isMain && !thread->currentFiber) {
		const bool r = thread->MainWorker(&ctx);
		if (wg) wg->Settle();
		return r;
	}

	// Anyone else: plain wait on the group, polling the predicate.
	unsigned spins = 0;
	while (!ctx.Done() && poolActive.load(std::memory_order_acquire)) {
		if (wg && !pred) { WaitFor(*wg, pin); return true; }
		BareWaitBackoff(spins);
	}
	return ctx.Done();
}

#if defined(JLIBSCHED_STATS)
namespace {
	// Times one WaitFor call; the end may be recorded on another thread (a fiber resumes elsewhere).
	struct StatWaitTimer {
		std::uint64_t t0 = JLIB_STAT_TICKS();
		~StatWaitTimer() { JLIB_STAT_HIST(WaitFor, JLIB_STAT_TICKS() - t0); }
	};
}
#endif

void TaskScheduler::WaitFor(WaitGroup& wg, Pin pin) {
	CheckSuspendableCurrent("TaskScheduler::WaitFor");   // lambda tasks run without a fiber
	JLIB_STAT_ONLY(StatWaitTimer statWait;)
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	// In-pool main outside a fiber: run pool work until the group is done (blocks on its wait word).
	if (thread && thread->isMain && !current) {
		Thread::WaitCtx ctx;
		ctx.wg = &wg;
		thread->MainWorker(&ctx);
		wg.Settle();
		return;
	}

	if (current != nullptr) {
		
		current->BeginSuspend(pin);
		wg.AddWaiter(current->owningTask);

		CheckSuspendableCurrent("TaskScheduler::WaitFor");
		JLIB_EPOCH_CHECK_NO_GUARD("TaskScheduler::WaitFor");
		Thread::TsanSwitchToScheduler();
		ContextSwitch(&current->ctx, current->homeCtx);
		return;
	}

	// A worker with no fiber here is a coroutine that called the blocking form instead of
	// co_await WaitAsync (asserted above). Failure case: the thread blocks, nothing is handed off.
	// Main out of the pool (its helper, or main itself before the helper exists): bounded help,
	// then park on the group.
	if ((thread && thread->isHelper)
	    || (!thread && detail::RunsGates() && GetMainMode() == MainMode::OutOfPool)) {
		OutOfPoolMainWait(wg);
		return;
	}
	if (thread != nullptr) {
		wg.BlockThread();
		return;
	}
	else {
		
		unsigned waitSpins = 0;

		// A slotted thread waiting here (main out of the pool) clears its own bag. Never the
		// orphans: those belong to the pool workers.
		const bool sweeps = detail::RunsGates();
		if (sweeps) Reclaimer::Gate(false, false);

		t_bareWaitPolls = t_bareWaitYields = t_bareWaitHelped = 0;
		while ((wg.n.load(std::memory_order_acquire) & WaitGroup::COUNT_MASK) > 0) {
			++t_bareWaitPolls;
			if (sweeps && (t_bareWaitPolls & 0xFFu) == 0) Reclaimer::Gate(false, false);
			bool ranSomething = false;
			
			if (t_heldMutexes == 0 && BareWaitHelp()) {
				++t_spinHelpDepth;
				ranSomething = TryRunStolenNativeTask();
				--t_spinHelpDepth;
			}
			if (!ranSomething)
				BareWaitBackoff(waitSpins);
			else
				++t_bareWaitHelped;
		}
		wg.Settle();
	}
}
void JLib::TaskScheduler::PushBatch(Task* tasks[], size_t count, size_t worker, size_t minPerSegment)
{
	if (!tasks || count == 0) return;
	JLIB_STAT(PushBatches);
	JLIB_STAT_N(PushBatchTasks, count);

	// A batch is a linked run for one worker to load onto its own deque: always the normal inbox.
	auto submitRun = [&](size_t first, size_t len, int chosen) {
		for (size_t i = first; i + 1 < first + len; ++i)
			tasks[i]->next.store(tasks[i + 1], std::memory_order_relaxed);

		normalInboxes[chosen]->push_batch(tasks[first], tasks[first + len - 1]);

		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();
	};

	if (worker != kAnyWorker) {
		// K never drains a normal inbox: a batch aimed at K goes to a compute worker instead.
		const int chosen = IsReservedIndex(worker, workers.size()) ? PickNextWorker(Lane::Normal)
		                                                           : (int)worker;
		submitRun(0, count, chosen);
		return;
	}

	if (minPerSegment == 0) minPerSegment = 1;
	const size_t nw = workers.size();

	const size_t hotN = GetHotWorkers();
	const size_t reachable = nw > hotN ? nw - hotN : nw;

	size_t segments = (nw == 0) ? 1 : (count / minPerSegment);
	if (segments < 1)         segments = 1;
	if (segments > reachable) segments = reachable;
	const size_t per = count / segments;
	const size_t rem = count % segments;

	size_t first = 0;
	for (size_t s = 0; s < segments; ++s) {
		const size_t len = per + (s < rem ? 1 : 0);
		if (len == 0) continue;
		const int chosen = PickNextWorker(Lane::Normal);
		submitRun(first, len, chosen);
		first += len;
	}
}

void TaskScheduler::PushBatch(Task* tasks[], size_t count, BatchSpread spread) {
	if (!tasks || count == 0 || workers.empty()) return;
	if (spread == BatchSpread::Wide) {
		PushBatch(tasks, count, kAnyWorker, 64);
		return;
	}
	const size_t chosen = (size_t)PickNextWorker(Lane::Normal);
	PushBatch(tasks, count, chosen, 0);
}

bool TaskScheduler::PushTo(Task* task, CorePref pref, bool hiPri) {
	if (!task || workers.empty()) return false;
	const size_t computeN = ReservedBase(workers.size());
	if (computeN == 0) return false;
	auto candidate = [&](size_t q, CorePref p) {
		Thread* w = workers[q];
		if (!w || w->isMain) return false;
		return p == CorePref::Any || (p == CorePref::P) == (isPCore[q] != 0);
	};
	auto count = [&](CorePref p) {
		size_t m = 0;
		for (size_t q = 0; q < computeN; ++q) m += candidate(q, p) ? 1 : 0;
		return m;
	};
	size_t m = count(pref);
	if (m == 0 && pref == CorePref::E) { pref = CorePref::P; m = count(pref); }   // one core class
	if (m == 0) { pref = CorePref::Any; m = count(pref); }
	if (m == 0) return false;

	// The k-th member of the class, then the next one that is not away.
	std::atomic<size_t>& cur = nextClassWorker[pref == CorePref::E ? 1 : 0];
	size_t k = cur.fetch_add(1, std::memory_order_relaxed) % m;
	size_t first = computeN, pick = computeN;
	for (size_t pass = 0; pass < 2 && pick == computeN; ++pass) {
		for (size_t q = 0; q < computeN; ++q) {
			if (!candidate(q, pref)) continue;
			if (pass == 0 && k) { --k; continue; }
			if (first == computeN) first = q;
			if (workers[q]->adoptSlot.load(std::memory_order_relaxed) != Thread::kAdoptAway) { pick = q; break; }
		}
	}
	if (pick == computeN) pick = first;   // every candidate away: the adopter takes it
	if (pick == computeN) return false;
	if (hiPri) return PushTo(pick, task);

	JLIB_STAT(Pushes);
	normalInboxes[pick]->push(task);
	workers[pick]->MarkQueuedWork();
	workers[pick]->NotifyWorker();
	return true;
}

bool TaskScheduler::PushTo(size_t worker, Task* task) {
	if (!task) return false;
	if (worker == kAnyWorker || worker >= workers.size()) return false;
	if (worker >= hiPriInboxes.size() || !hiPriInboxes[worker])   return false;
	if (worker >= workers.size()      || !workers[worker])        return false;
	JLIB_STAT(Pushes);

	hiPriInboxes[worker]->push(task);

	workers[worker]->MarkQueuedWork();
	workers[worker]->NotifyWorker();
	return true;
}


bool TaskScheduler::PushLaneIntake(Task** tasks, size_t n) noexcept {
	if (!tasks || n == 0) return false;
	TaskScheduler* s = instance;
	
	if (!s || !s->poolActive) return false;
	if (GetHotWorkers() == 0) return false;      

	if (!s->laneIntake.enqueue_bulk(tasks, n)) return false;   

	s->ioLastPushNs_.store(MonotonicNs(), std::memory_order_relaxed);

#if !defined(JLIB_LANEINTAKE_CTL_NO_NOTIFY)
	
	// Wake a PARKED reserved worker if there is one; an awake one would find the intake anyway,
	// and notifying only a busy one would leave the sleepers asleep behind its body.
	const size_t nAll = s->workers.size();
	size_t target = nAll;
	for (size_t w = ReservedBase(nAll); w < nAll; ++w) {
		if (!s->workers[w]) continue;
		if (target == nAll) target = w;
		if (s->workers[w]->workerState.load(std::memory_order_seq_cst) == Thread::WS_PARKED) { target = w; break; }
	}
	if (target < nAll) { s->workers[target]->MarkQueuedWork(); s->workers[target]->NotifyWorker(); }
#else
	
#endif
	return true;
}

static std::atomic<uint64_t> g_kSteals{ 0 };
static std::atomic<uint64_t> g_kStealsReturned{ 0 };
void TaskScheduler::NoteReservedSteal() noexcept {
	g_kSteals.fetch_add(1, std::memory_order_relaxed);
}
void TaskScheduler::NoteReservedStealReturned() noexcept {
	g_kStealsReturned.fetch_add(1, std::memory_order_relaxed);
}
void TaskScheduler::GetReservedStealStats(uint64_t& steals, uint64_t& returned) noexcept {
	steals   = g_kSteals.load(std::memory_order_relaxed);
	returned = g_kStealsReturned.load(std::memory_order_relaxed);
}

bool TaskScheduler::IoLaneQuiet() noexcept {
	TaskScheduler* s = instance;
	if (!s || !s->ReservedStealing()) return false;
	const long long last = s->ioLastPushNs_.load(std::memory_order_relaxed);

	if (last == 0) return true;
	const long long win = (long long)s->IoQuietWindowUs() * 1000ll;
	return (MonotonicNs() - last) > win;
}

Task* TaskScheduler::TakeLaneIntake() noexcept {
	TaskScheduler* s = instance;
	if (!s || !s->poolActive) return nullptr;
	Task* t = nullptr;
	return s->laneIntake.try_dequeue(t) ? t : nullptr;
}

bool TaskScheduler::LaneIntakeIdle() noexcept {
	TaskScheduler* s = instance;
	if (!s || !s->poolActive) return true;
	return s->laneIntake.size_approx() == 0;
}

bool TaskScheduler::PushIO(Task* task) noexcept {
	if (!task) return false;

	if (!IsLowLatency(task->lane)) return false;

	const size_t k = GetHotWorkers();
	if (k == 0) return false;                       

	const size_t start = ioSteer_.fetch_add(1, std::memory_order_relaxed) % k;
	
	const size_t nAll     = workers.size();
	const size_t laneBase = ReservedBase(nAll);
	const size_t w        = laneBase + start;

	return PushTo(w, task);
}

void TaskScheduler::WaitOnEventArmed(Event& event, const std::function<void()>& arm, Pin pin) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	CheckSuspendable(myTask, "TaskScheduler::WaitOnEventArmed");
	Fiber* myFiber = myTask->record->fiber;
	if (!myFiber) {
		throw std::runtime_error("WaitOnEventArmed called from a task with no assigned fiber -- "
			"only fiber tasks may suspend.");
	}

	if (IsTaskCancelled(myTask)) return;

	myFiber->BeginSuspend(pin);
	event.AddWaiter(myTask);

	if (arm) arm();

	CheckSuspendableCurrent("TaskScheduler::WaitOnEventArmed");
	JLIB_EPOCH_CHECK_NO_GUARD("TaskScheduler::WaitOnEventArmed");
	Thread::TsanSwitchToScheduler();
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}

void TaskScheduler::WaitOnEventArmed(const std::string& eventName, const std::function<void()>& arm, Pin pin) {
	WaitOnEventArmed(GetEvent(eventName), arm, pin);
}

void TaskScheduler::WaitOnEventDirectArmed(const std::function<void(DirectEvent*)>& arm, Pin pin) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	CheckSuspendable(myTask, "TaskScheduler::WaitOnEventDirectArmed");
	Fiber* myFiber = myTask->record->fiber;
	if (!myFiber) {
		throw std::runtime_error("WaitOnEventDirectArmed called from a task with no assigned "
			"fiber -- only fiber tasks may suspend.");
	}

	DirectEvent* e = eventPool.Acquire();   

	myFiber->BeginSuspend(pin);
	e->waiter.store(myTask, std::memory_order_release);

	if (arm) arm(e);

	CheckSuspendableCurrent("TaskScheduler::WaitOnEventDirectArmed");
	JLIB_EPOCH_CHECK_NO_GUARD("TaskScheduler::WaitOnEventDirectArmed");
	Thread::TsanSwitchToScheduler();
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);

	eventPool.Release(e);
}

bool TaskScheduler::IsOnFiber() {
	auto* t = Thread::GetCurrent();
	
	return t != nullptr && t->currentRunningTask != nullptr && t->currentFiber != nullptr;
}


void Event::WakeCoroutine(Task* t) {
	TaskScheduler::Instance().WakeTask(t);
}

void JLib::detail::WakeCoroutineWaiter(Task* t) {
	TaskScheduler::Instance().WakeTask(t);
}

Event& TaskScheduler::GetEvent(const std::string& name) {
	registryMtx.lock();
	if (eventRegistry.find(name) == eventRegistry.end())
		eventRegistry[name] = std::make_unique<Event>();
	Event& event = *eventRegistry[name];

#if defined(_DEBUG) || defined(JLIB_DEVELOPMENT)
	if (eventRegistry.size() == 4096)
		fprintf(stderr,
			"[JLib::Scheduler] WARNING: event registry has reached 4096 named events. Named events "
			"are for a BOUNDED set of rendezvous points; a name minted per operation (e.g. "
			"\"fence_\" + counter) grows this map without bound and will convoy on registryMtx. "
			"Use WaitOnEventDirectArmed for per-operation waits. Last inserted: \"%s\"\n",
			name.c_str());
#endif

	registryMtx.unlock();
	return event;
}
void TaskScheduler::Pause() {
	paused.store(true, std::memory_order_seq_cst);
}
void TaskScheduler::Resume() {
	paused.store(false, std::memory_order_seq_cst);
	NotifyAll();
}
void TaskScheduler::Stop(Task* worker_task) {
	
	(void)worker_task;
	stopFlag.store(true, std::memory_order_release);
}

Task* TaskScheduler::GetTask() {
	int& consecutiveHiPriSteals = ConsecutiveHiPriSteals();
	bool forceLoPri = (consecutiveHiPriSteals >= kStealFairnessWindow);

	Thread* thief = Thread::GetCurrent();
	
	const unsigned thiefCpu = JLib::platform::CurrentCpu();
	
	auto fiberlessRunnable = [&](StealBits sb) {
		return sb.type != TaskType::Fiber && sb.type != TaskType::Main;
	};

	size_t numThreads = deques.size();
	
	size_t start = rand() % numThreads;

	(void)forceLoPri;
	consecutiveHiPriSteals = 0;
	for (size_t i = 0; i < numThreads; ++i) {
		size_t target = (start + i) % numThreads;
		if (auto s = deques[target]->steal_if(fiberlessRunnable))
			return *s;
	}

	return nullptr;
}

bool TaskScheduler::TryRunStolenNativeTask() {
	
	// Spin-help never touches a hi-pri inbox: that inbox is run directly by its owner's loop only.
	Thread* laneOwner = Thread::GetCurrent();
	Task* task = nullptr;
	{
		const bool laneWorker = laneOwner && laneOwner->qIndex >= 0
		                     && IsReservedIndex((size_t)laneOwner->qIndex, workers.size());
		if (!laneWorker) {
			task = GetTask();
		}
#ifndef NDEBUG
		else {
			
			static std::atomic<bool> warned{ false };
			bool expected = false;
			if (warned.compare_exchange_strong(expected, true, std::memory_order_relaxed))
				fprintf(stderr,
				        "[JLib::Scheduler] LANE CONTRACT VIOLATED: reserved worker q%d is BLOCKED "
				        "inside a task.\n  Lane work must be short and non-blocking; a lane task "
				        "must not call WaitFor, SchedulerMutex\n  or a condition variable. The "
				        "lane will stall until the rest of the pool clears what it waits on.\n"
				        "  Break on this line to find the blocking call.\n",
				        laneOwner->qIndex);
		}
#endif
	}
	if (!task) {
		
		Thread* self = Thread::GetCurrent();
		if (!self || !self->IsPoolWorker()) return false;   // main's helper has no inboxes
		if (!self->DrainOwnInboxesToDeques()) return false;

		NotifyAll();

		task = GetTask();
		if (!task) return false;
	}

	if (DiscardIfCancelled(task)) return true;

	// GetTask only hands out coroutines. The frame owns its task: a suspended one is already queued
	// elsewhere and a finished one freed itself, so the task is not touched after the call.
	assert(task->type == TaskType::Coroutine && "spin-help runs coroutines only");
	task->started = 1;
	task->Execute();
	return true;
}

TaskAllocator* TaskScheduler::GetAllocator() {
	return &taskAllocator;
}

size_t TaskScheduler::GetWorkerCount() const {
	return workers.size();
}

void TaskScheduler::PrefaultTaskSlots(size_t slots) {
	taskAllocator.Prefault(slots);
}

bool TaskScheduler::LazyTaskSlabEnabled() { return CurrentConfig().lazyTaskSlab; }
bool TaskScheduler::SlabGrowthEnabled() noexcept {
	return detail::SlabGrowthEnabled().load(std::memory_order_relaxed);
}
TaskScheduler::SlabSizes TaskScheduler::CurrentSlabSizes() { return CurrentConfig().slab; }

size_t TaskScheduler::NormalFibersPerComputeWorker() { return CurrentConfig().fibers.normalPerComputeWorker; }
size_t TaskScheduler::TinyFibersPerKWorker()         { return CurrentConfig().fibers.tinyPerKWorker; }
size_t TaskScheduler::DeepFibersPerComputeWorker()   { return CurrentConfig().fibers.deepPerComputeWorker; }
size_t TaskScheduler::FiberMemoryLimit()             { return CurrentConfig().fiberMemoryLimit; }

#ifdef JLIBSCHED_RETRY_STATS
namespace JLib {
	size_t RetrySlotForCurrentThread() noexcept {
		
		if (Thread* w = Thread::Current()) {
			const int q = w->qIndex;
			if (q >= 0 && (size_t)q < kRetrySpillSlot) return (size_t)q;
		}
		return kRetrySpillSlot;
	}

	void RetryStatsReset() noexcept {
		for (size_t s = 0; s < (size_t)RetrySite::Count; ++s)
			for (size_t i = 0; i < kRetrySlots; ++i) {
				RetryCell& c = g_retry[s][i];
				c.calls.store(0, std::memory_order_relaxed);
				c.retries.store(0, std::memory_order_relaxed);
				c.maxRetries.store(0, std::memory_order_relaxed);
				c.ge8.store(0, std::memory_order_relaxed);
				c.ge64.store(0, std::memory_order_relaxed);
				c.ge512.store(0, std::memory_order_relaxed);
				c.ge4096.store(0, std::memory_order_relaxed);
			}
	}

	void RetryStatsReport() {
		printf(
			"  CAS retries per call (max and buckets, NOT retries/calls -- an average cannot see\n"
			"  one call that spun ten thousand times next to ten million that did not)\n");
		
		printf("  %-12s %14s %14s %8s %10s %8s %7s %8s\n",
			"site", "calls", "retries", "MAX", ">=8", ">=64", ">=512", ">=4096");
		for (size_t s = 0; s < (size_t)RetrySite::Count; ++s) {
			unsigned long long calls = 0, retries = 0, b8 = 0, b64 = 0, b512 = 0, b4096 = 0;
			unsigned mx = 0;
			for (size_t i = 0; i < kRetrySlots; ++i) {
				const RetryCell& c = g_retry[s][i];
				calls   += c.calls.load(std::memory_order_relaxed);
				retries += c.retries.load(std::memory_order_relaxed);
				b8      += c.ge8.load(std::memory_order_relaxed);
				b64     += c.ge64.load(std::memory_order_relaxed);
				b512    += c.ge512.load(std::memory_order_relaxed);
				b4096   += c.ge4096.load(std::memory_order_relaxed);
				
				const unsigned m = c.maxRetries.load(std::memory_order_relaxed);
				if (m > mx) mx = m;
			}
			
			if (calls == 0) {
				printf("  %-12s %14s\n", kRetrySiteNames[s], "(never called)");
				continue;
			}
			printf("  %-12s %14llu %14llu %8u %10llu %8llu %7llu %8llu\n",
				kRetrySiteNames[s], calls, retries, mx, b8, b64, b512, b4096);
		}
		printf(
			"  ^ MAX far above the buckets = one outlier, not a pattern -- look for a single\n"
			"    unlucky interleaving. A populated >=512 column is a real storm and is what an\n"
			"    exponential backoff in the failure path would be for. All-zero buckets with a\n"
			"    small MAX means the loop is not the problem, whatever the timing tail says.\n");
	}
}
#endif



Mode TaskScheduler::GetMode() noexcept {
	static_assert(kMaxHintQueues < Pin::kCurrent, "worker indices must fit below the Pin sentinels");
	return instance ? instance->cfg_.mode : Mode::Migrate;
}

// The one place a pin is resolved.
void JLib::StampPin(Task* t, Pin pin) noexcept {
	if (!t || !t->record) return;
	JLIB_STAT(Suspends);
	JLIB_STAT_ONLY(t->record->statSuspendAt = JLIB_STAT_TICKS();)
	uint16_t target = TaskScheduler::PinForced() ? Pin::kCurrent : pin.target;
	if (target == Pin::kCurrent) {
		Thread* self = Thread::GetCurrent();
		// Off the pool (main's helper, user threads) nothing can be pinned: no pin.
		target = (self && self->IsPoolWorker()) ? (uint16_t)self->qIndex : TaskRecord::kNoPin;
	} else if (target != Pin::kNone) {
		const bool valid = TaskScheduler::IsInitialized()
		                && target < TaskScheduler::Instance().GetWorkerCount();
		assert(valid && "Pin::Thread(n): no such worker");
		if (!valid) target = TaskRecord::kNoPin;
	}
	t->record->pinTo = target;
}

bool TaskScheduler::PushResume(size_t worker, Task* task) {
	TaskScheduler* s = instance;
	if (!task || !s || !s->poolActive) return false;
	return s->PushTo(worker, task);
}
bool TaskScheduler::NotifyHolder(size_t worker) {
	TaskScheduler* s = instance;
	
	if (!s || !s->poolActive) return false;
	if (worker >= s->workers.size() || !s->workers[worker]) return false;
	s->workers[worker]->MarkQueuedWork();
	s->workers[worker]->NotifyWorker();
	return true;
}


namespace JLib { namespace detail {
	void SetCurrentFiber(Fiber* f) noexcept {
		if (Thread* t = Thread::GetCurrent()) t->currentFiber = f;
	}
} }

TaskRecord* TaskScheduler::CurrentRecord() noexcept {
	Thread* t = Thread::GetCurrent();
	Task* task = t ? t->currentRunningTask : nullptr;
	return task ? task->record : nullptr;
}

void** TaskScheduler::CurrentLocals() noexcept {
	TaskRecord* r = CurrentRecord();
	if (!r) return nullptr;
	if (!r->locals) {
		// One slot per TaskRecord::kLocalSlots; 64 bytes, same slab class as the record.
		void* mem = Instance().taskAllocator.AllocSized(sizeof(void*) * TaskRecord::kLocalSlots);
		if (!mem) mem = ::operator new(sizeof(void*) * TaskRecord::kLocalSlots);
		r->locals = static_cast<void**>(mem);
		for (size_t i = 0; i < TaskRecord::kLocalSlots; ++i) r->locals[i] = nullptr;
	}
	return r->locals;
}

void TaskScheduler::ReleaseRecord(TaskRecord* r) noexcept {
	if (!r) return;
	if (r->locals) {
		detail::ReleaseFiberSlots(r->locals, TaskRecord::kLocalSlots);
		taskAllocator.Free(r->locals);
		r->locals = nullptr;
	}
	if (r->debts) {                       // only "any holder" debts can be left by now
		detail::HandOffFiberDebts(r->debts);
		r->debts = nullptr;
	}
	r->~TaskRecord();
	taskAllocator.Free(r);
}

bool TaskScheduler::HasFiberLocal() noexcept { return CurrentRecord() != nullptr; }

uint16_t TaskScheduler::AllocFiberLocalSlot() noexcept { return FiberRegistry::FlsAlloc(); }
FiberRegistry& TaskScheduler::Fibers() noexcept        { return FiberRegistry::Instance(); }

bool TaskScheduler::ReleaseOnFiberDeath(FiberDebt& node, void* obj,
                                        void (*release)(void*) noexcept) noexcept {
	if (!obj || !release) return false;
	TaskRecord* r = CurrentRecord();
	if (!r) return false;

	node.obj     = obj;
	node.release = release;
	node.holder  = FiberDebt::kAnyHolder;

	node.next = r->debts;
	r->debts  = &node;
	return true;
}

bool TaskScheduler::ReleaseOnWorker(FiberDebt& node, void* obj,
                                    void (*release)(void*) noexcept,
                                    size_t holder, uint32_t kind) noexcept {
	if (!obj || !release || kind == Fiber::kOwesNothing) return false;
	TaskRecord* r = CurrentRecord();
	if (!r) return false;

	node.obj     = obj;
	node.release = release;
	node.holder  = holder;   // the cleanup chain visits this holder at death

	node.next = r->debts;
	r->debts  = &node;

	r->owedKinds = (uint8_t)(r->owedKinds | kind);
	return true;
}

size_t TaskScheduler::DischargeFiberDebts(TaskRecord* r, size_t holder) noexcept {
	return FiberRegistry::DischargeDebts(r, holder);
}

void*& TaskScheduler::FiberLocal(size_t slot) noexcept {
	static thread_local void* t_offTask = nullptr;
	void** block = (slot < TaskRecord::kLocalSlots) ? CurrentLocals() : nullptr;
	if (!block) {
		t_offTask = nullptr;
		return t_offTask;
	}
	return block[slot];
}



Task* TaskScheduler::CreateTaskImpl(void(*fn)(void*), void* data, Lane lane, TaskType type,
                                    StackClass stack) {
	void* mem = taskAllocator.AllocSized(sizeof(Task));   
	if (!mem) return nullptr;
	detail::RecordTaskSize(sizeof(Task));   
	Task* t = ::new (mem) Task(fn, data, lane);
	t->record = NewTaskRecord();
	t->type = type;
	
	t->stackClass = stack;
	
	t->trivialDtor = 1;
	return t;
}

TaskRecord* TaskScheduler::NewTaskRecord() {
	static_assert(sizeof(TaskRecord) <= TaskAllocator::SMALL_SLOT, "TaskRecord must fit one cache line");
	void* mem = taskAllocator.AllocSized(sizeof(TaskRecord));
	if (!mem) mem = ::operator new(sizeof(TaskRecord));
	return ::new (mem) TaskRecord();
}

void TaskScheduler::FreeTask(Task* t) {
	if (!t) return;
	TaskRecord* r = t->record;
	CleanupTaskMetadata(t);
	DestroyTask(t);
	taskAllocator.Free(t);
	ReleaseRecord(r);
}

void TaskScheduler::MaybeBuddyWake(size_t k) noexcept {
	if (k >= siblingQIndex.size()) return;
	const int s = siblingQIndex[k];
	if (s < 0 || (size_t)s == k) return;
	if ((size_t)s >= workers.size() || !workers[(size_t)s]) return;
	if (k >= deques.size() || deques[k]->size_approx() <= kFatDeque) return;
	if (!workers[(size_t)s]->idleLinked.load(std::memory_order_relaxed)) return;
	workers[(size_t)s]->NotifyWorker();
}

bool TaskScheduler::PushTarget(Task* task, size_t worker) {
	if (!task) return false;

	size_t num_workers = workers.size();
	if (worker != kAnyWorker && worker < num_workers) {
		size_t idx = worker;
		
		const Lane useHi = (IsLowLatency(task->lane) && HiPriLaneActive()) ? Lane::LowLatency : Lane::Normal;
		(IsLowLatency(useHi) ? hiPriInboxes : normalInboxes)[idx]->push(task);
		
		workers[idx]->MarkQueuedWork();
		workers[idx]->NotifyWorker();
	}
	else {
		const Lane useHi = (IsLowLatency(task->lane) && HiPriLaneActive()) ? Lane::LowLatency : Lane::Normal;

		// Latency work goes to the shared lane intake, never one worker's inbox (the hi-pri inbox is
		// PushTo's). Refused by the intake -- off, or no K -- it is placed like normal work.
		if (IsLowLatency(useHi) && LaneIntakeEnabled() && PushLaneIntake(&task, 1))
			return true;

		const uint8_t chosen = (uint8_t)PickNextWorker(Lane::Normal);

		// Main in the pool whose own round-robin lands on slot 0: straight onto its own deque,
		// stealable, no inbox hop. Everyone else still goes through the chosen inbox.
		if (chosen == 0 && GetMainMode() == MainMode::InPool) {
			Thread* self = Thread::GetCurrent();
			if (self && self->isMain) {
				if (!deques[0]->push_bottom(task)) TaskDeque::FatalPushRefused();
				return true;
			}
		}

		// Diagnostic only: the dump's "LAST PUSH TARGET" marker. One global word written by every
		// pusher on every push, so it is a shared line for all of them -- too small to measure in
		// these benches, but there is no reason to pay it in a release build to mark a debug line.
		JLIB_STAT_ONLY(g_lastPushTarget.store((int)chosen, std::memory_order_relaxed);)

		normalInboxes[chosen]->push(task);
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();

		MaybeBuddyWake((size_t)chosen);

	}
	return true;
}

void TaskScheduler::WakeAdopterOf(int q) noexcept {
	for (Thread* sp : spares)
		if (sp->adoptSlot.load(std::memory_order_seq_cst) == q) { sp->Wake(); return; }
	for (Thread* w : workers)
		if (w && w->adoptSlot.load(std::memory_order_seq_cst) == q) { w->Wake(); return; }
}

void TaskScheduler::BlockBegin() {
	TaskScheduler* s = instance;
	Thread* self = Thread::GetCurrent();
	if (!s || !self || !self->IsPoolWorker()) return;   // off the pool: nothing of the pool to strand
	if (self->blockDepth++ != 0) return;

	if (std::atomic<size_t>* slot = CurrentEpochSlot(); slot && slot->load(std::memory_order_acquire) != SIZE_MAX) {
		std::fprintf(stderr,
			"[JLib::Scheduler] FATAL: BlockBegin/BlockInPlace inside an EpochGuard. The guard pins\n"
			"  reclamation for the whole pool for as long as the call blocks. End the guard first.\n");
		std::fflush(stderr);
		std::abort();
	}
	const size_t n = s->workers.size();
	const size_t me = (size_t)self->qIndex;
	JLIB_STAT(Blocks);
	{
		static std::atomic<bool> warned{ false };
		if ((PinForced() || !s->hiPriInboxes[me]->quiescent())
		    && !warned.exchange(true, std::memory_order_relaxed)) {
			std::fprintf(stderr,
				"[JLib::Scheduler] NOTE: a thread is blocking with pinned work queued for it%s. Pinned\n"
				"  and main-only work waits for that thread to return; only unpinned work is adopted.\n"
				"  This note prints once.\n", PinForced() ? " (Mode::Pinned pins every suspension)" : "");
			std::fflush(stderr);
		}
	}
	if (IsReservedIndex(me, n)) return;   // K never drains a normal inbox: there is nothing to adopt

	// What is already queued goes onto the deque, where it can be stolen right away.
	self->DrainOwnInboxesToDeques();

	// A worker that is itself adopting keeps that adoption (its owner's handback still finds it)
	// and takes the fallback: drain the adopted inbox once too, and later pushes wait.
	if (self->adoptSlot.load(std::memory_order_seq_cst) >= 0) {
		while (self->DrainAdoptedInbox()) {}
		JLIB_STAT(BlocksNoAdopter);
		return;
	}

	// Claim an adopter with a CAS, never a plain exchange (that would steal another blocker's).
	// A parked spare first -- it keeps the pool at full width -- then a free compute worker. Main
	// (in the pool) and K are never adopters: neither runs the loop that drains an adopted inbox.
	auto claim = [&](Thread* t) {
		int expected = Thread::kAdoptNone;
		if (!t->adoptSlot.compare_exchange_strong(expected, (int)me,
				std::memory_order_seq_cst, std::memory_order_seq_cst)) return false;
		self->adopter = t;
		return true;
	};
	for (Thread* sp : s->spares)
		if (claim(sp)) break;
	if (!self->adopter) {
		const size_t computeN = n - GetHotWorkers();
		const bool mainInPool = GetMainMode() == MainMode::InPool;
		for (size_t i = 1; i < computeN; ++i) {
			const size_t w = (me + i) % computeN;
			if (mainInPool && w == 0) continue;
			if (claim(s->workers[w])) break;
		}
	}
	// Away, adopted or not: a wake for this thread now goes to its adopter.
	self->adoptSlot.store(Thread::kAdoptAway, std::memory_order_seq_cst);
	std::atomic_thread_fence(std::memory_order_seq_cst);
	if (self->adopter) self->adopter->Wake();
	else JLIB_STAT(BlocksNoAdopter);   // everyone else is blocked or adopting: pushes wait
}

void TaskScheduler::BlockEnd() {
	TaskScheduler* s = instance;
	Thread* self = Thread::GetCurrent();
	if (!s || !self || !self->IsPoolWorker()) return;
	assert(self->blockDepth > 0 && "BlockEnd without BlockBegin");
	if (self->blockDepth == 0 || --self->blockDepth != 0) return;
	if (IsReservedIndex((size_t)self->qIndex, s->workers.size())) return;
	if (self->adoptSlot.load(std::memory_order_relaxed) != Thread::kAdoptAway) return;   // was adopting: kept it

	const int me = self->qIndex;
	Thread* adopter = self->adopter;
	if (adopter) {
		int expected = me;
		adopter->adoptSlot.compare_exchange_strong(expected, Thread::kAdoptNone,
			std::memory_order_seq_cst, std::memory_order_seq_cst);
	}
	self->adoptSlot.store(Thread::kAdoptNone, std::memory_order_seq_cst);
	if (adopter) {
		// The adopter may be mid-pop from this inbox: wait that out (a pop, never a task).
		while (adopter->draining.load(std::memory_order_seq_cst) == me)
			platform::CpuRelax();
		self->adopter = nullptr;
	}
}

[[noreturn]] void JLib::FatalNoEpochSlot() {
	std::fprintf(stderr,
		"[JLib::Scheduler] FATAL: an EpochGuard on a thread with no epoch slot.\n"
		"  Main and the pool's threads have one. Any other thread must hold a JLib::ThreadScope\n"
		"  while it uses epochs (EpochGuard, RetirePtr, the lock-free structures):\n"
		"\n"
		"      std::thread([] { JLib::ThreadScope scope; /* ... */ });\n"
		"\n"
		"  Without one it used to share main's slot, and could clear main's announcement while\n"
		"  main was mid-traversal.\n");
	std::fflush(stderr);
	std::abort();
}

JLib::ThreadScope::ThreadScope() {
	assert(TaskScheduler::IsInitialized() && "ThreadScope needs a running pool (Init first)");
	if (CurrentThreadId() != kNoThreadSlot) return;   // main, a pool thread, or already scoped
	slot_ = EpochManager::Instance().ClaimExternalSlot();
	if (slot_ == kNoThreadSlot) {
		std::fprintf(stderr,
			"[JLib::Scheduler] FATAL: no free external thread slot (%zu in use). Raise\n"
			"  Config::externalThreads at Init, or end ThreadScopes that are no longer needed.\n",
			EpochManager::Instance().ExternalSlotCount());
		std::fflush(stderr);
		std::abort();
	}
	owner_ = true;
	thread_id = slot_;
	detail::RunsGates() = true;
	JLIB_STAT_ONLY(detail::StatLabelThread(-1, "external (ThreadScope)");)
}

JLib::ThreadScope::~ThreadScope() {
	if (!owner_) return;
	EpochManager& em = EpochManager::Instance();
	if (std::atomic<size_t>* s = em.ThreadSlot(slot_); s && s->load(std::memory_order_acquire) != SIZE_MAX) {
		std::fprintf(stderr,
			"[JLib::Scheduler] FATAL: ThreadScope ended while an EpochGuard is still open on this\n"
			"  thread. End the guard first: releasing the slot under it would unpin a live traversal.\n");
		std::fflush(stderr);
		std::abort();
	}
	// This thread will not gate again: free what is already safe, hand the rest to the orphan
	// store (the pool sweeps it), and give back the hazard row.
	em.TryReclaim(false);
	detail::OrphanEpochEntries(detail::EpochBag().items);
	HazardDomain& hd = HazardDomain::Instance();
	hd.Scan(false);
	hd.HandOffPending();
	hd.ReleaseCurrentReader();
	detail::RunsGates() = false;
	thread_id = kNoThreadSlot;
	em.ReleaseExternalSlot(slot_);
	slot_ = kNoThreadSlot;
}

// The calling thread, if it is a full-time compute worker of this pool; else null.
static Thread* ComputeSelf(TaskScheduler& s, const std::vector<Thread*>& workers) {
	Thread* self = Thread::GetCurrent();
	if (!self || self->isMain) return nullptr;
	const size_t q = (size_t)self->qIndex;
	if (q >= workers.size() || workers[q] != self) return nullptr;
	if (TaskScheduler::IsReservedIndex(q, workers.size())) return nullptr;
	(void)s;
	return self;
}

void TaskScheduler::ResumeFiber(Task* task) {
	if (!task) return;
	if (task->type == TaskType::Main) { PushMainQueue(task); return; }
	const uint16_t pin = task->record->pinTo;
	if (pin != TaskRecord::kNoPin) { JLIB_STAT(ResumePinned); PushTo(pin, task); return; }   // hi-pri inbox, never stolen
	if (IsNormalLane(task->lane)) {
		if (Thread* self = ComputeSelf(*this, workers)) {
			JLIB_STAT(ResumeLocal);
			if (!deques[(size_t)self->qIndex]->push_bottom(task)) TaskDeque::FatalPushRefused();
			return;
		}
	}
	JLIB_STAT(ResumePlaced);
	Requeue(task);   // unpinned, off a compute worker: placement
}

bool TaskScheduler::YieldFiber(Task* task) {
	if (!task) return false;
	Thread* self = Thread::GetCurrent();
	if (!self || !self->IsPoolWorker()) { ResumeFiber(task); return false; }   // a yield off the pool: place it
	const size_t q = (size_t)self->qIndex;
	if (task->type == TaskType::Main) { PushMainQueue(task); return self->isMain; }
	const uint16_t pin = task->record->pinTo;
	if (pin == q) {   // pinned here: own hi-pri inbox, no wake needed
		JLIB_STAT(ResumePinned);
		hiPriInboxes[q]->push(task);
		self->MarkQueuedWork();
		return true;
	}
	if (pin != TaskRecord::kNoPin) { JLIB_STAT(ResumePinned); PushTo(pin, task); return false; }
	if (IsNormalLane(task->lane) && ComputeSelf(*this, workers) == self) {
		JLIB_STAT(ResumeLocal);
		if (!deques[q]->push_bottom(task)) TaskDeque::FatalPushRefused();
		return true;
	}
	JLIB_STAT(ResumePlaced);
	Requeue(task);
	return false;
}

bool TaskScheduler::WakeTask(Task* task) {
	if (!task) return false;
	if (task->started) { ResumeFiber(task); return true; }
	return Push(task);
}

TaskScheduler::RequeueResult TaskScheduler::Requeue(Task* task) {
	if (!task) return RequeueResult::Failed;
	if (task->type == TaskType::Main) { PushMainQueue(task); return RequeueResult::Pinned; }

	const uint16_t pin = task->record->pinTo;
	if (pin != TaskRecord::kNoPin && PushTo(pin, task)) {
		return RequeueResult::Pinned;
	}

	if (task->started) {
		if (PushTarget(task)) return RequeueResult::Stealable;
	}

	const Lane useHi = (IsLowLatency(task->lane) && HiPriLaneActive()) ? Lane::LowLatency : Lane::Normal;

	// Same placement as PushTarget: latency work to the shared intake, never a worker's inbox.
	if (IsLowLatency(useHi) && LaneIntakeEnabled() && PushLaneIntake(&task, 1))
		return RequeueResult::Stealable;

	const uint8_t chosen = (uint8_t)PickNextWorker(Lane::Normal);
	normalInboxes[chosen]->push(task);
	workers[chosen]->MarkQueuedWork();
	workers[chosen]->NotifyWorker();

	return RequeueResult::Pinned;
}

int TaskScheduler::PickNextWorker(Lane lane) {
	
	const size_t n = workers.size();
	if (n == 0) return 0;

	const size_t hotN     = GetHotWorkers();
	const size_t laneBase = (hotN < n) ? (n - hotN) : 0;

	if (IsLowLatency(lane) && hotN)
		return (int)(laneBase + (nextHotWorker.fetch_add(1, std::memory_order_relaxed) % hotN));

	const size_t computeN = (hotN < n) ? (n - hotN) : n;
	if (computeN == 0) return 0;

	const size_t start = (size_t)nextWorker.fetch_add(1, std::memory_order_relaxed);

	// Only compute workers are ever on the idle stack (see the park path), so a popped worker is
	// always a valid target. Never re-register someone else: two pushes of one node make a cycle.
	if (SearchingCount() == 0) {
		const size_t idleQ = PopIdleWorker();
		if (idleQ != kNoIdleWorker && idleQ < computeN) return (int)idleQ;
	}

	// A thread that is away (blocked; its inbox adopted) is skipped as a hint: the adopter would
	// take the push anyway, but a live target is one fewer hop. Adoption is what makes it safe.
	for (size_t i = 0; i < computeN; ++i) {
		const size_t q = (start + i) % computeN;
		Thread* w = workers[q];
		if (w && !w->idleLinked.load(std::memory_order_relaxed)
		      && w->adoptSlot.load(std::memory_order_relaxed) != Thread::kAdoptAway)
			return (int)q;
	}

	return (int)(start % computeN);
}

uint64_t TaskScheduler::GetCurrentTimeMs() const {
	using namespace std::chrono;
	return duration_cast<milliseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

TaskType TaskScheduler::CurrentTaskType() noexcept {
	
	if (Thread* w = Thread::GetCurrent())
		if (Task* t = w->currentRunningTask)
			return t->type;
	return TaskType::Fiber;   // not inside a task
}

Task* TaskScheduler::GetCurrentTask() const {
	
	Thread* currentThread = Thread::GetCurrent();
	if (currentThread && currentThread->currentRunningTask) {
		return currentThread->currentRunningTask;
	}
	return nullptr; 
}

void TaskScheduler::CleanupTaskMetadata(Task* task) {
	if (!task) return;
	
	(void)task;
}

bool JLib::CurrentTaskCancelled() {
	if (!TaskScheduler::IsInitialized()) return false;
	Task* t = TaskScheduler::Instance().GetCurrentTask();
	
	if (!t) return false;
	return IsTaskCancelled(t);
}

void SchedulerMutex::Lock(Pin pin) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		
		Task* callerTask = (TaskScheduler::IsInitialized()) ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
		WaitNode node;   // on this fiber's stack: it is reachable exactly while this frame waits
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			if (!locked) {
				locked = true;
				spinLock.clear(std::memory_order_release);
				{
					while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
					lockHolder = callerTask;
					holderLock.clear(std::memory_order_release);
				}
				return;
			}

			current->BeginSuspend(pin);
			node.fiber = current;
			waiters.PushBack(&node);
			spinLock.clear(std::memory_order_release);
		}
		
		CheckSuspendableCurrent("SchedulerMutex::Lock");
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerMutex::Lock");
		Thread::TsanSwitchToScheduler();
		ContextSwitch(&current->ctx, current->homeCtx);
		
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
	}
	else {
		
		if (Try_Lock()) return;

		if (thread != nullptr && thread->currentRunningTask != nullptr) {
			if (auto hook = s_blockViolationHook.load(std::memory_order_relaxed)) { hook(); return; }
			fprintf(stderr,
				"[JLib::Scheduler] INVARIANT VIOLATED: a task with no fiber blocked on a contended\n"
				"  SchedulerMutex. Only a fiber can wait here -- it suspends and frees its worker.\n"
				"  A task without a fiber cannot suspend, so waiting would pin the worker while its\n"
				"  inbox may hold tasks NOBODY ELSE MAY RUN (inbox work is unstealable). The pool\n"
				"  deadlocks and reports it as a lost wake.\n"
				"  Fix: take contended locks from fiber tasks. From anything else, use Try_Lock\n"
				"  and handle failure without waiting.\n");
			fflush(stderr);
			std::abort();
		}

		bareWaiters.fetch_add(1, std::memory_order_seq_cst);
		{
			std::unique_lock<std::mutex> lk(bareMtx);
			bareCv.wait(lk, [this] { return Try_Lock(); });
		}
		bareWaiters.fetch_sub(1, std::memory_order_release);
		
	}
}

WaitPrimitive::WaitPrimitive() {
	if (!TaskScheduler::IsInitialized()) return;
	TaskScheduler& s = TaskScheduler::Instance();
	std::lock_guard<std::mutex> lk(s.primitivesMtx);
	nextPrimitive_ = s.primitivesHead;
	if (s.primitivesHead) s.primitivesHead->prevPrimitive_ = this;
	s.primitivesHead = this;
}

WaitPrimitive::~WaitPrimitive() { LeaveRegistry(); }

void WaitPrimitive::LeaveRegistry() noexcept {
	
	if (!TaskScheduler::IsInitialized()) return;
	TaskScheduler& s = TaskScheduler::Instance();
	std::lock_guard<std::mutex> lk(s.primitivesMtx);
	if (!nextPrimitive_ && !prevPrimitive_ && s.primitivesHead != this) return;   
	if (prevPrimitive_)                prevPrimitive_->nextPrimitive_ = nextPrimitive_;
	else if (s.primitivesHead == this) s.primitivesHead = nextPrimitive_;
	if (nextPrimitive_) nextPrimitive_->prevPrimitive_ = prevPrimitive_;
	nextPrimitive_ = prevPrimitive_ = nullptr;
}

void SchedulerMutex::CancelWaiters(CancelToken tok) {
	// One pass, no victim buffer: the matching nodes come off as a detached chain and the rest stay
	// queued in order.
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	WaitNode* victims = waiters.TakeIf([tok](const WaitNode* w) {
		return w->result && (!tok.Valid() || CancelToken(w->token).IsWithin(tok));
	});
	spinLock.clear(std::memory_order_release);

	while (victims) {
		// Read the node out BEFORE the wake: it lives on the waiter's stack, and the wake lets that
		// frame run and return, taking the node with it.
		WaitNode* const nextVictim = victims->next;
		Fiber* const f  = victims->fiber;
		Task*  const co = victims->coro;
		*victims->result = WaitResult::Cancelled;

		if (f) {
			Thread::Resume(f);
		} else if (co && TaskScheduler::IsInitialized()) {
			// WakeTask, not Push: a started coroutine resumes through the resume path, which
			// is where the pin written at its suspension is read.
			TaskScheduler::Instance().WakeTask(co);
		}
		victims = nextVictim;
	}
}

void SchedulerMutex::Unlock()
{
	
	if (OnBareThread() && t_heldMutexes > 0) --t_heldMutexes;

	Task* wasHolder;
	WaitNode* next = nullptr;
	{
		while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
		wasHolder = lockHolder;
		lockHolder = nullptr;
		holderLock.clear(std::memory_order_release);
	}

	for (;;) {
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			next = waiters.PopFront();   // FIFO: a mutex must not starve its oldest waiter
			if (!next) locked = false;
			spinLock.clear(std::memory_order_release);
		}
		if (!next) break;

		// A cancelled waiter is woken so it can return Cancelled, but it does not take the lock:
		// keep looking for a live one. Everything the node holds is read BEFORE the wake -- the
		// node lives on the waiter's stack, and waking it lets that frame run and leave.
		if (next->result && IsTaskCancelled(next->fiber ? next->fiber->owningTask : next->coro)) {
			Fiber* f = next->fiber;
			Task*  co = next->coro;
			*next->result = WaitResult::Cancelled;
			if (f) Thread::Resume(f);
			else if (co && TaskScheduler::IsInitialized())
				TaskScheduler::Instance().WakeTask(co);
			continue;
		}

		if (next->result) *next->result = WaitResult::Ok;
		break;
	}

	if (next && next->fiber) {
		Fiber* f = next->fiber;
		Thread::Resume(f);
	}
	else if (next && next->coro) {
		Task* co = next->coro;
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = co;
			holderLock.clear(std::memory_order_release);
		}
		if (TaskScheduler::IsInitialized()) TaskScheduler::Instance().WakeTask(co);
	}

	else if (bareWaiters.load(std::memory_order_seq_cst) != 0) {
		{ std::lock_guard<std::mutex> lk(bareMtx); }
		bareCv.notify_one();
	}
}

std::atomic<void(*)()> SchedulerMutex::s_blockViolationHook{ nullptr };

WaitResult SchedulerMutex::LockCancellable(Pin pin) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
	const uint32_t tok = callerTask ? callerTask->cancelToken : CancelToken::kNone;

	if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;

	if (current != nullptr) {
		WaitResult result = WaitResult::Ok;
		WaitNode node;   // on this fiber's stack
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			if (!locked) {
				locked = true;
				spinLock.clear(std::memory_order_release);
				{
					while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
					lockHolder = callerTask;
					holderLock.clear(std::memory_order_release);
				}
				return WaitResult::Ok;
			}
			
			current->BeginSuspend(pin);
			node.fiber = current; node.result = &result; node.token = tok;
			waiters.PushBack(&node);
			spinLock.clear(std::memory_order_release);
		}
		CheckSuspendableCurrent("SchedulerMutex::LockCancellable");
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerMutex::LockCancellable");
		Thread::TsanSwitchToScheduler();
		ContextSwitch(&current->ctx, current->homeCtx);

		if (result == WaitResult::Cancelled) return WaitResult::Cancelled;
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
		return WaitResult::Ok;
	}

	while (!Try_Lock()) {
		if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;
		ContendedSpinStep();
	}
	return WaitResult::Ok;
}

bool SchedulerMutex::LockAsyncEnqueue(Task* coroTask, WaitNode* node, WaitResult* result) {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (!locked) {
		locked = true;
		spinLock.clear(std::memory_order_release);
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = coroTask;
			holderLock.clear(std::memory_order_release);
		}
		if (result) *result = WaitResult::Ok;
		return true;
	}
	node->coro   = coroTask;
	node->result = result;
	node->token  = coroTask ? coroTask->cancelToken : CancelToken::kNone;
	waiters.PushBack(node);
	spinLock.clear(std::memory_order_release);
	return false;
}

bool SchedulerMutex::LockAsyncEnqueue(Task* coroTask, WaitNode* node) {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (!locked) {
		locked = true;
		spinLock.clear(std::memory_order_release);
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = coroTask;
			holderLock.clear(std::memory_order_release);
		}
		return true;
	}

	node->coro = coroTask;
	waiters.PushBack(node);
	spinLock.clear(std::memory_order_release);
	return false;
}

bool SchedulerMutex::Try_Lock()
{
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (!locked) {
		locked = true;
		spinLock.clear(std::memory_order_release);
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
		
		if (OnBareThread()) ++t_heldMutexes;
		return true;
	}
	spinLock.clear(std::memory_order_release);
	return false;
}

void SchedulerSemaphore::Wait(Pin pin) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;
	if (current != nullptr) {
		WaitNode node;   // on this fiber's stack: reachable exactly while this frame waits
		{

			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }

			if (permits > 0) {
				--permits;
				spinLock.clear(std::memory_order_release);
				return;
			}

			current->BeginSuspend(pin);
			node.fiber = current;
			waiters.PushBack(&node);
			spinLock.clear(std::memory_order_release);
		}
		CheckSuspendableCurrent("SchedulerSemaphore::Wait");
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerSemaphore::Wait");
		Thread::TsanSwitchToScheduler();
		ContextSwitch(&current->ctx, current->homeCtx);
	}
	else {
		
		SpinThenHelp([this] { return Try_Wait(); });
	}
}

WaitResult SchedulerSemaphore::WaitCancellable(Pin pin) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
	const uint32_t tok = callerTask ? callerTask->cancelToken : CancelToken::kNone;

	if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;

	if (current != nullptr) {
		WaitResult result = WaitResult::Ok;
		WaitNode   node;   // on this fiber's stack
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }

			if (permits > 0) {
				--permits;
				spinLock.clear(std::memory_order_release);
				return WaitResult::Ok;
			}

			if (IsTaskCancelled(callerTask)) {
				spinLock.clear(std::memory_order_release);
				return WaitResult::Cancelled;
			}

			current->BeginSuspend(pin);
			node.fiber = current; node.result = &result; node.token = tok;
			waiters.PushBack(&node);
			spinLock.clear(std::memory_order_release);
		}
		CheckSuspendableCurrent("SchedulerSemaphore::WaitCancellable");
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerSemaphore::WaitCancellable");
		Thread::TsanSwitchToScheduler();
		ContextSwitch(&current->ctx, current->homeCtx);

		return result;
	}

	bool got = false;
	SpinThenHelp([&] {
		if (Try_Wait()) { got = true; return true; }
		return IsTaskCancelled(callerTask);
	});
	return got ? WaitResult::Ok : WaitResult::Cancelled;
}

SchedulerSemaphore::ScopedPermit::ScopedPermit(SchedulerSemaphore& s) : sem(s) {
	sem.Wait();
	if (OnBareThread()) ++t_heldMutexes;
}

SchedulerSemaphore::ScopedPermit::~ScopedPermit() {
	
	if (OnBareThread() && t_heldMutexes > 0) --t_heldMutexes;
	sem.Signal();
}

bool SchedulerSemaphore::WaitAsyncEnqueue(Task* coroTask, WaitNode* node, WaitResult* result) {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (permits > 0) {
		--permits;
		spinLock.clear(std::memory_order_release);
		if (result) *result = WaitResult::Ok;
		return true;
	}

	if (IsTaskCancelled(coroTask)) {
		spinLock.clear(std::memory_order_release);
		if (result) *result = WaitResult::Cancelled;
		return true;
	}

	node->coro   = coroTask;
	node->result = result;
	node->token  = coroTask ? coroTask->cancelToken : CancelToken::kNone;
	waiters.PushBack(node);
	spinLock.clear(std::memory_order_release);
	return false;
}

bool SchedulerSemaphore::Try_Wait() {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (permits > 0) {
		--permits;
		spinLock.clear(std::memory_order_release);
		return true;
	}
	spinLock.clear(std::memory_order_release);
	return false;
}

void SchedulerSemaphore::Signal()
{
	
	// Every decision is made while holding the lock, and only the wake happens outside it. The
	// queue used to be re-examined through a snapshot taken BEFORE the unlock: if a waiter queued
	// in that window, the stale "it was empty" said to bank a permit instead, and that waiter was
	// never woken -- a permit sitting unused next to a task asleep forever.
	for (;;) {
		while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }

		WaitNode* const next = waiters.PopFront();
		if (!next) {
			if (permits < maxPermits) ++permits;
			spinLock.clear(std::memory_order_release);
			return;
		}

		// Everything the node holds is read while the node is still ours -- it lives on the
		// waiter's stack, and the wake below lets that frame run and return.
		Fiber* const f  = next->fiber;
		Task*  const co = next->coro;
		WaitResult* const res = next->result;
		spinLock.clear(std::memory_order_release);

		// A cancelled waiter still has to be woken to learn it was cancelled, but it does NOT
		// consume the signal: keep looking for a live waiter, and bank the permit only when the
		// queue is really empty (checked under the lock, above).
		const bool skip = res && IsTaskCancelled(f ? f->owningTask : co);
		if (res) *res = skip ? WaitResult::Cancelled : WaitResult::Ok;

		if (f) Thread::Resume(f);
		else if (co && TaskScheduler::IsInitialized())
			TaskScheduler::Instance().WakeTask(co);

		if (!skip) return;
	}
}

void SchedulerSemaphore::CancelWaiters(CancelToken tok) {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	WaitNode* victims = waiters.TakeIf([tok](const WaitNode* w) {
		return w->result && (!tok.Valid() || CancelToken(w->token).IsWithin(tok));
	});
	spinLock.clear(std::memory_order_release);

	while (victims) {
		WaitNode* const nextVictim = victims->next;
		Fiber* const f  = victims->fiber;
		Task*  const co = victims->coro;
		*victims->result = WaitResult::Cancelled;

		if (f) Thread::Resume(f);
		// WakeTask, not Push: the coroutine is already started, so it must resume through the
		// resume path (which honours its pin) rather than be placed like a fresh task.
		else if (co && TaskScheduler::IsInitialized())
			TaskScheduler::Instance().WakeTask(co);

		victims = nextVictim;
	}
}

void SchedulerConditionVariable::LockQueue() {
	while (spinLock.test_and_set(std::memory_order_acquire)) {
		platform::CpuRelax();
	}
}

void SchedulerConditionVariable::UnlockQueue() {
	spinLock.clear(std::memory_order_release);
}

void SchedulerConditionVariable::Wait(SchedulerMutex& mutex, Pin pin) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		WaitNode node;   // on this fiber's stack
		node.fiber = current;

		// BeginSuspend BEFORE the node is reachable, then release the mutex: the same order the
		// mutex's own wait path uses. Publishing first would let a notify resume a fiber that has
		// not started suspending; releasing the mutex first would let one land before this fiber is
		// in the queue at all, which is the lost wakeup a condition variable exists to avoid.
		LockQueue();
		current->BeginSuspend(pin);
		waitingQueue.PushBack(&node);
		UnlockQueue();

		mutex.Unlock();

		CheckSuspendableCurrent("SchedulerConditionVariable::Wait");
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerConditionVariable::Wait");
		Thread::TsanSwitchToScheduler();
		ContextSwitch(&current->ctx, current->homeCtx);

		mutex.Lock(pin);
	}
	else {

		mutex.Unlock();
		ContendedSpinStep();
		mutex.Lock();
	}
}

WaitResult SchedulerConditionVariable::WaitCancellable(SchedulerMutex& mutex, Pin pin) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
	const uint32_t tok = callerTask ? callerTask->cancelToken : CancelToken::kNone;

	if (current != nullptr) {
		
		if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;

		WaitResult result = WaitResult::Ok;
		WaitNode   node;   // on this fiber's stack
		node.fiber  = current;
		node.result = &result;
		node.token  = tok;

		LockQueue();

		if (IsTaskCancelled(callerTask)) {
			UnlockQueue();
			return WaitResult::Cancelled;
		}
		current->BeginSuspend(pin);
		waitingQueue.PushBack(&node);
		UnlockQueue();

		mutex.Unlock();

		CheckSuspendableCurrent("SchedulerConditionVariable::WaitCancellable");
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerConditionVariable::WaitCancellable");
		Thread::TsanSwitchToScheduler();
		ContextSwitch(&current->ctx, current->homeCtx);

		// Nothing to unlink: whoever woke this fiber -- a notify or CancelWaiters -- took the node
		// off the queue before resuming it, which is also what lets the node live on this stack.
		mutex.Lock(pin);
		return result;
	}

	if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;
	mutex.Unlock();
	ContendedSpinStep();
	mutex.Lock();
	return IsTaskCancelled(callerTask) ? WaitResult::Cancelled : WaitResult::Ok;
}

void SchedulerConditionVariable::CancelWaiters(CancelToken tok) {
	LockQueue();
	WaitNode* victims = waitingQueue.TakeIf([tok](const WaitNode* w) {
		return w->result && (!tok.Valid() || CancelToken(w->token).IsWithin(tok));
	});
	UnlockQueue();

	while (victims) {
		// Detached, so nobody else can reach these nodes -- but each one still lives on its waiter's
		// stack, so read it out before the wake lets that frame run.
		WaitNode* const next = victims->next;
		Fiber* const f  = victims->fiber;
		Task*  const co = victims->coro;
		*victims->result = WaitResult::Cancelled;

		if (f) Thread::Resume(f);
		else if (co && TaskScheduler::IsInitialized())
			TaskScheduler::Instance().WakeTask(co);

		victims = next;
	}
}

// The wake happens OUTSIDE the queue lock now, and that is safe for the reason the old
// semaphore-per-waiter version could not manage: popping a node detaches it, and a waiter cannot
// return -- cannot take its stack, and its node, with it -- until this resume puts it back on a
// worker. So the node stays ours between the pop and the wake without the lock helping.
void SchedulerConditionVariable::Notify_One() {
	for (;;) {
		LockQueue();
		WaitNode* const n = waitingQueue.PopFront();
		if (!n) { UnlockQueue(); return; }
		Fiber* const f  = n->fiber;
		Task*  const co = n->coro;
		WaitResult* const res = n->result;
		UnlockQueue();

		// A cancelled waiter is woken so it can learn it was cancelled, but it does not consume the
		// notification: keep looking for a live one.
		const bool skip = res && IsTaskCancelled(f ? f->owningTask : co);
		if (res) *res = skip ? WaitResult::Cancelled : WaitResult::Ok;

		if (f) Thread::Resume(f);
		else if (co && TaskScheduler::IsInitialized())
			TaskScheduler::Instance().WakeTask(co);

		if (!skip) return;
	}
}

void SchedulerConditionVariable::Notify_All() {
	LockQueue();
	WaitNode* n = waitingQueue.TakeAll();
	UnlockQueue();

	while (n) {
		WaitNode* const next = n->next;
		Fiber* const f  = n->fiber;
		Task*  const co = n->coro;
		if (n->result)
			*n->result = IsTaskCancelled(f ? f->owningTask : co) ? WaitResult::Cancelled
			                                                     : WaitResult::Ok;
		if (f) Thread::Resume(f);
		else if (co && TaskScheduler::IsInitialized())
			TaskScheduler::Instance().WakeTask(co);
		n = next;
	}
}

void TaskScheduler::ParallelFor(int begin, int end, std::function<void(int, int)> func) {
	const int n = end - begin;
	if (n <= 0) return;
	const size_t leaves = std::max<size_t>(1, workers.size() * 8);
	const int grain = (int)std::max<size_t>(1, ((size_t)n + leaves - 1) / leaves);
	ParallelFor(begin, end, grain, std::move(func));
}

