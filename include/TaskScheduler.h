// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#define NOMINMAX
#include "Task.h"
#include "Stats.h"
#include "CancelToken.h"
#include "TaskMPSCQueue.h"
#include "concurrentqueue.h"   
#include "Epochs.h"
#include "TaskDeque.h"
#include "TaskAllocator.h"
#include "Topology.h"   
#include <cstdio>   
#include <cstdlib>  
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>   
#include <functional>
#include <string>
#include <unordered_map>
#include <thread>

#include <queue>
#include "GlobalFiberPool.h"

#include "DirectEvent.h"
#include "WaitGroup.h"
#include "WaitPrimitive.h"
#include "WaitList.h"
#include "Event.h"
namespace JLib {
	
	class FiberRegistry;
	class Thread;

	namespace detail {
		inline void RecordTaskSize(size_t n) { JLIB_STAT_HIST(TaskBytes, n); (void)n; }
	}

	enum class WaitResult : uint8_t {
		Ok,          
		Cancelled,   
	};

	class TaskScheduler;

	namespace detail {
		
		void TeardownForTesting(TaskScheduler& scheduler);

		void DestroyForTesting();
	}

	class TaskScheduler {
		friend void detail::TeardownForTesting(TaskScheduler&);
		friend void detail::DestroyForTesting();
		friend class Thread;
		
		friend class WaitPrimitive;
		friend class GlobalFiberPool;
		friend class TaskDAG;   // released nodes are a fan-out: PushTarget (round-robin), not Push

	public:
		
		Task* GetCurrentTask() const;

		static TaskType CurrentTaskType() noexcept;
		void CleanupTaskMetadata(Task* task);

		bool DiscardIfCancelled(Task* task);

		static TaskScheduler& Instance() {
			if (!instance)
				throw std::runtime_error("Call TaskScheduler::Init() before Instance()!");
			return *instance;
		}
		// Init(Mode::Migrate)                     -> main in the pool
		// Init(Mode::Pinned)                      -> main out of the pool
		// Init(Mode::Pinned, MainMode::InPool)    -> explicit
		static void Init(Mode mode, MainMode main = MainMode::Default, size_t poolSize = 0);
		// Kept as it always was: Mode::Migrate with main OUT of the pool.
		static void Init(size_t poolSize = 0);

		~TaskScheduler();
		bool PushMain(Task* task);
		void ProcessMainThread();
		
		void WaitForMain(WaitGroup& wg);
		void NotifyAll();

		void ResumeAll();

		void DumpPoolState(const char* why) const;
		
        enum class AffinityPolicy : uint8_t { Hard = 0, Ideal, None, PhysicalOnly };
		static AffinityPolicy GetAffinityPolicy();

		// K after clamping to the pool: Config::hotWorkers, capped at kMaxReservedWorkers and n-1.
		static size_t GetHotWorkers() noexcept;

		static size_t ReservedBase(size_t nAll) noexcept {
			const size_t k = GetHotWorkers();
			return (k <= nAll) ? (nAll - k) : 0;
		}
		static bool IsReservedIndex(size_t q, size_t nAll) noexcept {
			return q >= ReservedBase(nAll);
		}

		static bool   HiPriLaneActive() noexcept;

		static constexpr size_t kMaxReservedWorkers = 2;

		static constexpr size_t kFatDeque = 32;

		static void   ClampHotWorkersToPool() noexcept;

		static unsigned GetWorkerParkCount(size_t q) noexcept;
		static void     ResetWorkerParkCounts() noexcept;

		static void     LeaveHunt() noexcept;
		static uint32_t SearchingCount() noexcept;

		static bool     TryLeaveHuntForPark() noexcept;

		// Says "I am looking for work": the counter LeaveHunt's 1->0 handoff and the last-hunter
		// rule are built on. Unconditional -- nothing caps how many workers may search.
		static void     EnterHunt() noexcept;

		// Chosen at Init (see Mode, MainMode).
		static MainMode GetMainMode() noexcept;
		static Mode     GetMode() noexcept;
		static bool     PinForced() noexcept { return GetMode() == Mode::Pinned; }

		// MainMode::InPool only, called on main: run pool work until nothing is left for main.
		static bool MainWorker();
		// MainMode::InPool only: wake main out of a WaitFor so it re-checks its predicate.
		static void WakeMain() noexcept;

		// Called on a pool thread (main in the pool included) about to block in code it cannot
		// suspend: a third-party library, a driver call, Present with vsync, a sleep. Its normal
		// inbox is adopted by a free compute worker, which drains it until BlockEnd, so nothing
		// pushed to this thread waits for it; its deque was always stealable. Pinned and main-only
		// work (the hi-pri inbox) still waits -- only this thread may run it. Blocking inside an
		// EpochGuard is fatal (it would stall reclamation for everyone). Nests; a no-op off the
		// pool. If no worker is free to adopt (at most half the pool can be adopted at once) the
		// inbox is moved onto the deque once and later pushes wait. Model: tests/verify/adopt_model.c.
		static void BlockBegin();
		static void BlockEnd();
		template <typename F>
		static decltype(auto) BlockInPlace(F&& f) {
			BlockBegin();
			struct End { ~End() { BlockEnd(); } } end;
			return f();
		}
		// The same for main, kept by name.
		static void MainAwayBegin() { BlockBegin(); }
		static void MainAwayEnd()   { BlockEnd(); }
		class MainAway {
		public:
			MainAway()  { MainAwayBegin(); }
			~MainAway() { MainAwayEnd(); }
			MainAway(const MainAway&) = delete;
			MainAway& operator=(const MainAway&) = delete;
		};

		static constexpr size_t kNoIdleWorker = ~(size_t)0;

		static constexpr size_t kAnyWorker = ~(size_t)0;
		static void   InitIdleStack(size_t n);
		static void   RegisterIdleWorker(size_t q) noexcept;   
		static void   UnregisterIdleWorker(size_t q) noexcept; 
		static size_t PopIdleWorker() noexcept;                
		static size_t IdleWorkerCount() noexcept;              

		static unsigned LastBareWaitPolls()  noexcept;
		static unsigned LastBareWaitYields() noexcept;
		static unsigned LastBareWaitHelped() noexcept;

		enum class PowerThrottling : uint8_t { Topology = 0, OptOut, SystemManaged, Force };
		static PowerThrottling GetWorkerPowerThrottling();

		// How ParallelFor spreads a range (after the width/grain front end).
		//   Cursor:    one shared cursor, each helper claims a grain at a time. Best for compute
		//              bodies with pieces of ~1-30 us.
		//   LazyPCore: one contiguous range per P-core, then lazy binary splitting through the
		//              workers' own deques (E-cores join by stealing halves). Best for tiny pieces,
		//              where the shared cursor contends, and for memory-bound bodies at any size
		//              (each thread streams a contiguous range). Main out of the pool splits onto
		//              the non-worker deque (push-only).
		//   Auto:      LazyPCore when the probed cost of a piece is under 0.5 us, else Cursor
		//              (Cursor when there is no probe: measuredWidth off). It cannot tell a
		//              memory-bound body; pass LazyPCore per call for those.
		enum class PforMode : uint8_t { Cursor = 0, LazyPCore = 1, Auto = 2 };

		void ParallelFor(int begin, int end, int grain, std::function<void(int, int)> func);
		// The same with this call's PforMode instead of the pool's (e.g. LazyPCore for a
		// memory-bound body, which Auto cannot detect).
		void ParallelFor(int begin, int end, int grain, std::function<void(int, int)> func, PforMode mode);

		void ParallelFor(int begin, int end, std::function<void(int, int)> func);
		
		void RunCursorRange(int start, int end, int grain, std::function<void(int, int)>& func);
		// PforMode::LazyPCore. Hands one contiguous range to each P-core (idle ones by their normal
		// inbox; busy ones' shares go on the caller's own deque instead, stealable at once), then the
		// caller and every piece run grain by grain and, while their own deque is empty, push the
		// upper half there for a thief.
		void RunLazyRange(int start, int end, int grain, std::function<void(int, int)>& func, size_t width);
		void RunLazyPiece(std::function<void(int, int)>* func, int lo, int hi, int grain, WaitGroup* wg);

		bool Push(Task* task);
		void WaitFor(WaitGroup& wg, Pin pin = Pin::None);
		// Returns when the group reaches 0, pred(arg) is true, or the pool stops. Whoever makes
		// pred true must call WakeMain(). `wg` may be null to wait on the predicate alone.
		// Returns true if the group finished or pred became true.
		bool WaitFor(WaitGroup* wg, bool (*pred)(void*), void* arg, Pin pin = Pin::None);

		bool PushTo(size_t worker, Task* task);
		// Round-robin over the compute workers of one core class, skipping main and threads that
		// are away. hiPri: that worker's hi-pri inbox (never stolen); otherwise its normal inbox,
		// which it drains to its deque, where the work can be stolen.
		bool PushTo(Task* task, CorePref pref = CorePref::P, bool hiPri = true);

		bool PushIO(Task* task) noexcept;

		// How often K stole, and how often I/O arrived after the pickup and the task was given back.
		static void     NoteReservedSteal() noexcept;
		static void     NoteReservedStealReturned() noexcept;
		static void     GetReservedStealStats(uint64_t& steals, uint64_t& returned) noexcept;
		static bool     IoLaneQuiet() noexcept;

		static bool  PushLaneIntake(Task** tasks, size_t n) noexcept;
		static Task* TakeLaneIntake() noexcept;
		static bool  LaneIntakeIdle() noexcept;
		
		enum class RequeueResult { Failed, Pinned, Stealable };
		RequeueResult Requeue(Task* task);

		// Where a suspended task goes when it becomes runnable again (reads record->pinTo once).
		//   pinned:   PushTo(pinTo) -- that worker's hi-pri inbox, never stolen.
		//   unpinned: a compute worker keeps it on the bottom of its own deque (stealable). Main, K
		//             and non-pool threads place it with Requeue.
		// Separate from Requeue on purpose.
		void ResumeFiber(Task* task);
		// Same rule for a yield. Returns true if the task stayed on the calling thread's own queues.
		bool YieldFiber(Task* task);
		// External waiters (locks, semaphore, future, acceptor, I/O) wake a task here. A started task
		// resumes through ResumeFiber; a fresh task is pushed.
		bool WakeTask(Task* task);
		// A batch placer (the reactors' lane steering) must hand a pinned task to WakeTask instead.
		static bool IsPinned(const Task* t) {
			return t->record && t->record->pinTo != TaskRecord::kNoPin;
		}
		// The only way a TaskType::Main task is ever queued. Main in the pool: PushTo(0). Main out
		// of the pool: mainQ push + kick main.
		void PushMainQueue(Task* task);
		// Main out of the pool: run one mainQ task (false if none).
		bool RunOneMainTask();
		// Main out of the pool inside WaitFor: routed work, bounded steals, then park on the group.
		void OutOfPoolMainWait(WaitGroup& wg);
		
		// How a batch is handed out:
		//   Wide:   split into runs (about 64 tasks each) across several workers' inboxes, one wake each.
		//   Narrow: the whole batch into ONE worker's inbox with one wake; that worker moves it into
		//           its own deque as it goes, where the rest of the pool can steal it.
		// A batch has no lane: it is a run for a compute worker to load onto its own deque. Latency
		// work goes to the shared lane intake (PushLaneIntake), never as a batch.
		enum class BatchSpread : uint8_t { Wide, Narrow };
		void PushBatch(Task* tasks[], size_t count, BatchSpread spread);

		// Explicit form: a specific worker (everything to it), or kAnyWorker with runs of at least
		// minPerSegment tasks. A batch aimed at K goes to a compute worker instead.
		void PushBatch(Task* tasks[], size_t count, size_t worker = kAnyWorker, size_t minPerSegment=64);

		template<typename F>
		
		size_t PushArray(size_t begin, size_t end, size_t chunkSize, F&& fn, WaitGroup* wg = nullptr,
		                 Lane lane = Lane::Normal) {
			if (end <= begin) return 0;
			if (chunkSize == 0) chunkSize = 1;
			const size_t total  = end - begin;
			const size_t chunks = (total + chunkSize - 1) / chunkSize;

			std::vector<Task*> ts;
			ts.reserve(chunks);
			for (size_t c = 0; c < chunks; ++c) {
				const size_t lo = begin + c * chunkSize;
				const size_t hi = (lo + chunkSize > end) ? end : lo + chunkSize;
				
				Task* t = CreateInternalTask([fn, lo, hi]() { for (size_t i = lo; i < hi; ++i) fn(i); }, lane);
				if (!t) {                                   
					for (size_t i = lo; i < hi; ++i) fn(i);
					continue;
				}
				t->waitGroup = wg;
				ts.push_back(t);
			}

			if (wg && !ts.empty())
				wg->n.fetch_add((int)ts.size(), std::memory_order_relaxed);
			if (!ts.empty()) {
				// Latency chunks go to the shared intake; refused (off, or no K), they are a batch.
				if (!(IsLowLatency(lane) && HiPriLaneActive() && LaneIntakeEnabled()
				      && PushLaneIntake(ts.data(), ts.size())))
					PushBatch(ts.data(), ts.size(), kAnyWorker, 64);
			}
			return ts.size();
		}
		
		static bool IsInitialized() {
			return instance != nullptr;
		}

		size_t GetWorkerCount() const;

		const std::vector<Thread*>& GetThreads() const { return workers; }

		void PrefaultTaskSlots(size_t slots);

		struct SlabSizes {

			size_t slots256 = 4 * 1024;
			size_t slots128 = 2 * 1024;
			size_t slots80  = 16 * 1024;
			size_t slots64  = 24 * 1024;
			size_t slots512 = 1024;      // coroutine frames and lambda tasks of 257-512 B
		};

		struct FiberBudget {
			size_t normalPerComputeWorker = 64;
			size_t tinyPerKWorker         = 64;
			size_t deepPerComputeWorker   = 1;
		};

		// Settable while the pool runs (the Set* instance methods below). Each Init starts from the
		// values in its Config; nothing carries over from an earlier pool.
		struct Tunables {
			PforMode pforMode          = PforMode::Auto;
			// A hunter that stole from a victim tries the same victim again on its next pass, up to
			// this many steals in a row; a miss (or the victim's flag clear) ends it. 1 = off.
			// Clamped to 1..8. K never sticks. Measured best at 8 for single- and multi-producer
			// fan-out (and it narrows the gap between producers); neutral on fork trees and pfor.
			unsigned stickyStealCap    = 8;
			size_t   minItersPerWorker = 64;
			size_t   leavesPerWorker   = 8;
			bool     measuredWidth     = true;
			bool     rememberedCost    = false;
			unsigned wakeCostNs        = 3000;
			bool     parallelForSerial = false;
			unsigned ioQuietWindowUs   = 500;
			bool     reservedStealing  = true;
			bool     laneIntake        = true;
			bool     bareWaitHelp      = false;
			int      fastSpinTries     = -1;   // -1 = the build's JLIBSCHED_FAST_SPIN_TRIES
		};

		// Everything that shapes a pool. Read once by Init and fixed for that pool's life.
		struct Config {
			Mode            mode      = Mode::Migrate;
			MainMode        main      = MainMode::Default;
			size_t          workers   = 0;          // 0 = GetSafeTC()
			size_t          hotWorkers = 0;         // K; at least 1 when io is on
			AffinityPolicy  affinity  = AffinityPolicy::Ideal;
			PowerThrottling power     = PowerThrottling::OptOut;
			unsigned        reservedCores = 0;
			bool            timers    = false;      // also reserves a core
			bool            io        = false;      // implies timers
			unsigned        ioCompletionThreads = 1;
			FiberBudget     fibers;
			size_t          fiberMemoryLimit = size_t(1) << 30;   // 0 = no limit
			SlabSizes       slab;
			bool            slabGrowth   = true;
			bool            lazyTaskSlab = false;
			// Epoch slots for threads outside the pool (ThreadScope), reserved at Init. Only
			// claimed slots cost anything: an idle one never holds back reclamation.
			size_t          externalThreads = 16;
			// Parked OS threads that stand in for a thread blocked in BlockInPlace: a spare adopts
			// its inbox before any worker does, so the pool keeps its full width while the call
			// blocks (e.g. Present with vsync every frame). 0 = adoption by workers only, no extra
			// threads. Not used in Mode::Pinned (a spare is not a slot; nothing can pin to it).
			size_t          spareThreads = 0;
			Tunables        tunables;
		};

		static void Init(const Config& cfg);
		// The Config the running pool was started with (defaults before Init).
		static const Config& CurrentConfig() noexcept;
		// Which pool this is: 1 for the first Init in the process, then 2, ... 0 when none is running.
		// Memory that must go back to the pool that made it records this.
		static std::uint64_t PoolGeneration() noexcept;

		void     SetMinItersPerWorker(size_t n) noexcept;
		size_t   GetMinItersPerWorker() const noexcept;
		void     SetLeavesPerWorker(size_t n) noexcept;
		size_t   GetLeavesPerWorker() const noexcept;
		void     SetMeasuredWidth(bool on) noexcept;
		bool     GetMeasuredWidth() const noexcept;
		void     SetRememberedCost(bool on) noexcept;
		bool     GetRememberedCost() const noexcept;
		void     SetWakeCostNs(unsigned ns) noexcept;
		unsigned GetWakeCostNs() const noexcept;
		void     SetParallelForSerial(bool on) noexcept;
		bool     ParallelForSerial() const noexcept;
		void     SetPforMode(PforMode m) noexcept;
		PforMode GetPforMode() const noexcept;
		void     SetStickyStealCap(unsigned n) noexcept;
		unsigned GetStickyStealCap() const noexcept;
		void     SetIoQuietWindowUs(unsigned us) noexcept;
		unsigned IoQuietWindowUs() const noexcept;
		void     SetReservedStealing(bool on) noexcept;
		bool     ReservedStealing() const noexcept;
		void     SetLaneIntake(bool on) noexcept;
		bool     LaneIntakeEnabled() const noexcept;
		void     SetBareWaitHelp(bool on) noexcept;
		bool     BareWaitHelp() const noexcept;
#if defined(JLIBSCHED_TUNABLE_FAST_SPIN)
		void     SetFastSpinTries(int n) noexcept;
		int      GetFastSpinTries() const noexcept;
#endif

		static bool LazyTaskSlabEnabled();
		static bool SlabGrowthEnabled() noexcept;
		static SlabSizes CurrentSlabSizes();

		static bool TimersEnabled() noexcept;

		static bool IoReactorEnabled() noexcept;
		static unsigned IoCompletionThreads() noexcept;

		static bool ReserveTimerCore() noexcept;

		static std::string SlabUsageString(const char* label = "slab usage");

		static void ReportSlabUsage(const char* label = "slab usage");

		static TaskAllocator::Usage SlabUsage();

		static std::uint64_t OutstandingFiberRows() noexcept;

		static unsigned GetReservedCores() noexcept;
		static bool ReserveIoCore() noexcept;

		static size_t NormalFibersPerComputeWorker();
		static size_t TinyFibersPerKWorker();
		static size_t DeepFibersPerComputeWorker();
		// Fibers are made in blocks as tasks need them; the budget is the first block. Growth stops
		// at Config::fiberMemoryLimit bytes of fiber stack across all classes.
		static size_t FiberMemoryLimit();


		static uint16_t AllocFiberLocalSlot() noexcept;

		static FiberRegistry& Fibers() noexcept;

		static bool   HasFiberLocal() noexcept;
		static void*& FiberLocal(size_t slot) noexcept;

		template <typename T>
		static T* FiberLocalAs(size_t slot) noexcept {
			return static_cast<T*>(FiberLocal(slot));
		}

		static bool ReleaseOnFiberDeath(FiberDebt& node, void* obj,
		                                void (*release)(void*) noexcept) noexcept;

		static bool ReleaseOnWorker(FiberDebt& node, void* obj,
		                            void (*release)(void*) noexcept,
		                            size_t holder, uint32_t kind) noexcept;

		static size_t DischargeFiberDebts(TaskRecord* r, size_t holder) noexcept;

		// The record of the task running on this thread (fiber or not), or null.
		static TaskRecord* CurrentRecord() noexcept;
		// That record's task-local block, allocated on first use; null when not in a task.
		static void**      CurrentLocals() noexcept;
		// Frees a record: task-local slots (with their deleters), leftover debts, the record itself.
		void               ReleaseRecord(TaskRecord* r) noexcept;

		template <typename T>
		static bool DeleteOnFiberDeath(FiberDebt& node, T* p) noexcept {
			if (!p) return false;
			return ReleaseOnFiberDeath(node, p,
				[](void* q) noexcept { delete static_cast<T*>(q); });
		}


		static bool PushResume(size_t worker, Task* task);

		static bool NotifyHolder(size_t worker);

		GlobalFiberPool& GetGlobalPool();
		
		Event& GetEvent(const std::string& name);
		
		void WaitOnEvent(Event& ev, Pin pin = Pin::None);
		void WaitOnEvent(const std::string& eventName, Pin pin = Pin::None);

		[[nodiscard]] WaitResult WaitOnEventCancellable(Event& ev, Pin pin = Pin::None);
		
		void WaitOnEventArmed(Event& ev, const std::function<void()>& arm, Pin pin = Pin::None);
		void WaitOnEventArmed(const std::string& eventName, const std::function<void()>& arm, Pin pin = Pin::None);
		
		void WaitOnEventDirectArmed(const std::function<void(DirectEvent*)>& arm, Pin pin = Pin::None);
		
		bool IsOnFiber();
		void Pause();
		void Resume();
		void Stop(Task* worker_task);
		TaskAllocator* GetAllocator();

		bool TryRunStolenNativeTask();

		Task* CreateTaskImpl(void(*fn)(void*), void* data, Lane lane, TaskType type,
		                     StackClass stack = StackClass::Standard);

		// Every task gets its record at birth; FreeTask is the one place a task and its record die.
		TaskRecord* NewTaskRecord();
		void        FreeTask(Task* t);

		Task* CreateTask(void(*fn)(void*), void* data, Lane lane = Lane::Normal, TaskType type = TaskType::Fiber,
		                 StackClass stack = StackClass::Standard) {
			return CreateTaskImpl(fn, data, lane, type, stack);
		}

		// A native task: fn(data) is called on the worker's own stack, no fiber is checked out. It
		// must not suspend (WaitFor, SchedulerMutex, ... are fatal inside it); it may block the OS
		// thread only inside BlockInPlace, which hands its inbox to another thread meanwhile.
		Task* CreateNativeTask(void(*fn)(void*), void* data, Lane lane = Lane::Normal) {
			Task* t = CreateTaskImpl(fn, data, lane, TaskType::Fiber);
			if (t) t->native = 1;
			return t;
		}

		Task* CreateInternalTask(void(*fn)(void*), void* data, Lane lane = Lane::Normal,
		                         StackClass stack = StackClass::Standard) {
			return CreateTaskImpl(fn, data, lane, TaskType::Fiber, stack);
		}

		template<typename F>
		auto CreateTask(F&& f, Lane lane = Lane::Normal, StackClass stack = StackClass::Standard) {
			constexpr TaskType type = TaskType::Fiber;   // may run on a fiber, must not suspend (native)
			using L = LambdaTask<std::decay_t<F>>;
			
			detail::RecordTaskSize(sizeof(L));

			static_assert(alignof(L) <= 16, "lambda over-aligned for the slot");

			void* mem = taskAllocator.AllocSized(sizeof(L));
			if (!mem) { JLIB_STAT(TaskHeapAllocs); mem = ::operator new(sizeof(L)); }
			if (!mem) return static_cast<L*>(nullptr);
			L* t = ::new (mem) L(std::forward<F>(f));
			t->record = NewTaskRecord();
 			t->lane = lane;

			t->type = type;
			t->stackClass = stack;
			
			t->trivialDtor = std::is_trivially_destructible_v<std::decay_t<F>> ? 1 : 0;
			return t;
		}

		template<typename F>
		auto CreateInternalTask(F&& f, Lane lane = Lane::Normal, StackClass stack = StackClass::Standard) {
			return CreateTask(std::forward<F>(f), lane, stack);
		}

		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushTarget(t);
		}
	private:

		explicit TaskScheduler(const Config& cfg);

		void Join();

		struct AtExitDestroyer { ~AtExitDestroyer(); };
		static AtExitDestroyer atExitDestroyer;

		std::atomic<bool> paused{ false };

		std::vector<std::unique_ptr<TaskDeque>> deques;

		// "Deque q may have work": ONE BYTE PER DEQUE, PACKED, in an allocation of its own rather
		// than beside the deques. A thief reads these instead of the deques, so scanning never
		// pulls an owner's top_/bottom_ away from it.
		//
		// Not a bitmap: a bitmap is one word, so every owner's transition is an atomic
		// read-modify-write against every other owner's -- writers serialise on one address.
		// Separate bytes are plain relaxed stores to distinct addresses: no RMW, no serialising,
		// writers never wait on each other.
		//
		// Packed and NOT padded onto separate lines, deliberately. Packing means an owner's store
		// moves the line, but the two costs fall in opposite phases: transitions are frequent only
		// while the pool is BUSY, and hunters probe hard only while it is IDLE. So the false
		// sharing happens when nobody is reading, and the whole array is ONE FETCH when nobody is
		// writing. Padding would trade that single fetch for one per worker -- the cost this
		// exists to remove.
		struct FlagsDeleter {
			void operator()(std::atomic<std::uint8_t>* p) const noexcept {
				::operator delete(p, std::align_val_t(platform::kCacheLine));
			}
		};
		std::unique_ptr<std::atomic<std::uint8_t>[], FlagsDeleter> workFlags;

		bool DequeHasWork(size_t q) const noexcept {
			return workFlags && workFlags[q].load(std::memory_order_relaxed) != 0;
		}

		size_t nonWorkerLane = 0;
		std::atomic<bool> nonWorkerLaneClaimed{ false };

		TaskDeque* LaneForCurrentThread();
		size_t LaneIndexForCurrentThread();
		std::vector<std::unique_ptr<TaskMPSCQueue>> normalInboxes;
		
		moodycamel::ConcurrentQueue<Task*> laneIntake;

		// Direct-run inboxes: popped and run by their owner only, never moved to a deque, never
		// stolen. Hi-pri lane work and pinned resumes both land here.
		std::vector<std::unique_ptr<TaskMPSCQueue>> hiPriInboxes;

		static GlobalFiberPool* globalPool;
		
		JLIB_NOINLINE static int& ConsecutiveHiPriSteals() noexcept;   // TLS; see JLIB_NOINLINE
		static constexpr int kStealFairnessWindow = 8; 
		uint64_t GetCurrentTimeMs() const;
		
		void RunCounted(WaitGroup& wg, Task* t);
		static size_t GetSafeTC();
		
		Task* GetTask();
		void StartPool(size_t poolSize);
		
		bool PushTarget(Task* task, size_t worker = kAnyWorker);

		// Wakes whichever worker has adopted queue q's inbox (Thread::Wake, when q is away).
		void WakeAdopterOf(int q) noexcept;

		void MaybeBuddyWake(size_t k) noexcept;
		
		int PickNextWorker(Lane lane = Lane::Normal);
		
		void BuildTopology(unsigned int num_workers);
		
		std::vector<std::vector<int>> clusterMates;
		
		std::vector<std::vector<int>> matesSameClass, matesOtherClass;
		
		std::vector<int> qToCpu;

		std::vector<int> siblingQIndex;

		std::vector<topology::CpuMask> llcMaskOfWorker;
		
		std::vector<char> isPCore;
		
		std::vector<char> isPCpu;
		
		static TaskScheduler* instance;

		// Declared before taskAllocator: its slab sizes come from here.
		const Config cfg_;
		const std::uint64_t poolGen_;

		struct LiveTunables {
			std::atomic<size_t>   minItersPerWorker;
			std::atomic<size_t>   leavesPerWorker;
			std::atomic<bool>     measuredWidth;
			std::atomic<bool>     rememberedCost;
			std::atomic<unsigned> wakeCostNs;
			std::atomic<bool>     parallelForSerial;
			std::atomic<uint8_t>  pforMode;
			std::atomic<unsigned> stickyStealCap;
			std::atomic<unsigned> ioQuietWindowUs;
			std::atomic<bool>     reservedStealing;
			std::atomic<bool>     laneIntake;
			std::atomic<bool>     bareWaitHelp;
			std::atomic<int>      fastSpinTries;
			explicit LiveTunables(const Tunables& t) noexcept;
		};
		LiveTunables live_;

		// K after ClampHotWorkersToPool.
		std::atomic<size_t> hotWorkers_{ 0 };

		// "Workers looking for work" (EnterHunt/LeaveHunt). Its own cache line: every hunter hits it
		// with seq_cst read-modify-writes.
		struct alignas(64) HuntCounter { std::atomic<uint32_t> n{ 0 }; };
		HuntCounter searching_;

		// Parked workers a LeaveHunt can hand off to: a Treiber stack of worker indices (+1, 0 = empty)
		// with a version in the high 32 bits of the top.
		struct IdleNode {
			std::atomic<uint32_t> next{ 0 };
			std::atomic<uint32_t> armed{ 0 };
		};
		std::vector<IdleNode>  idleNodes_;
		std::atomic<uint64_t>  idleTop_{ 0 };
		std::atomic<uint32_t>  idleCount_{ 0 };

		// When the reserved lane last received I/O (IoLaneQuiet).
		std::atomic<long long> ioLastPushNs_{ 0 };

		TaskAllocator taskAllocator{ cfg_.slab.slots256, cfg_.slab.slots128, cfg_.slab.slots80,
		                             cfg_.slab.slots64, cfg_.slab.slots512, cfg_.lazyTaskSlab };

	public:
		
		struct AbiCanary { char unused; };
		AbiCanary abiCanary{};

	private:
		
		WaitPrimitive* primitivesHead = nullptr;
		std::mutex     primitivesMtx;

		std::unordered_map<std::string, std::unique_ptr<Event>> eventRegistry;
		std::mutex registryMtx;
		EventPool eventPool{ 1024 };   
		std::atomic<bool> poolActive{ false };
		
		static constexpr size_t kHintWords     = 4;
		static constexpr size_t kMaxHintQueues = kHintWords * 64;   

		bool WorkerQueuesEmpty(size_t q) const noexcept {
			if (q >= deques.size()) return true;
			
			if (q >= hiPriInboxes.size()) return deques[q]->empty();
			return deques[q]->empty() && hiPriInboxes[q]->empty();
		}
		
		std::atomic<size_t> ioSteer_{ 0 };

		std::atomic<int> nextWorker{ 0 };
		
		std::atomic<size_t> nextHotWorker{ 0 };
		std::atomic<size_t> nextClassWorker[2]{};   // PushTo(CorePref) round-robin: P, E
		
		std::vector<int> pWorkers, eWorkers;
		std::atomic<size_t> nextPWorker{ 0 }, nextEWorker{ 0 };
		std::atomic<bool> stopFlag{ false };
		
		std::vector<Thread*> workers;
		// MainMode::OutOfPool: main's helper Thread (not in workers; see Thread::isHelper).
		Thread* mainHelper = nullptr;
		// Config::spareThreads: parked stand-ins for a thread blocked in BlockInPlace (Thread::isSpare).
		std::vector<Thread*> spares;
		TaskMPSCQueue mainQ;
		std::mutex poolMutex;
	};

	using SchedulerConfig = TaskScheduler::Config;

	// Gives a thread outside the pool what pool threads have: an epoch slot (EpochGuard, RetirePtr
	// and the lock-free structures work), a hazard reader row, and its own retire bag. Put one at
	// the top of the thread's body, after Init:
	//
	//     std::thread([] { JLib::ThreadScope scope; /* ... use the data structures ... */ });
	//
	// Nests, and is a no-op on main or a pool thread (they already have a slot). Ending the scope
	// while an EpochGuard is open on the thread is fatal. What its bag still holds goes to the
	// shared orphan store, which the pool sweeps. Config::externalThreads sets how many threads
	// can hold a scope at once.
	class ThreadScope {
	public:
		ThreadScope();
		~ThreadScope();
		ThreadScope(const ThreadScope&) = delete;
		ThreadScope& operator=(const ThreadScope&) = delete;
		size_t Slot() const noexcept { return slot_; }
	private:
		size_t slot_  = kNoThreadSlot;
		bool   owner_ = false;   // false: the thread already had a slot, nothing to undo
	};

	inline bool IsTaskCancelled(const Task* t) {
		if (!t) return false;
		if (t->cancelledDirect) return true;
		return CancelToken(t->cancelToken).Cancelled();
	}

	bool CurrentTaskCancelled();

	// The waiter queues live in WaitList now (see WaitList.h): the node sits on the waiting fiber's
	// stack or inside the coroutine's awaiter, so joining a queue allocates nothing. The spinlock
	// each primitive holds across the queue operation guards its STATE (`locked`, `permits`), and
	// the enqueue has to be inside it -- publishing a waiter after dropping the lock is the missed
	// wake: the signal sees an empty queue and banks a permit next to a task asleep forever.

	// A lock whose waiter SUSPENDS instead of blocking its worker.
	//
	// USE std::mutex FOR SHORT CRITICAL SECTIONS. Taking an uncontended std::mutex is a single CAS;
	// this pays a suspend, a queue push, a wake and a resume, which is hundreds of nanoseconds. The
	// measured crossover is a hold of about a microsecond (bench cases lock_s*/lock_b*): below it,
	// std::mutex wins and no implementation work changes that -- it is arithmetic.
	//
	// USE THIS WHEN THE HOLD IS LONG, OR WHEN THE HOLDER MAY SUSPEND (waiting on I/O, a WaitGroup,
	// another task). With more waiters than workers, a blocking lock puts every worker to sleep in
	// the kernel and the whole pool stops until the lock is free. Measured (bench mix_susp vs
	// mix_block: 64 contenders holding 100 us, plus 4000 independent tasks): the independent work
	// finishes at 1.8 ms here against 123.9 ms with a blocking lock, while the totals match. The
	// lock itself is not faster -- everything else keeps running while it is held.
	class SchedulerMutex : public WaitPrimitive {
	private:
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;
		bool locked = false;
		Task* lockHolder = nullptr;
		WaitList waiters;
		std::atomic_flag holderLock = ATOMIC_FLAG_INIT;

		std::mutex              bareMtx;
		std::condition_variable bareCv;
		std::atomic<int>        bareWaiters{ 0 };

	public:
		
		static std::atomic<void(*)()> s_blockViolationHook;
	private:

	public:
		SchedulerMutex() = default;
		~SchedulerMutex() { LeaveRegistry(); }   

		void CancelWaiters(CancelToken tok = CancelToken{});

	protected:
		
		void DrainForShutdown() override { CancelWaiters(); }
	public:

		void Lock(Pin pin = Pin::None);

		void Unlock();

		bool Try_Lock();

		[[nodiscard]] WaitResult LockCancellable(Pin pin = Pin::None);

		// `node` is the awaiter's own WaitNode: it lives in the coroutine frame, which outlives the
		// wait. Only read on the false (queued) return.
		bool LockAsyncEnqueue(Task* coroTask, WaitNode* node);

		bool LockAsyncEnqueue(Task* coroTask, WaitNode* node, WaitResult* result);
	};

	class SchedulerSemaphore : public WaitPrimitive {
	private:
		WaitList waiters;
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;   // guards `permits` only
		int permits;
		const int maxPermits;

	public:

		explicit SchedulerSemaphore(int initialPermits, int maxPermits = INT_MAX)
			: permits(initialPermits), maxPermits(maxPermits) {}

		~SchedulerSemaphore() { LeaveRegistry(); }   

		void Wait(Pin pin = Pin::None);

		[[nodiscard]] WaitResult WaitCancellable(Pin pin = Pin::None);

		void CancelWaiters(CancelToken tok = CancelToken{});

	protected:
		
		void DrainForShutdown() override { CancelWaiters(); }
	public:

		bool Try_Wait();

		void Signal();

		bool WaitAsyncEnqueue(Task* coroTask, WaitNode* node, WaitResult* result = nullptr);

		class ScopedPermit {
		public:
			explicit ScopedPermit(SchedulerSemaphore& s);
			~ScopedPermit();
			ScopedPermit(const ScopedPermit&) = delete;
			ScopedPermit& operator=(const ScopedPermit&) = delete;
		private:
			SchedulerSemaphore& sem;
		};
	};

	class SchedulerConditionVariable : public WaitPrimitive {
	private:
		
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;

		// Waiters queue HERE, directly. Each one used to build a private SchedulerSemaphore(0,1) on
		// its stack and wait on that, so one CV wait was two primitives, two spinlocks and two
		// suspends, and a notify was a signal into a semaphore that then did the real wake. The node
		// is on the waiting fiber's stack, so queueing directly costs nothing and the wake is one
		// Resume.
		WaitList waitingQueue;

		void LockQueue();
		void UnlockQueue();

	public:
		SchedulerConditionVariable() = default;
		~SchedulerConditionVariable() { LeaveRegistry(); }   

		void Wait(SchedulerMutex& mutex, Pin pin = Pin::None);

		[[nodiscard]] WaitResult WaitCancellable(SchedulerMutex& mutex, Pin pin = Pin::None);

		void CancelWaiters(CancelToken tok = CancelToken{});

	protected:
		
		void DrainForShutdown() override { CancelWaiters(); }
	public:

		void Notify_One();

		void Notify_All();
	};

#if defined(_WIN32)
}
extern "C" __declspec(dllimport) void __stdcall OutputDebugStringA(const char*);
namespace JLib {
#endif

	namespace detail {
		
		#if defined(__GNUC__) || defined(__clang__)
		#	pragma GCC diagnostic push
		#	pragma GCC diagnostic ignored "-Winvalid-offsetof"
		#endif
		
		struct AbiComponents {
			
			uint32_t sizeEpochManager, taskLayout, sizeTaskAllocator, sizeTaskDeque;
			uint32_t sizeTaskMPSCQueue, sizeWaitGroup, sizeTaskScheduler;
			uint32_t offsetAbiCanary, iteratorDebugLevel;
		};

		namespace {
			
			inline uint32_t TaskLayoutFingerprint() {
				uint32_t h = 2166136261u;                       
				auto mix = [&h](uint32_t v) { h ^= v; h *= 16777619u; };
				mix((uint32_t)sizeof(Task));
				mix((uint32_t)alignof(Task));
				mix((uint32_t)sizeof(TaskFlagPacking));         
				mix(TaskFlagBitLayout());                       
				mix((uint32_t)offsetof(Task, fn));
				mix((uint32_t)offsetof(Task, data));
				mix((uint32_t)offsetof(Task, record));
				mix((uint32_t)offsetof(Task, next));
				mix((uint32_t)offsetof(Task, waitGroup));
				return h;
			}

			inline AbiComponents LocalAbiComponents() {
				AbiComponents c{};
				c.sizeEpochManager    = (uint32_t)sizeof(EpochManager);
				c.taskLayout          = TaskLayoutFingerprint();
				c.sizeTaskAllocator   = (uint32_t)sizeof(TaskAllocator);
				c.sizeTaskDeque       = (uint32_t)sizeof(TaskDeque);
				c.sizeTaskMPSCQueue   = (uint32_t)sizeof(TaskMPSCQueue);
				c.sizeWaitGroup       = (uint32_t)sizeof(WaitGroup);
				c.sizeTaskScheduler   = (uint32_t)sizeof(TaskScheduler);
				c.offsetAbiCanary     = (uint32_t)offsetof(TaskScheduler, abiCanary);
#if defined(_ITERATOR_DEBUG_LEVEL)
				c.iteratorDebugLevel  = (uint32_t)_ITERATOR_DEBUG_LEVEL;
#endif
				return c;
			}
		}
		#if defined(__GNUC__) || defined(__clang__)
		#	pragma GCC diagnostic pop
		#endif

		AbiComponents JLibScheduler_STALE_LIBRARY_rebuild_the_Scheduler_for_this_configuration();

		namespace {
		[[maybe_unused]] const bool g_abiChecked = [] {
			const AbiComponents lib = JLibScheduler_STALE_LIBRARY_rebuild_the_Scheduler_for_this_configuration();
			const AbiComponents hdr = LocalAbiComponents();

			char msg[1600];
			int n = std::snprintf(msg, sizeof msg,
				"[JLib::Scheduler] FATAL: this translation unit was compiled against DIFFERENT "
				"Scheduler headers than the Scheduler library it is linked to.\n"
				"  Fields that disagree (library vs this TU):\n");

			const struct { const char* name; uint32_t l, h; } fields[] = {
				{ "sizeof(EpochManager)",              lib.sizeEpochManager,   hdr.sizeEpochManager   },
				{ "Task layout (size/align/offsets)",  lib.taskLayout,         hdr.taskLayout         },
				{ "sizeof(TaskAllocator)",             lib.sizeTaskAllocator,  hdr.sizeTaskAllocator  },
				{ "sizeof(TaskDeque)",                 lib.sizeTaskDeque,      hdr.sizeTaskDeque      },
				{ "sizeof(TaskMPSCQueue)",             lib.sizeTaskMPSCQueue,  hdr.sizeTaskMPSCQueue  },
				{ "sizeof(WaitGroup)",                 lib.sizeWaitGroup,      hdr.sizeWaitGroup      },
				{ "sizeof(TaskScheduler)",             lib.sizeTaskScheduler,  hdr.sizeTaskScheduler  },
				{ "offsetof(TaskScheduler,abiCanary)", lib.offsetAbiCanary,    hdr.offsetAbiCanary    },
				{ "_ITERATOR_DEBUG_LEVEL",             lib.iteratorDebugLevel, hdr.iteratorDebugLevel },
			};
			bool differs = false;
			for (const auto& f : fields) {
				if (f.l == f.h) continue;
				differs = true;
				n += std::snprintf(msg + n, (n < (int)sizeof msg) ? sizeof msg - n : 0,
					"    %-36s %u vs %u\n", f.name, f.l, f.h);
			}
			if (!differs) return true;

			std::snprintf(msg + n, (n < (int)sizeof msg) ? sizeof msg - n : 0,
				"  Rebuild EVERY library that includes TaskScheduler.h, not just the Scheduler --\n"
				"  a stale Sound/Renderer/Physics library carries its own inlined copy of CreateTask\n"
				"  and reaches the wrong offset in a correctly-built scheduler object.\n"
				"  Note the Scheduler ships Debug, Development and Release; rebuilding only some of\n"
				"  them causes exactly this. _ITERATOR_DEBUG_LEVEL differing means /MDd vs /MD.\n"
				"  Continuing would fault at an unrelated address -- refusing instead.\n");

			std::fputs(msg, stderr);
			std::fflush(stderr);
			
#if defined(_WIN32)
			OutputDebugStringA(msg);
#endif
			std::abort();
			return false;
		}();
		}
	}
}
