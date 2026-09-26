// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <optional>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <condition_variable>
#include <random>
#include <thread>
#include "Task.h"
#include "Fiber.h"
#include "Epochs.h"
#include "ThreadLocalCache.h"
#include "TsanFiber.h"   
#include "GlobalFiberPool.h"
#include "WaitGroup.h"
#include "Memory.h"
#include <cassert>
#include <cstddef>
#include <mimalloc.h>   // the real header: allocation here is plain mimalloc, not a wrapper
#include <new>
constexpr uint32_t FLAG_IO_READY = 0x1; // Bit 0 (1)
constexpr uint32_t FLAG_TIMEOUT = 0x2; // Bit 1 (2)
constexpr uint32_t FLAG_CANCELLED = 0x4; // Bit 2 (4)
constexpr uint32_t FLAG_RESUME = 0x8;
constexpr size_t kAlign = 64;

namespace JLib {
	class TaskScheduler;
    // One parking lot: 64 slots, sized and indexed by one uint64_t word.
    // Lots are block allocated and never move -- live TaskHandles point into them from any thread.
    struct ParkingLot {
        std::atomic<Task*>    parked[kAlign]{};   // shared: resumers claim out of here
        std::atomic<uint32_t> flags[kAlign]{};    // shared: any thread may set a wake bit
        std::atomic<uint32_t> gen[kAlign]{};      // shared: what makes a stale handle harmless

        // 1 = occupied; free slots are ~bits. Single writer (the owner allocates and frees), atomic
        // only so a thief may read it. The only slot index: a free list beside it could disagree.
        std::atomic<uint64_t> bits{ 0 };

        // Consent, not occupancy: one bit per slot whose task has been kicked.
        std::atomic<uint64_t> sig{ 0 };

        // Slots a thief must not touch: the task must come back on this worker. Masked out of the
        // thief's scan, so a pinned slot is visible only to its owner. Single writer -- the owner
        // sets it at park and the next park overwrites it.
        std::atomic<uint64_t> pinned{ 0 };

        // popcount(sig & ~pinned), maintained rather than recomputed: ready AND takeable by a
        // thief, so one load lets a thief skip a whole lot.
        std::atomic<int32_t> stealableSize{ 0 };

        // No deadline here. A lot holds addressability; timing is the observer's, and the one copy
        // lives on the fiber in Fiber::waitRecord.deadlineMs.
    };

    struct WaitHandle {
        Fiber* fiber;
        std::atomic<bool> signaled{ false };
    };

    namespace detail {
        // The raw TLS slot. Two callers only: SetCurrentFiber, which runs mid-switch where the stack
        // cannot be asked, and tests asserting about the slot itself. Everything else -- every
        // question of the form "which worker is this" -- uses TaskScheduler::SelfWorker(GetWorkers()),
        // which resolves through the task's home and validates it against the live workers array.
        Thread* TlsThreadRaw() noexcept;
    }

    class Thread {

        friend class TaskScheduler;
        friend Thread* detail::TlsThreadRaw() noexcept;

        // A worker opens another lot before the current one is full, so the allocation never
        // happens at the moment of need -- a park that had to wait on malloc would be a park that
        // can fail for a reason nothing else in the protocol can express.
        static constexpr int    kGrowWhenFreeBelow = 14;   // ~50 of 64 used
        // 255 x 64 = 16320 parks per worker: what the handle's `parked` byte can name. Only fibers
        // take slots, so the live ceiling is fibers checked out per worker -- park_test concentrates
        // 1200 on one worker and the fiber pool grows past 1900, so 1024 is not enough.
        static constexpr size_t kMaxLots = 255;

        // LOT 0 LIVES HERE, not on the heap. Every worker has one, so allocating it separately was
        // an allocation and a pointer chase that bought nothing; a worker that never parks pays
        // for it either way. Overflow lots still come from the allocator, one at a time on demand.
        // ~1 KB per worker.
        ParkingLot lot0;

        // Published with release, read with acquire: a resumer on another thread resolves
        // lots[handle.parked - 1] and must see a fully constructed lot.
        std::atomic<ParkingLot*> lots[kMaxLots]{};
        std::atomic<size_t>      lotCount{ 0 };

        // Owner only: how many slots are occupied across ALL lots. The hot path guard -- one load
        // and a branch on every hunt pass -- must not become a loop over lots.
        int parkedCount = 0;
        unsigned stealRotor = 0;   // rotation over dirty shards; see StealParked

    public:
        // ParkingLot::stealableSize summed over this thread's lots: a thief skips a whole worker on
        // one load. TaskScheduler::dirtyShards is the coarse filter above it.
        std::atomic<int32_t> stealableTotal{ 0 };

        // THE RESUME ARRAY: what this thread was TOLD is ready, as opposed to what it would have
        static constexpr size_t   kResumeChunk  = 1024;   // handles per chunk, 8 KB
        static constexpr size_t   kResumeChunks = 16;     // 16 * 1024 = 16384 > 16320 live parks
        static constexpr uint32_t kResumeSlotMask = (uint32_t)kResumeChunk - 1;
        static constexpr uint32_t kResumeChunkShift = 10;
        // A RING, so the write index wraps instead of running off the chunk table. It used to be a
        // plain counter reset only when a drain caught up EXACTLY -- every stalled drain skipped the
        // reset, so a worker that stalls (main, which drains only inside WaitFor) counted up until
        // `w >> kResumeChunkShift` left the table and PushResume aborted.
        static constexpr uint32_t kResumeCapacity = (uint32_t)(kResumeChunk * kResumeChunks);
        static constexpr uint32_t kResumeIndexMask = kResumeCapacity - 1;
        static_assert((kResumeCapacity & kResumeIndexMask) == 0, "capacity must be a power of two");

        std::atomic<std::atomic<TaskHandle>*> resumeChunks[kResumeChunks]{};
        std::atomic<uint32_t> resumeWrite{ 0 };
        std::atomic<uint32_t> resumeRead{ 0 };   // owner advances it

        // Hand a kicked task's key to its owner. Always succeeds; see the bound above.
        void PushResume(TaskHandle h) noexcept;

        // Resolve a chunk, publishing it if this is the first use. Any thread.
        std::atomic<TaskHandle>* ResumeChunk(size_t k) noexcept;
    private:

        // Owner only. Claims a slot, opening a lot if the ones it has are filling up. Returns the
        // 1-based lot number for the handle, or 0 if every lot is full and no more may be opened.
        size_t TakeSlot(ParkingLot*& outLot, int& outIndex) noexcept;
        bool   OpenLot() noexcept;
    public:
        std::atomic<uint64_t> occupied{ 0 };
        std::atomic<std::uint64_t> fiberAcquires{ 0 };
        std::atomic<std::uint64_t> fiberRecycles{ 0 };

        std::uint64_t FiberAcquireCount() const noexcept {
            return fiberAcquires.load(std::memory_order_relaxed);
        }
        std::uint64_t FiberRecycleCount() const noexcept {
            return fiberRecycles.load(std::memory_order_relaxed);
        }

		static void PushPathFieldOffsets(size_t& hasQueuedWorkOff,
		                                 size_t& workerStateOff) noexcept;

        Context schedulerCtx;

        void* tsanSchedulerFiber = nullptr;

        static void TsanSwitchToScheduler() noexcept {
            if (Thread* t = GetCurrent()) tsan::SwitchTo(t->tsanSchedulerFiber);
        }
        Fiber* currentFiber = nullptr;
        Task* currentRunningTask = nullptr;
        // This thread's own mimalloc heap and its thread-local part, created at thread start by
        // detail::EnsureThreadHeap. Neither may be cached across a suspension.
        mi_heap_t*  heap  = nullptr;
        mi_theap_t* theap = nullptr;

        // Plain mimalloc over this thread's theap -- passing the heap is what skips mimalloc's own
        // default-heap lookup, so nothing here asks which thread is running. Task code calls it as
        // task->record->home->Alloc(n), and `home` is written at every hand-over. Free with
        // mi_free(p) from any thread. Null theap means no pool: mi_malloc, the default heap.
        void* Alloc(std::size_t bytes) noexcept {
            return theap ? mi_theap_malloc(theap, bytes) : mi_malloc(bytes);
        }
        void* AllocAligned(std::size_t bytes, std::size_t alignment) noexcept {
            return theap ? mi_theap_malloc_aligned(theap, bytes, alignment)
                         : mi_malloc_aligned(bytes, alignment);
        }

        // WORKER-LOCAL STORAGE: one T per Thread, keyed by type -- no slot table to register with;
        template <class T> T& Local();
        template <class T> T* PeekLocal() const noexcept;
        static constexpr std::size_t kMaxLocalTypes = 32;

        int qIndex = 0;
        // Epoch slot, handed out by StartPool: 0 is always main; workers are 1..N (OutOfPool) or 1..N-1 (InPool).
        size_t epochId = 0;
        // MainMode::InPool: this Thread is slot 0 and is driven by the OS main thread. It never parks on
        // workerState; it blocks on mainWait (see KickWaitWord).
        bool isMain = false;
        // MainMode::OutOfPool: main's helper. Not a pool slot (qIndex -1, no queues, nothing is ever
        // pinned to it); it lets main run stolen tasks, fibers included, while it waits.
        bool isHelper = false;
        bool IsPoolWorker() const noexcept { return qIndex >= 0; }
        int  stealCursor = 0;   // helper's round-robin victim
        std::atomic<int> mainWait{ kWaitRunning };

        // What main is waiting for inside Worker(): a group reaching 0, or a predicate.
        struct WaitCtx {
            WaitGroup* wg = nullptr;
            bool (*pred)(void*) = nullptr;
            void* arg = nullptr;
            bool Done() const {
                if (wg && (wg->n.load(std::memory_order_acquire) & WaitGroup::COUNT_MASK) == 0) return true;
                return pred && pred(arg);
            }
        };

        // MainMode::InPool: run Worker() on main. With no ctx it returns once main has nothing to do; with a ctx
        // it returns when ctx is done or the pool stops. False if this is not main's slot.
        bool MainWorker(WaitCtx* ctx = nullptr);
        // Drop main's TLS binding before its Thread is destroyed.
        JLIB_NOINLINE void ReleaseCurrentThread() noexcept;

        int GetWorkerState() const noexcept { return workerState.load(std::memory_order_seq_cst); }

        std::thread& GetThread() { return thread; }

        void RequestStop() {
            running.store(false, std::memory_order_release);
            
            Wake();
        }

        void Wake() noexcept;

        std::atomic<bool> busy{ false };

        Thread(TaskScheduler& scheduler);
        Thread(const Thread& other) = delete;
        Thread& operator=(const Thread& other) = delete;
        ~Thread();

        uint64_t GetCurrentTimeMs();

        // Owner only: take a slot and publish the handle. timeoutMs 0 = no deadline.
        bool SuspendTask(Task* task, uint64_t timeoutMs = 0);

        // Owner only: deliver every parked task whose flag is set or whose deadline has passed,
        // and reclaim the slots of any a resumer already took. The one sanctioned sweep.

        // The owner's path: pop keys it was handed and claim them. O(1) each, no scan.
        Task* DrainResumes();

        // Sweep one worker's lots for kicked slots. victim == this is the owner sweep; any other
        // worker is a thief sweep. Returns one task to run, or null.
        Task* PollTasks(Thread* victim);

        // Hunt a dirty shard belonging to someone else. Null if nothing was worth taking.
        Task* StealParked();
        // Claim the task a handle names, or null if someone else won. Reclaims the slot either way.
        Task* ClaimByHandle(TaskHandle h);

        // Pin check for direct delivery: would ResumeFiber have left this task on this thread?
        bool CanRunHere(const Task* task) const;

        // Wake a parked task from any thread: the handle names worker, lot and slot, so it is one
        // bounds check and one fetch_or. By value, never a Task*, so a handle for a task that has
        // since finished fails the generation check instead of touching freed memory.
        // The only grantor of consent (ParkingLot::sig); any "signal this task" API calls it.
        static void Kick(TaskHandle h, uint32_t FLAG_RESUMED);

        // UNPINNED wake: consent only. The task stays in the lot for whoever is hunting; nobody is
        // named and nobody is woken while a hunter is already up.
        static void Signal(TaskHandle h, uint32_t FLAG_RESUMED);

        // Same consent, for a caller already holding the waiter (a primitive popping its own node).
        // Skips the validation a bare handle needs, including the generation compare.
        static bool SignalDirect(Task* t, uint32_t FLAG_RESUMED);

        // Hand the running task to worker N. A yield with a named placement: the task stays
        // runnable and lands in N's hi-pri inbox, which nobody may steal from.
        static bool SendTo(uint16_t worker);
        static bool SendToMain();

        // Same semantics per handle, with the not-per-slot work paid once per (worker, lot): one sig
        // RMW, one dirty-shard set, one MarkQueuedWork, one wake.
        static void KickBatch(const TaskHandle* handles, size_t n, uint32_t FLAG_RESUMED);

        // Is the park this handle names still live? Read-only, stops at the generation check. False
        // means someone already claimed the task, so a watcher can retire its watch.
        static bool ParkLive(TaskHandle h) noexcept;

        int allocate(Task* task);

        void addFlagBit(int index, uint32_t flagBit);

        // Owner only: slot back to its lot's free list.
        void release(ParkingLot* lot, int index);
        
        // Two capacities, because the two stack classes have separate pools and separate budgets:
        // sizing the deep cache from the standard pool's fair share gave every worker a 32-slot
        // cache in front of a pool holding one deep fiber per worker.
        void StartWorker(size_t cpu_affinity, size_t fiberCacheCapacity, size_t deepCacheCapacity);

		void AdoptCurrentThread(size_t fiberCacheCapacity, size_t deepCacheCapacity);
		// Main's helper (MainMode::OutOfPool): bind to the calling thread, and steal/run one task.
		void AdoptAsHelper();
		// HelpSteal is gone: main does not steal out of the pool (see OutOfPoolMainWait).
		void RunHelped(Task* t);
        std::thread::id GetID();

        int GetQueueLoad();
        void SetQueueIndex(size_t index);

        bool DrainOwnInboxesToDeques();

        void Join();
#undef Yield
        // Yield / Suspend / Resume are Fiber's: the fiber is the stack, so it moves with the task.
        // Reach it with FiberFromStack(), or keep the one you hold.

        // Park: false if no slot was free, in which case the caller must NOT switch out.
        bool StoreSuspended(Task* task, uint64_t timeoutMs = 0);
        // Unpark: true only for the one caller that took the task out of its slot.
        bool ResumeTask(Task* task);
        void NotifyWorker(bool force = false);

        void MarkQueuedWork() { hasQueuedWork.store(true, std::memory_order_seq_cst); }

        bool Parked() const { return workerState.load(std::memory_order_seq_cst) == WS_PARKED; }

        bool Ready();

        struct DebugState {
            int  qIndex;
            int  workerState;      
            bool hasQueuedWork;
            bool busy;
            bool running;
        };
        DebugState GetDebugState() const {
            return DebugState{
                qIndex,
                workerState.load(std::memory_order_relaxed),
                hasQueuedWork.load(std::memory_order_relaxed),
                busy.load(std::memory_order_relaxed),
                running.load(std::memory_order_relaxed)
            };
        }
        // Runs every Local<T>'s destructor and frees it. Join calls it for each Thread, before the
        // Thread's heap is released.
        void DestroyLocals() noexcept;

    private:
        // The TLS slot: the thread executing this stack. Library code reaches it through
        // detail::TlsThread(); everything else uses TaskScheduler::SelfWorker(GetWorkers()).
        JLIB_NOINLINE static Thread* GetCurrent();   // see JLIB_NOINLINE

        static constexpr int kSlots = 64;

        void*  localObjs_[kMaxLocalTypes] = {};
        void (*localDtors_[kMaxLocalTypes])(void*) = {};
        static std::size_t NextLocalTypeId() noexcept;
        template <class T> static std::size_t LocalTypeId() noexcept {
            static const std::size_t id = NextLocalTypeId();
            return id;
        }

        Fiber* AcquireFiber(Task* task);
        void ReleaseFiber(Fiber* f);

        void OnFiberReturned(Fiber* f, Task* task) noexcept;
        uint32_t FastRand();
        // Returns true if it left because `ctx` finished (main only), false on stop.
        bool Worker(WaitCtx* ctx = nullptr);

        TaskScheduler* scheduler;
        
        ThreadLocalCache<> localCache;
        ThreadLocalCache<> deepCache;
        ThreadLocalCache<>& CacheFor(StackClass c) {
            return c == StackClass::Deep ? deepCache : localCache;
        }
        static thread_local Thread* instance;       

        std::atomic<unsigned> parkCount{ 0 };

        std::atomic<bool> hasQueuedWork{ false };

        std::atomic<int> workerState{ 0  };

        // BlockBegin/End: blocked in code it cannot suspend, so the slot is busy until it returns.
        // Its queued work went to the deque (stealable) and unplaced pushes skip it; work that may
        // only run here waits. Next to workerState so a pusher reads one line.
        std::atomic<bool> away{ false };
        int blockDepth = 0;

        std::atomic<bool> idleLinked{ false };

        bool yieldedLastPass = false;


        enum WorkerState : int { WS_EMPTY = 0, WS_NOTIFIED = 1, WS_PARKED = 2 };
        
        std::atomic<bool> running{ false };
        std::atomic<bool> ready{ false };
        std::atomic<bool> joining{ false };

        Task* task = nullptr;
        std::thread thread;
        std::thread::native_handle_type nativeHandle;

    };

    inline std::atomic<size_t>* CurrentEpochSlot() {
        return EpochManager::Instance().ThreadSlot(CurrentThreadId());
    }

    [[noreturn]] void FatalNoEpochSlot();   // TaskScheduler.cpp

    inline std::atomic<size_t>* CurrentEpochSlotOrDie() {
        std::atomic<size_t>* s = CurrentEpochSlot();
        if (!s) FatalNoEpochSlot();
        return s;
    }

    class EpochGuard {
    public:

        EpochGuard() : slotted_(CurrentEpochSlotOrDie()) {}
        EpochGuard(const EpochGuard&) = delete;
        EpochGuard& operator=(const EpochGuard&) = delete;
    private:
        SlotEpochGuard slotted_;
    };

    template <class T> T& Thread::Local() {
        const std::size_t id = LocalTypeId<T>();
        assert(id < kMaxLocalTypes && "Thread::Local: more distinct types than kMaxLocalTypes");
        if (id >= kMaxLocalTypes) std::abort();   // release too: past here it writes out of bounds
        void*& slot = localObjs_[id];
        if (!slot) {
            // Only this thread builds its own copy: it allocates from this thread's theap.
            void* mem = AllocAligned(sizeof(T), alignof(T) < alignof(std::max_align_t) ? alignof(std::max_align_t) : alignof(T));
            slot = ::new (mem) T();
            localDtors_[id] = [](void* p) { static_cast<T*>(p)->~T(); mi_free(p); };
        }
        return *static_cast<T*>(slot);
    }

    template <class T> T* Thread::PeekLocal() const noexcept {
        const std::size_t id = LocalTypeId<T>();
        return id < kMaxLocalTypes ? static_cast<T*>(localObjs_[id]) : nullptr;
    }
};
