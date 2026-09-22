// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <optional>
#include <atomic>
#include <chrono>
#include <cstdint>
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
#include <new>
struct mi_heap_s;    // mimalloc heap (Memory.h); only pointers live here
struct mi_theap_s;   // its thread-local part
namespace JLib {
	class TaskScheduler;

    struct WaitHandle {
        Fiber* fiber;
        std::atomic<bool> signaled{ false };
    };
    class Thread {
        
        friend class TaskScheduler;

    public:
        
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

        static Thread* Current() noexcept { return GetCurrent(); }   // TLS goes through GetCurrent

        Context schedulerCtx;

        void* tsanSchedulerFiber = nullptr;

        static void TsanSwitchToScheduler() noexcept {
            if (Thread* t = GetCurrent()) tsan::SwitchTo(t->tsanSchedulerFiber);
        }
        Fiber* currentFiber = nullptr;
        Task* currentRunningTask = nullptr;
        // This worker's own heap (Memory.cpp), created on its first allocation, and that heap's
        // thread-local part for the no-lookup path. Only ever read through GetCurrent() -- see
        // Memory.h for why neither may be cached across a suspension.
        mi_heap_s*  heap  = nullptr;
        mi_theap_s* theap = nullptr;
        // Allocate from THIS thread's heap (Memory.cpp). Task code reaches it as
        // task->record->home->Alloc(n) -- the record, not TLS, says which thread it is on now.
        // Only valid on this thread: home is written at every hand-over, so it always is (debug
        // builds assert it). Free with JLib::Free, from any thread.
        void* Alloc(std::size_t bytes) noexcept;
        void* AllocAligned(std::size_t bytes, std::size_t alignment) noexcept;

        // WORKER-LOCAL STORAGE: one T per Thread, keyed by type -- no slot table to register with;
        // the per-worker copies ARE the workers[] array (TaskScheduler::GetWorkers). Built the first
        // time THIS thread asks, in its own heap, so per-worker buckets need no locks: each worker
        // only ever writes its own.
        //   task->record->home->Local<Bucket>()   from task code (home is the running thread)
        //   for (Thread* w : TaskScheduler::GetWorkers()) w->PeekLocal<Bucket>()   collect, AFTER the join
        // Destroyed at Join, before the heap. T must be default-constructible.
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
        
        void StartWorker(size_t cpu_affinity, size_t fiberCacheCapacity);

		void AdoptCurrentThread(size_t fiberCacheCapacity);
		// Main's helper (MainMode::OutOfPool): bind to the calling thread, and steal/run one task.
		void AdoptAsHelper();
		// HelpSteal is gone: main does not steal out of the pool (see OutOfPoolMainWait).
		void RunHelped(Task* t);
        std::thread::id GetID();

        int GetQueueLoad();
        void SetQueueIndex(size_t index);

        bool DrainOwnInboxesToDeques();

        void Join();
        JLIB_NOINLINE static Thread* GetCurrent();   // the thread-state accessor; see JLIB_NOINLINE
#undef Yield
        static void Yield(Fiber* targetFiber, Pin pin = Pin::None);
        static void Suspend(Fiber* targetFiber, Pin pin = Pin::None);
        static void Resume(Fiber* targetFiber);
        static void Yield(Pin pin = Pin::None);
        static void Suspend(Pin pin = Pin::None);
        static void Resume();
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

        // TaskScheduler::BlockBegin/End: this thread is blocked in code it cannot suspend, so its
        // slot is busy until the call returns. Nobody stands in for it -- its queued work went onto
        // the deque before it left (stealable), and unplaced pushes skip an away thread. Work that
        // may only run here (pinned resumes, PushTo(worker), main-only) waits, by definition.
        // Next to workerState so a pusher reads it from a line it already touches.
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
        void*& slot = localObjs_[id];
        if (!slot) {
            // Only this thread builds its own copy (AllocAligned asserts this is the running thread).
            void* mem = AllocAligned(sizeof(T), alignof(T) < alignof(std::max_align_t) ? alignof(std::max_align_t) : alignof(T));
            slot = ::new (mem) T();
            localDtors_[id] = [](void* p) { static_cast<T*>(p)->~T(); JLib::Free(p); };
        }
        return *static_cast<T*>(slot);
    }

    template <class T> T* Thread::PeekLocal() const noexcept {
        const std::size_t id = LocalTypeId<T>();
        return id < kMaxLocalTypes ? static_cast<T*>(localObjs_[id]) : nullptr;
    }
};
