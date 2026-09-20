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
        static void CoYield(Fiber* targetFiber, Pin pin = Pin::None);
        static void Suspend(Fiber* targetFiber, Pin pin = Pin::None);
        static void Resume(Fiber* targetFiber);
        static void CoYield(Pin pin = Pin::None);
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
    private:
        Fiber* AcquireFiber(Task* task);
        void ReleaseFiber(Fiber* f);

        void OnFiberReturned(Fiber* f, Task* task) noexcept;
        uint32_t FastRand();
        // Returns true if it left because `ctx` finished (main only), false on stop.
        bool Worker(WaitCtx* ctx = nullptr);

        TaskScheduler* scheduler;
        
        ThreadLocalCache<> localCache;                  
        ThreadLocalCache<> tinyCache;
        ThreadLocalCache<> deepCache;
        ThreadLocalCache<>& CacheFor(StackClass c) {
            switch (c) {
                case StackClass::Tiny: return tinyCache;
                case StackClass::Deep: return deepCache;
                default:               return localCache;
            }
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
};
