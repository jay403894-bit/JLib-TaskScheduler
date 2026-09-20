// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <functional>
#include <atomic>
#include <cassert>
#include "platform.h"

namespace JLib {
    struct Fiber;
    struct Task;
    struct DirectEvent;
    
    struct WaitGroup;

    // Fiber: runs on a fiber and may block. A native task (the `native` bit: every lambda task, and
    // fn+ctx tasks from CreateNativeTask) is called directly on the worker's stack instead and must
    // not suspend (fatal at every suspend point); it may block the OS thread only inside
    // TaskScheduler::BlockInPlace. For a lambda that is a lifetime proof: its closure lives in the
    // task's slab slot, which is freed as soon as the call returns.
    // Coroutine: resumed by a direct call on the worker's stack; waits by co_await
    // (JLIBSCHED_COROUTINES, Coroutine.h).
    // Main: must run on the OS main thread. Set by PushMain; every resume/yield/requeue sends it
    // back through PushMainQueue -- slot 0's hi-pri inbox with main in the pool (on a fiber), mainQ
    // with main out of it (run directly).
    enum class TaskType : uint8_t { Fiber, Coroutine, Main };

    enum class StackClass : uint8_t { Standard = 0, Tiny = 1, Deep = 2 };

    enum class Lane : uint8_t { Normal = 0, LowLatency = 1 };

    // Which compute workers a push may pick, by core class. On a machine with one core class every
    // compute worker counts as P, and E falls back to P. K is never picked.
    enum class CorePref : uint8_t { P = 0, E = 1, Any = 2 };

    // Where a suspended task resumes. The suspend primitive writes it into TaskRecord::pinTo at the
    // moment of suspension; the resume path reads it once. Nothing else looks at it.
    //   Pin::None       resume anywhere: the resumer's own deque (stealable)
    //   Pin::Current    the thread that suspended: its hi-pri inbox (never stolen)
    //   Pin::Thread(n)  worker n: its hi-pri inbox (never stolen)
    //   Pin::Main       the main thread, in EITHER main mode -- "finish this part on main"
    // Mode::Pinned makes every suspension Current, whatever is passed.
    //
    // Pin::Main is its own sentinel because Pin::Thread(0) is main only when main is IN the pool;
    // out of the pool main is not a worker slot and no index names it. The sentinel carries the
    // meaning in both modes: main in the pool resumes through slot 0's hi-pri inbox, main out of
    // it through its own work queue, which ProcessMainThread drains.
    struct Pin {
        static constexpr uint16_t kNone    = 0xFFFF;
        static constexpr uint16_t kCurrent = 0xFFFE;
        static constexpr uint16_t kMain    = 0xFFFD;
        uint16_t target = kNone;

        static const Pin None;
        static const Pin Current;
        static const Pin Main;
        static constexpr Pin Thread(uint16_t worker) noexcept { return Pin{ worker }; }
        constexpr bool operator==(Pin o) const noexcept { return target == o.target; }
        constexpr bool operator!=(Pin o) const noexcept { return target != o.target; }
    };
    inline constexpr Pin Pin::None{ Pin::kNone };
    inline constexpr Pin Pin::Current{ Pin::kCurrent };
    inline constexpr Pin Pin::Main{ Pin::kMain };

    // Resolves `pin` for the calling thread and writes it into t->record->pinTo. Called only by
    // the suspend primitives (Fiber::BeginSuspend / BeginYield, the coroutine ArmResume).
    JLIB_NOINLINE void StampPin(Task* t, Pin pin) noexcept;   // reads the current thread

    // Fiber affinity, chosen at Init.
    //   Migrate: each suspension uses the Pin its caller passes (default Pin::None).
    //   Pinned:  every suspension is Pin::Current, whatever the caller passes.
    enum class Mode : uint8_t { Migrate = 0, Pinned = 1 };

    // Is main a worker, chosen at Init.
    //   OutOfPool: main is not a pool slot; it drains mainQ via ProcessMainThread().
    //   InPool:    main is slot 0, runs Worker() itself inside its waits, and blocks on its own
    //              wait word, never on workerState.
    //   Default:   derived from Mode -- Migrate -> InPool, Pinned -> OutOfPool.
    enum class MainMode : uint8_t { OutOfPool = 0, InPool = 1, Default = 2 };

    constexpr bool IsLowLatency(Lane l) noexcept { return l == Lane::LowLatency; }
    constexpr bool IsNormalLane(Lane l) noexcept { return l == Lane::Normal; }
    
#define JLIB_TASK_FLAG_FIELDS            \
        Lane      lane          : 1;     \
        TaskType  type          : 2;     \
        uint8_t   priorityBoost : 1;     \
        uint8_t   trivialDtor   : 1;     \
        StackClass stackClass   : 2;     \
        uint8_t   native    : 1;

    // A task's durable identity: born with the task (CreateTask), dies with it (FreeTask).
    // Holds scheduling state that follows the task; the body (a fiber today) is attached when the
    // task first runs. Allocated from the 64-byte slab class and must stay within one cache line.
    struct FiberDebt;
    struct Task;
    struct TaskRecord {
        static constexpr size_t   kLocalSlots = 8;
        static constexpr uint16_t kNoPin      = Pin::kNone;

        Fiber*      fiber       = nullptr;   // body; null until first run
        FiberDebt*  debts       = nullptr;   // cleanup owed at death; each names its holder
        void**      locals      = nullptr;   // task-local slots, allocated on first use
        Task*       waitNext    = nullptr;   // link while the task is suspended on a WaitGroup
        uint32_t    generation  = 0;         // bumped on reuse; lets stale references detect it
        uint16_t    pinTo       = kNoPin;    // worker a suspended task must resume on (see Pin)
#if defined(JLIBSCHED_STATS)
        uint64_t    statFirstRun  = 0;       // clock ticks at first run
        uint64_t    statSuspendAt = 0;       // clock ticks at the last suspension, 0 while running
#endif
    };

    struct alignas(16) Task {
        using Func = void(*)(void*);

        Func fn;
        void* data = nullptr;
        TaskRecord* record = nullptr;   // null only for queue stubs, which never run
        std::atomic<Task*> next{ nullptr };
        WaitGroup* waitGroup = nullptr;
        
        JLIB_TASK_FLAG_FIELDS
        
        uint8_t started = 0;

        uint8_t cancelledDirect = 0;

        uint32_t cancelToken = 0xFFFFFFFFu;   

        Task()
            : fn(nullptr), data(nullptr), record(nullptr), next(nullptr),
              lane(Lane::Normal), type(TaskType::Fiber),
              priorityBoost(0), trivialDtor(0),
              stackClass(StackClass::Standard), native(0) { ; }
        Task(Func f, void* d = nullptr, Lane ln = Lane::Normal)
            : fn(f), data(d), record(nullptr), next(nullptr),
              lane(ln), type(TaskType::Fiber),
              priorityBoost(0), trivialDtor(0),
              stackClass(StackClass::Standard), native(0) {
        }
        virtual ~Task() {

        }

        void* operator new(std::size_t) = delete;
        void* operator new[](std::size_t) = delete;
        void operator delete(void*) noexcept {
            assert(false && "Task is slab-allocated by TaskAllocator; never delete one");
        }
        void operator delete[](void*) = delete;

        inline void Execute() noexcept {
            fn(data);
        }
    };

    inline void DestroyTask(Task* t) noexcept {
        if (!t->trivialDtor) t->~Task();
    }
    
    static_assert(sizeof(Task) == 64, "Task must stay exactly one 64-byte cache line");

    namespace detail {
        
        struct TaskFlagPacking { JLIB_TASK_FLAG_FIELDS };
        
        static_assert(sizeof(TaskFlagPacking) == 1,
                      "Task's six flags must pack into ONE byte -- see the flag block in Task");

        inline uint32_t TaskFlagBitLayout() {
            uint32_t h = 2166136261u;                      
            auto mixByte = [&h](unsigned char b) { h ^= b; h *= 16777619u; };

            TaskFlagPacking p{};
            auto probe = [&](auto setter) {
                unsigned char* raw = reinterpret_cast<unsigned char*>(&p);
                for (size_t i = 0; i < sizeof p; ++i) raw[i] = 0;
                setter(p);
                for (size_t i = 0; i < sizeof p; ++i) mixByte(raw[i]);
            };

            probe([](TaskFlagPacking& f) { f.lane          = Lane::LowLatency; });
            
            probe([](TaskFlagPacking& f) { f.type          = TaskType::Main; });   // non-zero value
            probe([](TaskFlagPacking& f) { f.priorityBoost = 1; });
            probe([](TaskFlagPacking& f) { f.trivialDtor   = 1; });
            probe([](TaskFlagPacking& f) { f.stackClass    = StackClass::Deep; });
            return h;
        }
    }

    template<typename F>
    class alignas(16) LambdaTask : public Task {
        F func;
    public:
        
        static_assert(alignof(F) <= 16, "LambdaTask capture is over-aligned for a slab slot");
        
        LambdaTask(F&& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(std::move(f))
        {
            this->data = this;
            
            this->native = 1;
        }

        LambdaTask(const F& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(f)
        {
            this->data = this;
            this->native = 1;
        }
		~LambdaTask() {
		}
        
        void* operator new(std::size_t) = delete;
        void* operator new[](std::size_t) = delete;
        void operator delete(void*) noexcept {
            assert(false && "LambdaTask is slab-allocated by TaskAllocator; never delete one");
        }
        void operator delete[](void*) = delete;

    private:
        static void ExecuteWrapper(void* ptr) {
            LambdaTask* self = static_cast<LambdaTask*>(ptr);

            self->func();
        }
    };
 
};
