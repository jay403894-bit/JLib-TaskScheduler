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
    class Thread;
    
    struct WaitGroup;

    // What a worker is holding when it picks a task up. This is METADATA THE LOOP READS: the type
    enum class TaskType : uint8_t { Fiber, Coroutine, Native };

    // A fiber's stack size. The values index per-class arrays (GlobalFiberPool, Thread's caches),
    // so they must stay dense from 0.
    enum class StackClass : uint8_t { Standard = 0, Deep = 1 };

    // Which compute workers a push may pick, by core class. On a machine with one core class every
    // compute worker counts as P, and E falls back to P. K is never picked.
    enum class CorePref : uint8_t { P = 0, E = 1, Any = 2 };

    // Where a suspended task resumes. The suspend primitive writes it into TaskRecord::pinTo at the
    struct Pin {
        static constexpr uint16_t kNone    = 0xFFFF;
        static constexpr uint16_t kCurrent = 0xFFFE;
        static constexpr uint16_t kMain    = 0xFFFD;
        uint16_t target = kNone;

        // Binary: a suspension either keeps its thread or does not. Naming a DIFFERENT worker is
        // Thread::SendTo, an operation -- a pin is an argument to a suspend, so it cannot switch.
        static const Pin None;
        static const Pin Current;
        static const Pin Main;
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
    enum class MainMode : uint8_t { OutOfPool = 0, InPool = 1, Default = 2 };
    
#define JLIB_TASK_FLAG_FIELDS            \
        TaskType  type          : 2;     \
        uint8_t   priorityBoost : 1;     \
        uint8_t   trivialDtor   : 1;     \
        StackClass stackClass   : 2;

    // A task's durable identity: born with the task (CreateTask), dies with it (FreeTask).
    struct TaskDebt {
        TaskDebt* next = nullptr;
        void*      obj  = nullptr;
        void     (*release)(void*) noexcept = nullptr;
    };
    // Where a suspended task is parked: its slot in the owning Thread's SuspendTable. Written once
    struct TaskHandle {
        uint16_t worker_id  = 0;
        uint8_t  index      = 0;
        uint8_t  parked     = 0;   // 0 = not parked; nothing else in the word is meaningful
        uint32_t generation = 0;
    };
    static_assert(sizeof(TaskHandle) == 8, "TaskHandle must stay one word: TaskRecord is 64 bytes");
    struct Task;
    struct TaskRecord {
        static constexpr size_t   kLocalSlots = 8;
        static constexpr uint16_t kNoPin      = Pin::kNone;
        // Thread::SendTo's target, consumed by the next placement (TaskScheduler::YieldFiber).
        // kSendMain names main, which has no worker index when it is out of the pool.
        static constexpr uint16_t kNoSend     = 0xFFFFu;
        static constexpr uint16_t kSendMain   = 0xFFFEu;

        Fiber*      fiber       = nullptr;   // body; null until first run
        TaskDebt*  debts       = nullptr;   // released at task death, oldest last
        void**      locals      = nullptr;   // task-local slots, allocated on first use
        Task*       waitNext    = nullptr;   // link while suspended on a WaitGroup or an Event
        Task*       waitPrev    = nullptr;   // back link, Event only (its list is doubly linked)
        uint32_t    generation  = 0;         // bumped on reuse; lets stale references detect it
        uint16_t    pinTo       = kNoPin;    // worker a suspended task must resume on (see Pin)
        uint16_t    sendTo      = kNoSend;   // fits pinTo's padding; see kNoSend above
        // The Thread running this task NOW: written by the scheduler before every hand-over -- a
        Thread*     home        = nullptr;
        // The park slot, by value: nothing to allocate, and one atomic word so a resumer reading it
        // concurrently with a park gets one or the other, never a mix.
        std::atomic<TaskHandle> handle{ TaskHandle{} };
#if defined(JLIBSCHED_STATS)
        uint64_t    statFirstRun  = 0;       // clock ticks at first run
        uint64_t    statSuspendAt = 0;       // clock ticks at the last suspension, 0 while running
#endif
    };

    // The slab hands out 64-byte slots: a bigger record would write past its own.
    static_assert(sizeof(TaskRecord) <= 64, "TaskRecord must fit its 64-byte slab slot");
    static_assert(std::atomic<TaskHandle>::is_always_lock_free,
                  "the park handle must be one lock-free word: resumers read it concurrently");

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

        // Runs on the worker's own stack with no fiber, and must not suspend. Derived from the type
        // rather than stored: the deque's steal tag already encodes Native as its own code, so a
        // separate bit would be a second copy of the same fact with nothing keeping the two equal.
        bool native() const noexcept { return type == TaskType::Native; }

        Task()
            : fn(nullptr), data(nullptr), record(nullptr), next(nullptr),
              type(TaskType::Fiber),
              priorityBoost(0), trivialDtor(0),
              stackClass(StackClass::Standard) { ; }
        Task(Func f, void* d = nullptr)
            : fn(f), data(d), record(nullptr), next(nullptr),
               type(TaskType::Fiber),
              priorityBoost(0), trivialDtor(0),
              stackClass(StackClass::Standard) {
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
            
            probe([](TaskFlagPacking& f) { f.type          = TaskType::Native; });   // non-zero value
            probe([](TaskFlagPacking& f) { f.priorityBoost = 1; });
            probe([](TaskFlagPacking& f) { f.trivialDtor   = 1; });
            probe([](TaskFlagPacking& f) { f.stackClass    = StackClass::Deep; });            return h;
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

            this->type = TaskType::Native;
        }

        LambdaTask(const F& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(f)
        {
            this->data = this;
            this->type = TaskType::Native;
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
