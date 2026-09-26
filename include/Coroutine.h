// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

// Optional C++20 coroutine layer. Build the library with -DJLIBSCHED_COROUTINES=ON.
// A coroutine task runs directly on a worker's stack and waits only by co_await; its TaskRecord
// holds the pin (record->pinTo, written by ArmResume at each suspension), and every wake goes through
// TaskScheduler::WakeTask so Pin mode resumes it on its home worker.
#if !defined(JLIBSCHED_COROUTINES)
    #error "JLib/Coroutine.h: the library was built without JLIBSCHED_COROUTINES"
#endif

#if !defined(__cpp_impl_coroutine) || __cpp_impl_coroutine < 201902L
    
    #if !defined(_MSVC_LANG) || _MSVC_LANG < 202002L
        #if __cplusplus < 202002L
            #error "JLib/Coroutine.h requires C++20 (MSVC: /std:c++20, GCC/Clang: -std=c++20)"
        #endif
    #endif
    #error "JLib/Coroutine.h requires C++20 coroutines, but __cpp_impl_coroutine is not defined"
#endif

#include "TaskScheduler.h"
#include "Hazard.h"      
#include "Future.h"      

#include <coroutine>
#include <cstdint>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace JLib {

    struct TaskNode;
    void SignalExternalNode(TaskNode* node);

    namespace detail {
        // Frames come from the task slab (FrameAlloc) or the heap; the old fixed-slot frame pool
        // that used to sit here is gone -- nothing called it.
        inline std::atomic<bool>& FramePoolEnabled() { static std::atomic<bool> b{ true }; return b; }
        
        inline std::atomic<bool>& SmallFrameClassEnabled() { static std::atomic<bool> b{ true }; return b; }
    }
    
    inline void SetCoroFramePooling(bool on) {
        detail::FramePoolEnabled().store(on, std::memory_order_relaxed);
    }
    
    inline void SetCoroSmallFrameClass(bool on) {
        detail::SmallFrameClassEnabled().store(on, std::memory_order_relaxed);
    }
    inline bool CoroSmallFrameClass() {
        return detail::SmallFrameClassEnabled().load(std::memory_order_relaxed);
    }

    inline bool CoroFramePooling() {
        return detail::FramePoolEnabled().load(std::memory_order_relaxed);
    }

    namespace detail {

        // Every frame carries where it came from, so it goes back there -- not to whatever pool
        // happens to be running when the frame dies. poolGen 0 = the heap. 16 bytes keeps the
        // frame at the alignment operator new promises.
        struct alignas(16) FrameHeader { std::uint64_t poolGen; std::uint64_t unused; };
        inline constexpr std::size_t kFrameHeader = sizeof(FrameHeader);
        static_assert(kFrameHeader == 16, "the frame header must keep 16-byte alignment");

        inline void* FrameAlloc(std::size_t n) {
            JLIB_STAT(CoroFrames);
            JLIB_STAT_HIST(CoroFrameBytes, n);
            const std::size_t total = n + kFrameHeader;
            void*         base = nullptr;
            std::uint64_t gen  = 0;
            if (FramePoolEnabled().load(std::memory_order_relaxed)
                && TaskScheduler::IsInitialized()) {
                auto* a = TaskScheduler::Instance().GetAllocator();

                const std::size_t want =
                    SmallFrameClassEnabled().load(std::memory_order_relaxed)
                        ? total
                        : (total <= TaskAllocator::SLOT ? TaskAllocator::SLOT : total);
                base = a->AllocSized(want);
                if (base) gen = TaskScheduler::PoolGeneration();
            }
            if (!base) {
                JLIB_STAT(CoroFramesHeap);
                base = ::operator new(total);
            }
            ::new (base) FrameHeader{ gen, 0 };
            return static_cast<std::byte*>(base) + kFrameHeader;
        }

        inline void FrameFree(void* p) noexcept {
            if (!p) return;
            void* base = static_cast<std::byte*>(p) - kFrameHeader;
            const std::uint64_t gen = static_cast<FrameHeader*>(base)->poolGen;
            if (gen == 0) { ::operator delete(base); return; }
            // A slab frame goes back only to the pool that made it. That pool gone (a frame freed
            // at exit, after the pool shut down) means its memory went with it: nothing to free,
            // and above all no ::operator delete on a slab slot.
            if (TaskScheduler::IsInitialized() && TaskScheduler::PoolGeneration() == gen)
                TaskScheduler::Instance().GetAllocator()->FreeSized(base);
        }
    }

#define JLIB_CORO_FRAME_ALLOC                                                                   \
        static void* operator new(std::size_t n) { return ::JLib::detail::FrameAlloc(n); }      \
        static void operator delete(void* p) noexcept { ::JLib::detail::FrameFree(p); }         \
        static void operator delete(void* p, std::size_t) noexcept { ::JLib::detail::FrameFree(p); }


    namespace detail {
        
        inline void ResumeCoroutine(void* p) {
            std::coroutine_handle<>::from_address(p).resume();
        }

        // The coroutine suspend primitive: writes the pin, then arms the task to resume this frame.
        // Every awaiter calls it before registering the task.
        template <typename P>
        inline Task* ArmResume(std::coroutine_handle<P> h, Pin pin) noexcept {

            JLIB_EPOCH_CHECK_NO_GUARD_CORO();

            if (const std::size_t d = HazardDomain::SuspendUnsafeDepth())
                HazardDomain::FatalSuspendWithGuard(d);

            Task* t = h.promise().task;
            StampPin(t, pin);
            t->data = h.address();
            return t;
        }

        inline bool CurrentCoroTaskCancelled() noexcept {
            if (!TaskScheduler::IsInitialized()) return false;
            return IsTaskCancelled(TaskScheduler::Instance().GetCurrentTask());
        }
    }

    class Coro {
    public:
        struct promise_type;
        using Handle = std::coroutine_handle<promise_type>;

        struct promise_type {
            
            JLIB_CORO_FRAME_ALLOC

            Task* task = nullptr;

            TaskNode* dagNode = nullptr;

            Coro get_return_object() noexcept { return Coro{ Handle::from_promise(*this) }; }

            std::suspend_always initial_suspend() noexcept { return {}; }

            std::suspend_never final_suspend() noexcept { Complete(); return {}; }

            void return_void() noexcept {}

            void unhandled_exception() { throw; }

            void Complete() noexcept {
                Task* t = task;
                if (!t) return;          
                task = nullptr;

                if (t->waitGroup) {
                    t->waitGroup->Done();
                }
                JLIB_STAT_ONLY(if (t->record && t->record->statFirstRun)
                    JLIB_STAT_HIST(TaskLife, JLIB_STAT_TICKS() - t->record->statFirstRun);)

                // The promise owns the task once it runs: the worker never touches it after
                // resume() returns, so completion is the one place it dies.
                TaskScheduler::Instance().FreeTask(t);

                if (TaskNode* n = dagNode) {
                    dagNode = nullptr;                 
                    SignalExternalNode(n);
                }
            }
        };

        Coro() noexcept = default;
        explicit Coro(Handle h) noexcept : h_(h) {}

        Coro(Coro&& other) noexcept : h_(std::exchange(other.h_, {})) {}
        Coro& operator=(Coro&& other) noexcept {
            if (this != &other) {
                if (h_) h_.destroy();
                h_ = std::exchange(other.h_, {});
            }
            return *this;
        }
        Coro(const Coro&) = delete;
        Coro& operator=(const Coro&) = delete;

        ~Coro() { if (h_) h_.destroy(); }

        Handle Release() noexcept { return std::exchange(h_, {}); }
        explicit operator bool() const noexcept { return static_cast<bool>(h_); }

    private:
        Handle h_{};
    };

    inline bool Spawn(Coro&& c, WaitGroup* wg = nullptr,
                      uint32_t cancelToken = CancelToken::kNone) {
        Coro::Handle h = c.Release();
        if (!h) return false;

        auto& sched = TaskScheduler::Instance();
        Task* t = sched.CreateTask(&detail::ResumeCoroutine,
                                   h.address(), TaskType::Coroutine);
        if (!t) { h.destroy(); return false; }

        h.promise().task = t;
        t->cancelToken = cancelToken;
        if (wg) {
            wg->n.fetch_add(1, std::memory_order_relaxed);
            t->waitGroup = wg;
        }

        return sched.Push(t);
    }

    inline bool Spawn(Coro&& c, TaskNode* node,
                      uint32_t cancelToken = CancelToken::kNone) {
        Coro::Handle h = c.Release();
        if (!h) return false;

        auto& sched = TaskScheduler::Instance();
        Task* t = sched.CreateTask(&detail::ResumeCoroutine,
                                   h.address(), TaskType::Coroutine);
        if (!t) { h.destroy(); return false; }

        h.promise().task    = t;
        t->cancelToken = cancelToken;
        h.promise().dagNode = node;

        return sched.Push(t);
    }

    // Every awaiter carries the Pin for its suspension (default Pin::None) and hands it to
    // ArmResume, the coroutine suspend primitive.

    // co_await Reschedule{} / Reschedule{ Pin::Current }: the coroutine form of a fiber yield.
    struct Reschedule {
        Pin pin = Pin::None;

        bool await_ready() const noexcept { return false; }

        template <typename P>
        void await_suspend(std::coroutine_handle<P> h) const noexcept {
            JLIB_STAT(Yields);
            Task* t = detail::ArmResume(h, pin);
            TaskScheduler::Instance().YieldFiber(t);
        }

        void await_resume() const noexcept {}
    };

    // co_await SendTo{ n }: the coroutine form of Thread::SendTo. Same mechanism -- the target
    // rides on the record and YieldFiber places it -- with the suspension the plain call cannot do.
    // TaskRecord::kSendMain names main.
    struct SendTo {
        uint16_t worker = TaskRecord::kSendMain;

        bool await_ready() const noexcept { return false; }

        template <typename P>
        void await_suspend(std::coroutine_handle<P> h) const noexcept {
            JLIB_STAT(Yields);
            Task* t = detail::ArmResume(h, Pin::None);
            if (t->record) t->record->sendTo = worker;
            TaskScheduler::Instance().YieldFiber(t);
        }

        void await_resume() const noexcept {}
    };

    // co_await WaitAsync(wg): the coroutine form of TaskScheduler::WaitFor. The task joins the
    // group's waiter list (the same one a fiber joins) and the worker returns to its loop.
    class WaitGroupAwaiter {
    public:
        WaitGroupAwaiter(WaitGroup& wg, Pin pin) noexcept : wg_(wg), pin_(pin) {}

        bool await_ready() const noexcept {
            if ((wg_.n.load(std::memory_order_acquire) & WaitGroup::COUNT_MASK) != 0) return false;
            wg_.Settle();   // not suspending: let any wake in progress finish before the group can go away
            return true;
        }

        template <typename P>
        void await_suspend(std::coroutine_handle<P> h) const {
            wg_.AddWaiter(detail::ArmResume(h, pin_));   // may resume elsewhere before this returns
        }

        void await_resume() const noexcept {}

    private:
        WaitGroup& wg_;
        Pin        pin_;
    };

    inline WaitGroupAwaiter WaitAsync(WaitGroup& wg, Pin pin = Pin::None) noexcept {
        return WaitGroupAwaiter{ wg, pin };
    }

    // co_await WaitEventAsync(ev): the coroutine form of TaskScheduler::WaitOnEvent. Returns
    // WaitResult::Cancelled if the task was cancelled before linking or woken by CancelWaiters.
    class EventAwaiter {
    public:
        EventAwaiter(Event& ev, Pin pin) noexcept : ev_(ev), pin_(pin) {}

        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            task_ = detail::ArmResume(h, pin_);
            if (IsTaskCancelled(task_)) return false;   // fast path; AddWaiter re-checks under the lock
            return ev_.AddWaiter(task_);                // true: may resume elsewhere before this returns
        }

        WaitResult await_resume() const noexcept {
            return IsTaskCancelled(task_) ? WaitResult::Cancelled : WaitResult::Ok;
        }

    private:
        Event& ev_;
        Pin    pin_;
        Task*  task_ = nullptr;
    };

    inline EventAwaiter WaitEventAsync(Event& ev, Pin pin = Pin::None) noexcept {
        return EventAwaiter{ ev, pin };
    }

    // co_await WaitDirectAsync(arm): the coroutine form of TaskScheduler::WaitOnEventDirectArmed.
    // The DirectEvent lives in the awaiter, which the coroutine frame keeps alive while suspended;
    // arm(ev) hands it to whoever will call ev->Signal().
    template <typename Arm>
    class DirectEventAwaiter {
    public:
        DirectEventAwaiter(Arm arm, Pin pin) : arm_(std::move(arm)), pin_(pin) {}

        bool await_ready() const noexcept { return false; }

        template <typename P>
        void await_suspend(std::coroutine_handle<P> h) {
            ev_.waiter.store(detail::ArmResume(h, pin_), std::memory_order_release);
            Arm arm = std::move(arm_);   // a signal inside arm may resume and destroy this awaiter
            arm(&ev_);
        }

        void await_resume() const noexcept {}

    private:
        DirectEvent ev_;
        Arm         arm_;
        Pin         pin_;
    };

    template <typename Arm>
    inline DirectEventAwaiter<std::decay_t<Arm>> WaitDirectAsync(Arm&& arm, Pin pin = Pin::None) {
        return DirectEventAwaiter<std::decay_t<Arm>>{ std::forward<Arm>(arm), pin };
    }

    class LockAwaiter {
    public:
        LockAwaiter(SchedulerMutex& m, Pin pin) noexcept : m_(m), pin_(pin) {}

        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            Task* t = detail::ArmResume(h, pin_);
            return !m_.LockAsyncEnqueue(t, &node_);
        }

        void await_resume() const noexcept {}

    private:
        SchedulerMutex& m_;
        Pin             pin_;
        // The awaiter is a temporary of the co_await expression, so it lives in the coroutine frame
        // until the wait completes: the queue can hold a pointer into it.
        WaitNode        node_;
    };

    class AcquireAwaiter {
    public:
        AcquireAwaiter(SchedulerSemaphore& s, Pin pin) noexcept : s_(s), pin_(pin) {}
        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            Task* t = detail::ArmResume(h, pin_);
            return !s_.WaitAsyncEnqueue(t, &node_);
        }

        void await_resume() const noexcept {}

    private:
        SchedulerSemaphore& s_;
        Pin                 pin_;
        WaitNode            node_;   // lives in the coroutine frame for the whole wait
    };

    class LockAwaiterCancellable {
    public:
        LockAwaiterCancellable(SchedulerMutex& m, Pin pin) noexcept : m_(m), pin_(pin) {}
        LockAwaiterCancellable(const LockAwaiterCancellable&) = delete;
        LockAwaiterCancellable& operator=(const LockAwaiterCancellable&) = delete;

        bool await_ready() noexcept {
            if (detail::CurrentCoroTaskCancelled()) {
                result_ = WaitResult::Cancelled;
                return true;
            }
            return false;
        }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            Task* t = detail::ArmResume(h, pin_);
            return !m_.LockAsyncEnqueue(t, &node_, &result_);
        }

        [[nodiscard]] WaitResult await_resume() const noexcept { return result_; }

    private:
        SchedulerMutex& m_;
        Pin             pin_;
        WaitResult      result_ = WaitResult::Ok;
        WaitNode        node_;   // lives in the coroutine frame for the whole wait
    };

    class AcquireAwaiterCancellable {
    public:
        AcquireAwaiterCancellable(SchedulerSemaphore& s, Pin pin) noexcept : s_(s), pin_(pin) {}
        AcquireAwaiterCancellable(const AcquireAwaiterCancellable&) = delete;
        AcquireAwaiterCancellable& operator=(const AcquireAwaiterCancellable&) = delete;

        bool await_ready() noexcept {
            if (detail::CurrentCoroTaskCancelled()) {
                result_ = WaitResult::Cancelled;
                return true;
            }
            return false;
        }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            Task* t = detail::ArmResume(h, pin_);
            return !s_.WaitAsyncEnqueue(t, &node_, &result_);
        }

        [[nodiscard]] WaitResult await_resume() const noexcept { return result_; }

    private:
        SchedulerSemaphore& s_;
        Pin                 pin_;
        WaitResult          result_ = WaitResult::Ok;
        WaitNode            node_;   // lives in the coroutine frame for the whole wait
    };

    inline LockAwaiter LockAsync(SchedulerMutex& m, Pin pin = Pin::None) noexcept {
        return LockAwaiter{ m, pin };
    }
    inline AcquireAwaiter AcquireAsync(SchedulerSemaphore& s, Pin pin = Pin::None) noexcept {
        return AcquireAwaiter{ s, pin };
    }

    inline LockAwaiterCancellable LockAsyncCancellable(SchedulerMutex& m, Pin pin = Pin::None) noexcept {
        return LockAwaiterCancellable{ m, pin };
    }

    inline AcquireAwaiterCancellable AcquireAsyncCancellable(SchedulerSemaphore& s, Pin pin = Pin::None) noexcept {
        return AcquireAwaiterCancellable{ s, pin };
    }

    namespace detail {
        struct LazyPromiseBase {
            JLIB_CORO_FRAME_ALLOC

            std::coroutine_handle<> continuation{};
            std::exception_ptr eptr;

            Task* task = nullptr;

            std::suspend_always initial_suspend() noexcept { return {}; }

            struct FinalAwaiter {
                bool await_ready() const noexcept { return false; }
                template <typename P>
                std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
                    std::coroutine_handle<> c = h.promise().continuation;
                    
                    return c ? c : std::noop_coroutine();
                }
                void await_resume() const noexcept {}
            };
            FinalAwaiter final_suspend() noexcept { return {}; }

            void unhandled_exception() noexcept { eptr = std::current_exception(); }
        };

        template <typename T>
        struct LazyValue : LazyPromiseBase {
            std::optional<T> value;
            template <typename U = T>
            void return_value(U&& v) { value.emplace(std::forward<U>(v)); }
            T&& Take() {
                if (eptr) std::rethrow_exception(eptr);
                return std::move(*value);
            }
        };

        template <>
        struct LazyValue<void> : LazyPromiseBase {
            void return_void() noexcept {}
            void Take() { if (eptr) std::rethrow_exception(eptr); }
        };
    }

    template <typename T = void>
    class Lazy {
    public:
        struct promise_type : detail::LazyValue<T> {
            Lazy get_return_object() noexcept {
                return Lazy{ std::coroutine_handle<promise_type>::from_promise(*this) };
            }
        };
        using Handle = std::coroutine_handle<promise_type>;

        Lazy() noexcept = default;
        explicit Lazy(Handle h) noexcept : h_(h) {}
        Lazy(Lazy&& o) noexcept : h_(std::exchange(o.h_, {})) {}
        Lazy& operator=(Lazy&& o) noexcept {
            if (this != &o) { if (h_) h_.destroy(); h_ = std::exchange(o.h_, {}); }
            return *this;
        }
        Lazy(const Lazy&) = delete;
        Lazy& operator=(const Lazy&) = delete;

        ~Lazy() { if (h_) h_.destroy(); }

        struct Awaiter {
            Handle h;
            
            bool await_ready() const noexcept { return !h || h.done(); }

            template <typename P>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<P> outer) noexcept {
                h.promise().continuation = outer;
                h.promise().task = outer.promise().task;   
                
                return h;
            }

            decltype(auto) await_resume() { return h.promise().Take(); }
        };

        Awaiter operator co_await() && noexcept { return Awaiter{ h_ }; }

        bool Done() const noexcept { return !h_ || h_.done(); }
        explicit operator bool() const noexcept { return static_cast<bool>(h_); }

    private:
        Handle h_{};
    };

    namespace detail {
        
        template <typename T>
        Coro SyncWaitRunner(Lazy<T>* lazy, std::optional<T>* out) {
            out->emplace(co_await std::move(*lazy));
        }
        inline Coro SyncWaitRunnerVoid(Lazy<void>* lazy) {
            co_await std::move(*lazy);
        }
    }

    template <typename T>
    T SyncWait(Lazy<T> lazy) {
        std::optional<T> out;
        WaitGroup wg;
        Spawn(detail::SyncWaitRunner<T>(&lazy, &out), &wg);
        TaskScheduler::Instance().WaitFor(wg);
        return std::move(*out);
    }

    inline void SyncWait(Lazy<void> lazy) {
        WaitGroup wg;
        Spawn(detail::SyncWaitRunnerVoid(&lazy), &wg);
        TaskScheduler::Instance().WaitFor(wg);
    }

    template <class T>
    struct FutureResult {
        FutureStatus status = FutureStatus::Broken;
        const T*     value  = nullptr;      

        [[nodiscard]] bool Ok() const noexcept { return status == FutureStatus::Ready; }
        const T& operator*() const noexcept { return *value; }
    };

    template <>
    struct FutureResult<void> {
        FutureStatus status = FutureStatus::Broken;
        [[nodiscard]] bool Ok() const noexcept { return status == FutureStatus::Ready; }
    };

    template <class T>
    class FutureAwaiter {
    public:
        FutureAwaiter(const Future<T>& f, CancelToken token, Pin pin = Pin::None) noexcept
            : f_(f), token_(token), pin_(pin) {}
        FutureAwaiter(const FutureAwaiter&) = delete;
        FutureAwaiter& operator=(const FutureAwaiter&) = delete;

        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            auto* s = f_.State_();
            if (!s) { w_.status = FutureStatus::Broken; return false; }
            Task* t = detail::ArmResume(h, pin_);
            return !s->ReadyOrQueue(&w_, t, token_);
        }

        [[nodiscard]] FutureResult<T> await_resume() const noexcept {
            FutureResult<T> r;
            r.status = w_.status;
            if constexpr (!std::is_void_v<T>) {
                if (r.status == FutureStatus::Ready && f_.State_()) r.value = f_.State_()->Value();
            }
            return r;
        }

    private:
        const Future<T>&     f_;
        CancelToken          token_;
        Pin                  pin_;
        detail::FutureWaiter w_{};
    };

    template <class T>
    class FutureRefAwaiter {
    public:
        explicit FutureRefAwaiter(const Future<T>& f) noexcept : inner_(f, CancelToken{}) {}

        bool await_ready() const noexcept { return inner_.await_ready(); }
        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) { return inner_.await_suspend(h); }

        [[nodiscard]] decltype(auto) await_resume() const noexcept {
            const FutureResult<T> r = inner_.await_resume();
            assert(r.Ok() && "co_await on a Future whose Promise was destroyed unset -- "
                             "use WaitFuture(fut, token) if the producer may legitimately go away");
            if constexpr (!std::is_void_v<T>) return (*r.value);
        }

    private:
        FutureAwaiter<T> inner_;
    };

    template <class T>
    [[nodiscard]] inline FutureRefAwaiter<T> operator co_await(const Future<T>& f) noexcept {
        return FutureRefAwaiter<T>(f);
    }

    template <class T>
    [[nodiscard]] inline FutureAwaiter<T> WaitFuture(const Future<T>& f, CancelToken token,
                                                    Pin pin = Pin::None) noexcept {
        return FutureAwaiter<T>(f, token, pin);
    }

} 
