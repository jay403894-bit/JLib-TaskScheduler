// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "TaskScheduler.h"
#include "Coroutine.h"
#include "TaskDAG.h"
#include "Thread.h"
#include <chrono>
#include <cstring>

#include <atomic>
#include <cstdio>
#include <string>
#include <thread>
#include <unordered_set>
#include <stdexcept>
#include <vector>

static int g_failures = 0;

static void Check(bool cond, const char* what) {
    std::printf("  %-64s %s\n", what, cond ? "ok" : "FAILED");
    if (!cond) ++g_failures;
}

static std::atomic<int> g_ran{ 0 };
static std::atomic<int> g_resumedElsewhere{ 0 };
static std::atomic<int> g_bodyOrder{ 0 };

static JLib::Coro Halves(std::atomic<int>* firstHalf, std::atomic<int>* secondHalf) {
    const auto before = std::this_thread::get_id();
    firstHalf->fetch_add(1, std::memory_order_relaxed);

    co_await JLib::Reschedule{};

    if (std::this_thread::get_id() != before)
        g_resumedElsewhere.fetch_add(1, std::memory_order_relaxed);
    secondHalf->fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

static JLib::Coro Yielding(int times, std::atomic<int>* counter) {
    for (int i = 0; i < times; ++i) {
        counter->fetch_add(1, std::memory_order_relaxed);
        co_await JLib::Reschedule{};
    }
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

static JLib::Coro Immediate(std::atomic<int>* counter) {
    counter->fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

static std::atomic<int> g_lazyStarted{ 0 };
static std::atomic<int> g_voidRan{ 0 };

static JLib::Lazy<int> Doubled(int n) {
    g_lazyStarted.fetch_add(1, std::memory_order_relaxed);
    co_await JLib::Reschedule{};
    co_return n * 2;
}

static JLib::Lazy<int> Nested(int n) {
    const int once = co_await Doubled(n);
    co_return co_await Doubled(once);
}

static JLib::Lazy<void> VoidLazy() {
    co_await JLib::Reschedule{};
    g_voidRan.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

static JLib::Lazy<int> Thrower() {
    co_await JLib::Reschedule{};
    throw std::runtime_error("from inside a Lazy");
    co_return 0;   
}

static JLib::Lazy<int> CatchesThrower(bool* caught) {
    try {
        co_return co_await Thrower();
    }
    catch (const std::runtime_error&) {
        *caught = true;
        co_return -1;
    }
}

static JLib::Lazy<long long> Chain(int n) {
    if (n == 0) co_return 0;
    co_return 1 + co_await Chain(n - 1);
}

static JLib::Coro NodeWork(std::atomic<int>* counter) {
    counter->fetch_add(1, std::memory_order_relaxed);
    co_await JLib::Reschedule{};
    co_return;
}

struct SupersetFixture {
    JLib::SchedulerMutex m;
    JLib::SchedulerSemaphore sem{ 3 };
    std::atomic<int> overlap{ 0 }, maxOverlap{ 0 };
    std::atomic<int> coroHits{ 0 }, fiberHits{ 0 }, threadHits{ 0 };
    std::atomic<int> inside{ 0 }, maxInside{ 0 }, semDone{ 0 };

    void Enter(std::atomic<int>& cur, std::atomic<int>& peak) {
        const int now = cur.fetch_add(1, std::memory_order_acq_rel) + 1;
        int prev = peak.load(std::memory_order_relaxed);
        while (now > prev && !peak.compare_exchange_weak(prev, now)) {}
    }
};

// Fiber side of the superset tests: raw function tasks (lambda tasks may not block).
static constexpr int kMutexIters = 200, kSemIters = 100;
static void MutexFiber(void* p) {
    auto* f = static_cast<SupersetFixture*>(p);
    for (int k = 0; k < kMutexIters; ++k) {
        f->m.Lock();
        f->Enter(f->overlap, f->maxOverlap);
        f->fiberHits.fetch_add(1, std::memory_order_relaxed);
        f->overlap.fetch_sub(1, std::memory_order_acq_rel);
        f->m.Unlock();
    }
}
static void SemaphoreFiber(void* p) {
    auto* f = static_cast<SupersetFixture*>(p);
    for (int k = 0; k < kSemIters; ++k) {
        f->sem.Wait();
        f->Enter(f->inside, f->maxInside);
        f->inside.fetch_sub(1, std::memory_order_acq_rel);
        f->sem.Signal();
        f->semDone.fetch_add(1, std::memory_order_relaxed);
    }
}

// Pin mode: every resume of a coroutine must land on the worker it first ran on.
static std::atomic<int> g_pinMoved{ 0 };
// Pinned: a suspension on a worker resumes on that worker. (Main, helping from outside the pool,
// may run a segment; a suspension there is unpinned by definition and is not checked.)
static JLib::Coro PinProbe(int yields) {
    for (int i = 0; i < yields; ++i) {
        JLib::Thread* at = JLib::TaskScheduler::SelfWorker(JLib::TaskScheduler::GetWorkers());
        co_await JLib::Reschedule{};
        if (at && JLib::TaskScheduler::SelfWorker(JLib::TaskScheduler::GetWorkers()) != at) g_pinMoved.fetch_add(1, std::memory_order_relaxed);
    }
}
static JLib::Coro PinLockProbe(JLib::SchedulerMutex* m, int iters) {
    for (int i = 0; i < iters; ++i) {
        JLib::Thread* at = JLib::TaskScheduler::SelfWorker(JLib::TaskScheduler::GetWorkers());
        co_await JLib::LockAsync(*m);
        if (at && JLib::TaskScheduler::SelfWorker(JLib::TaskScheduler::GetWorkers()) != at) g_pinMoved.fetch_add(1, std::memory_order_relaxed);
        m->Unlock();
    }
}

// A coroutine waits on a group by suspending (co_await WaitAsync); its worker keeps running,
// so the inner tasks may even land on that same worker.
static void SlowBody(void* p) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    static_cast<std::atomic<int>*>(p)->fetch_add(1, std::memory_order_relaxed);
}
static JLib::Coro AwaitsGroup(std::atomic<int>* done, std::atomic<int>* after, int n) {
    auto& s = JLib::TaskScheduler::Instance();
    JLib::WaitGroup inner;
    inner.n.store(n, std::memory_order_relaxed);
    for (int i = 0; i < n; ++i) {
        JLib::Task* t = s.CreateTask(&SlowBody, done, JLib::TaskType::Fiber);
        t->waitGroup = &inner;
        s.Push(t);
    }
    co_await JLib::WaitAsync(inner);
    after->store(done->load(), std::memory_order_relaxed);
    co_return;
}

static JLib::Coro MutexCoro(SupersetFixture* f, int iters) {
    for (int k = 0; k < iters; ++k) {
        co_await JLib::LockAsync(f->m);
        f->Enter(f->overlap, f->maxOverlap);
        f->coroHits.fetch_add(1, std::memory_order_relaxed);
        f->overlap.fetch_sub(1, std::memory_order_acq_rel);
        f->m.Unlock();
    }
}

static JLib::Coro SemaphoreCoro(SupersetFixture* f, int iters) {
    for (int k = 0; k < iters; ++k) {
        co_await JLib::AcquireAsync(f->sem);
        f->Enter(f->inside, f->maxInside);
        f->inside.fetch_sub(1, std::memory_order_acq_rel);
        f->sem.Signal();
        f->semDone.fetch_add(1, std::memory_order_relaxed);
    }
}

static JLib::Coro Ordered(std::atomic<bool>* wrongOrder) {
    const int a = g_bodyOrder.fetch_add(1, std::memory_order_relaxed);
    co_await JLib::Reschedule{};
    const int b = g_bodyOrder.fetch_add(1, std::memory_order_relaxed);
    if (b <= a) wrongOrder->store(true, std::memory_order_relaxed);
    co_return;
}

static JLib::Coro TinyFrame(int x) { (void)x; co_return; }

static JLib::SchedulerMutex     g_coCancelMutex;
static JLib::SchedulerSemaphore g_coCancelSem{ 0 };
static std::atomic<int>         g_coCancelled{ 0 }, g_coAcquired{ 0 }, g_coParked{ 0 }, g_coResumed{ 0 };

static JLib::Coro LockCancelProbe() {
    g_coParked.fetch_add(1, std::memory_order_relaxed);
    const JLib::WaitResult r = co_await JLib::LockAsyncCancellable(g_coCancelMutex);
    if (r == JLib::WaitResult::Cancelled) g_coCancelled.fetch_add(1, std::memory_order_relaxed);
    else { g_coAcquired.fetch_add(1, std::memory_order_relaxed); g_coCancelMutex.Unlock(); }
    co_return;
}

static JLib::Coro AcquireCancelProbe() {
    g_coParked.fetch_add(1, std::memory_order_relaxed);
    const JLib::WaitResult r = co_await JLib::AcquireAsyncCancellable(g_coCancelSem);
    g_coResumed.fetch_add(1, std::memory_order_relaxed);
    if (r == JLib::WaitResult::Cancelled) g_coCancelled.fetch_add(1, std::memory_order_relaxed);
    else g_coAcquired.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

static void TestCoroutineCancellableAwaiters(JLib::TaskScheduler& sched) {
    std::printf("coroutines observe cancellation while awaiting a mutex\n");
    {
        g_coCancelled.store(0); g_coAcquired.store(0); g_coParked.store(0);
        JLib::CancelScope scope;
        JLib::WaitGroup wg;

        g_coCancelMutex.Lock();                       

        constexpr int kN = 4;
        for (int i = 0; i < kN; ++i) {
            JLib::Spawn(LockCancelProbe(), &wg, scope.Token().Raw());
        }
        for (int s = 0; s < 200 && g_coParked.load() < kN; ++s)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        Check(g_coParked.load() == kN, "all coroutines reached the await");

        scope.Cancel();
        
        g_coCancelMutex.Unlock();
        sched.WaitFor(wg);

        char msg[128];
        std::snprintf(msg, sizeof msg,
                      "every awaiting coroutine returned Cancelled (got %d, acquired %d, of %d)",
                      g_coCancelled.load(), g_coAcquired.load(), kN);
        Check(g_coCancelled.load() == kN, msg);
        Check(g_coAcquired.load() == 0, "none of them believed it held the lock");

        Check(g_coCancelMutex.Try_Lock(), "the mutex is unheld: cancellation acquired nothing");
        g_coCancelMutex.Unlock();
    }

    std::printf("coroutines observe cancellation while awaiting a permit\n");
    {
        g_coCancelled.store(0); g_coAcquired.store(0); g_coParked.store(0); g_coResumed.store(0);
        JLib::CancelScope scope;
        JLib::WaitGroup wg;

        constexpr int kN = 4;
        for (int i = 0; i < kN; ++i)
            JLib::Spawn(AcquireCancelProbe(), &wg, scope.Token().Raw());

        for (int s = 0; s < 200 && g_coParked.load() < kN; ++s)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        Check(g_coParked.load() == kN, "all coroutines reached the await");

        scope.Cancel();
        g_coCancelSem.Signal();
        sched.WaitFor(wg);

        char msg[144];
        std::snprintf(msg, sizeof msg,
                      "every awaiting coroutine returned Cancelled (got %d, acquired %d, resumed %d of %d)",
                      g_coCancelled.load(), g_coAcquired.load(), g_coResumed.load(), kN);
        Check(g_coCancelled.load() == kN, msg);
        Check(g_coAcquired.load() == 0, "none of them believed it held a permit");
        Check(g_coCancelSem.Try_Wait(), "the permit survived: cancellation consumed nothing");
    }

    std::printf("CancelWaiters ejects coroutine waiters with no signal\n");
    {
        g_coCancelled.store(0); g_coAcquired.store(0); g_coParked.store(0); g_coResumed.store(0);
        JLib::SchedulerSemaphore sem(0);
        JLib::CancelScope scope;
        JLib::WaitGroup wg;

        constexpr int kN = 4;
        for (int i = 0; i < kN; ++i) {
            JLib::Spawn([](JLib::SchedulerSemaphore* s) -> JLib::Coro {
                g_coParked.fetch_add(1, std::memory_order_relaxed);
                const JLib::WaitResult r = co_await JLib::AcquireAsyncCancellable(*s);
                g_coResumed.fetch_add(1, std::memory_order_relaxed);
                if (r == JLib::WaitResult::Cancelled) g_coCancelled.fetch_add(1, std::memory_order_relaxed);
                else g_coAcquired.fetch_add(1, std::memory_order_relaxed);
                co_return;
            }(&sem), &wg, scope.Token().Raw());
        }

        for (int s = 0; s < 200 && g_coParked.load() < kN; ++s)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        Check(g_coParked.load() == kN, "all coroutines parked on the throttle");

        scope.Cancel();
        sem.CancelWaiters(scope.Token());     
        sched.WaitFor(wg);

        char m2[144];
        std::snprintf(m2, sizeof m2,
                      "every coroutine woke Cancelled with no signal (got %d, acquired %d, resumed %d of %d)",
                      g_coCancelled.load(), g_coAcquired.load(), g_coResumed.load(), kN);
        Check(g_coCancelled.load() == kN, m2);
        Check(g_coAcquired.load() == 0, "none of them believed it held a permit");
        Check(!sem.Try_Wait(), "no permit was invented: the counter is untouched");
    }

    std::printf("uncancelled awaiters are unchanged\n");
    {
        g_coCancelled.store(0); g_coAcquired.store(0); g_coParked.store(0);
        JLib::WaitGroup wg;
        JLib::Spawn(AcquireCancelProbe(), &wg);       
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        g_coCancelSem.Signal();
        sched.WaitFor(wg);
        Check(g_coAcquired.load() == 1, "an awaiter with no scope acquires normally");
        Check(g_coCancelled.load() == 0, "and does not report cancellation");
    }
}

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    const bool pin = argc > 1 && std::strchr(argv[1], 'p');

    JLib::TaskScheduler::Config cfg;
    cfg.mode = pin ? JLib::Mode::Pinned : JLib::Mode::Migrate;
    cfg.main = JLib::MainMode::OutOfPool;
    cfg.slab = { 4096, 512, 4096 };
    JLib::TaskScheduler::Init(cfg);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("coroutine mode -- workers=%zu fibers=%s\n\n", sched.GetWorkerCount(), pin ? "Pin" : "Migrate");

    if (pin) {
        std::printf("Pin: coroutines resume on their home worker\n");
        g_pinMoved.store(0);
        JLib::SchedulerMutex m;
        JLib::WaitGroup wg;
        for (int i = 0; i < 64; ++i) JLib::Spawn(PinProbe(16), &wg);
        for (int i = 0; i < 16; ++i) JLib::Spawn(PinLockProbe(&m, 64), &wg);
        sched.WaitFor(wg);
        Check(g_pinMoved.load() == 0, "no resume (yield or lock wake) changed worker");
        if (g_pinMoved.load()) std::printf("      moved %d times\n", g_pinMoved.load());
    }

    std::printf("co_await WaitAsync: a coroutine suspends on a group\n");
    {
        constexpr int kCoros = 32, kInner = 4;
        std::vector<std::atomic<int>> done(kCoros), after(kCoros);
        JLib::WaitGroup wg;
        for (int i = 0; i < kCoros; ++i) {
            done[i].store(0); after[i].store(-1);
            JLib::Spawn(AwaitsGroup(&done[i], &after[i], kInner), &wg);
        }
        sched.WaitFor(wg);
        int bad = 0;
        for (int i = 0; i < kCoros; ++i) if (after[i].load() != kInner) ++bad;
        Check(bad == 0, "each coroutine resumed only after its whole inner group finished");

        // Already-finished group: await_ready short-circuits, no suspension.
        JLib::WaitGroup outer;
        std::atomic<int> d{ 0 }, a{ -1 };
        JLib::Spawn(AwaitsGroup(&d, &a, 0), &outer);
        sched.WaitFor(outer);
        Check(a.load() == 0, "awaiting an empty group resumes at once");
    }

    std::printf("scheduling\n");
    {
        std::atomic<int> first{ 0 }, second{ 0 };
        g_ran.store(0);

        JLib::Coro c = Halves(&first, &second);
        
        Check(first.load() == 0 && second.load() == 0,
              "constructing a Coro runs none of the body (initial_suspend)");

        JLib::WaitGroup wg;
        const bool spawned = JLib::Spawn(std::move(c), &wg);
        Check(spawned, "Spawn accepted the coroutine");
        sched.WaitFor(wg);

        Check(first.load() == 1, "the half before co_await ran exactly once");
        Check(second.load() == 1, "the half after co_await ran exactly once");
        Check(g_ran.load() == 1, "the coroutine reached its end");
    }

    std::printf("completes inside the first resume\n");
    {
        std::atomic<int> n{ 0 };
        g_ran.store(0);
        JLib::WaitGroup wg;
        for (int i = 0; i < 64; ++i) JLib::Spawn(Immediate(&n), &wg);
        sched.WaitFor(wg);
        Check(n.load() == 64, "all 64 non-suspending coroutines ran");
        Check(g_ran.load() == 64, "all 64 reached their end");
    }

    std::printf("repeated suspension\n");
    {
        std::atomic<int> hits{ 0 };
        g_ran.store(0);
        JLib::WaitGroup wg;
        const int kCoros = 128, kYields = 16;
        for (int i = 0; i < kCoros; ++i) JLib::Spawn(Yielding(kYields, &hits), &wg);
        sched.WaitFor(wg);
        Check(hits.load() == kCoros * kYields, "every suspension resumed exactly once");
        Check(g_ran.load() == kCoros, "every coroutine reached its end");
    }

    std::printf("WaitGroup completion\n");
    {
        std::atomic<int> hits{ 0 };
        JLib::WaitGroup wg;
        for (int i = 0; i < 32; ++i) JLib::Spawn(Yielding(8, &hits), &wg);
        sched.WaitFor(wg);
        
        Check(hits.load() == 32 * 8, "WaitFor returned only after every body completed");
        Check((wg.n.load() & JLib::WaitGroup::COUNT_MASK) == 0, "the group drained to zero");
    }

    std::printf("ordering across a suspension\n");
    {
        std::atomic<bool> wrong{ false };
        g_bodyOrder.store(0);
        JLib::WaitGroup wg;
        for (int i = 0; i < 64; ++i) JLib::Spawn(Ordered(&wrong), &wg);
        sched.WaitFor(wg);
        Check(!wrong.load(), "the second half of each body ran after its own first half");
    }

    std::printf("slab accounting (leaks and double frees live here)\n");
    {
        std::atomic<int> hits{ 0 };
        int spawnFailures = 0;
        const int kBatches = 200, kPerBatch = 64;          
        for (int b = 0; b < kBatches; ++b) {
            JLib::WaitGroup wg;
            for (int i = 0; i < kPerBatch; ++i)
                if (!JLib::Spawn(Yielding(3, &hits), &wg)) ++spawnFailures;
            sched.WaitFor(wg);
        }
        Check(spawnFailures == 0,
              "12,800 coroutines through a 4,096-slot slab: no allocation ever failed");
        Check(hits.load() == kBatches * kPerBatch * 3,
              "every suspension of every coroutine resumed exactly once");
        if (spawnFailures) std::printf("      %d Spawn calls failed -- slab exhausted\n", spawnFailures);
    }

    std::printf("un-spawned coroutine\n");
    {
        std::atomic<int> n{ 0 };
        {
            JLib::Coro c = Immediate(&n);   
        }
        Check(n.load() == 0, "an un-spawned coroutine never runs its body");
    }

    std::printf("superset mutex (coroutines + fibers + bare threads, same lock)\n");
    SupersetFixture fx;
    {
        const int kIters = 200;
        JLib::WaitGroup coroWg;
        for (int i = 0; i < 8; ++i) JLib::Spawn(MutexCoro(&fx, kIters), &coroWg);

        JLib::WaitGroup fiberWg;
        fiberWg.n.store(4, std::memory_order_relaxed);
        for (int i = 0; i < 4; ++i) {
            auto* t = sched.CreateTask(&MutexFiber, &fx, JLib::TaskType::Fiber);
            t->waitGroup = &fiberWg;
            sched.Push(t);
        }

        std::vector<std::thread> threads;
        for (int i = 0; i < 3; ++i) {
            threads.emplace_back([&fx, kIters] {
                for (int k = 0; k < kIters; ++k) {
                    fx.m.Lock();
                    fx.Enter(fx.overlap, fx.maxOverlap);
                    fx.threadHits.fetch_add(1, std::memory_order_relaxed);
                    fx.overlap.fetch_sub(1, std::memory_order_acq_rel);
                    fx.m.Unlock();
                }
            });
        }

        sched.WaitFor(coroWg);
        sched.WaitFor(fiberWg);
        for (auto& th : threads) th.join();

        Check(fx.coroHits.load()   == 8 * kIters, "every coroutine acquisition completed");
        Check(fx.fiberHits.load()  == 4 * kIters, "every fiber acquisition completed");
        Check(fx.threadHits.load() == 3 * kIters, "every bare-thread acquisition completed");
        Check(fx.maxOverlap.load() == 1, "never two holders at once across all three context kinds");
        if (fx.maxOverlap.load() != 1)
            std::printf("      max simultaneous holders observed: %d\n", fx.maxOverlap.load());
    }

    std::printf("superset semaphore (permit count is the invariant)\n");
    {
        const int kPermits = 3;   
        const int kIters = 100;
        JLib::WaitGroup coroWg;
        for (int i = 0; i < 8; ++i) JLib::Spawn(SemaphoreCoro(&fx, kIters), &coroWg);

        JLib::WaitGroup fiberWg;
        fiberWg.n.store(4, std::memory_order_relaxed);
        for (int i = 0; i < 4; ++i) {
            auto* t = sched.CreateTask(&SemaphoreFiber, &fx, JLib::TaskType::Fiber);
            t->waitGroup = &fiberWg;
            sched.Push(t);
        }

        sched.WaitFor(coroWg);
        sched.WaitFor(fiberWg);

        Check(fx.semDone.load() == 12 * kIters, "every coroutine and fiber acquisition completed");
        Check(fx.maxInside.load() <= kPermits, "never more than the permit count inside at once");
        if (fx.maxInside.load() > kPermits)
            std::printf("      max concurrent permit holders: %d (limit %d)\n", fx.maxInside.load(), kPermits);
    }

    std::printf("Lazy<T> values\n");
    {
        Check(JLib::SyncWait(Doubled(21)) == 42, "SyncWait returns the coroutine's value");
        Check(JLib::SyncWait(Nested(10)) == 40, "a Lazy awaiting a Lazy composes (2x then 2x)");

        g_voidRan.store(0);
        JLib::SyncWait(VoidLazy());
        Check(g_voidRan.load() == 1, "Lazy<void> runs and SyncWait returns");
    }

    std::printf("Lazy<T> laziness and lifetime\n");
    {
        g_lazyStarted.store(0);
        {
            JLib::Lazy<int> l = Doubled(1);       
            Check(g_lazyStarted.load() == 0, "a Lazy does not start until it is awaited");
        }                                          
        Check(true, "destroying an un-awaited Lazy is well defined");

        Check(JLib::SyncWait(Doubled(3)) == 6, "the pool still works after an abandoned Lazy");
    }

    std::printf("Lazy<T> exceptions\n");
    {
        bool caught = false;
        int observed = 0;
        
        observed = JLib::SyncWait(CatchesThrower(&caught));
        Check(caught, "an exception thrown in a Lazy surfaces at the awaiting co_await");
        Check(observed == -1, "the awaiting coroutine resumed normally after catching");
    }

    std::printf("deep await chain (O(1) stack, or this crashes)\n");
    {
        const int kDepth = 100000;
        const long long got = JLib::SyncWait(Chain(kDepth));
        Check(got == kDepth, "100,000-deep await chain returned the right value");
        if (got != kDepth) std::printf("      expected %d, got %lld\n", kDepth, got);
    }

    std::printf("coroutine completes a DAG external node\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::TaskDAG dag(sched);
        auto* n    = dag.CreateExternalNode();
        auto* tail = dag.CreateNode(sched.CreateTask([&ran] { ran.fetch_add(10, std::memory_order_relaxed); }));
        dag.AddDependency(tail, n);
        Check(dag.Submit(), "DAG submitted");

        Check(JLib::Spawn(NodeWork(&ran), n), "coroutine spawned onto the external node");

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (ran.load(std::memory_order_acquire) != 11 &&
               std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
        Check(ran.load() == 11, "coroutine ran, signalled the node, and the dependent fired");
    }

    std::printf("100 coroutines, one external node each\n");
    {
        const int kN = 100;
        std::atomic<int> ran{ 0 };
        JLib::TaskDAG dag(sched);
        std::vector<JLib::TaskNode*> ext;
        ext.reserve(kN);
        bool allocOk = true;
        try {
            for (int i = 0; i < kN; ++i) {
                auto* n = dag.CreateExternalNode();
                auto* task = sched.CreateTask([&ran] { ran.fetch_add(10, std::memory_order_relaxed); });
                auto* t = task ? dag.CreateNode(task) : nullptr;
                if (!n || !t) { allocOk = false; break; }
                dag.AddDependency(t, n);
                ext.push_back(n);
            }
        }
        catch (const std::exception& e) {
            allocOk = false;
            std::printf("      slab exhausted: %s\n", e.what());
        }
        Check(allocOk, "every node allocated (slab not exhausted)");
        Check(dag.Submit(), "DAG submitted");
        int spawnFailures = 0;
        for (auto* n : ext) if (!JLib::Spawn(NodeWork(&ran), n)) ++spawnFailures;
        Check(spawnFailures == 0, "all 100 coroutines spawned");

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (ran.load(std::memory_order_acquire) != kN * 11 &&
               std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
        Check(ran.load() == kN * 11, "every coroutine completed its node exactly once");
        if (ran.load() != kN * 11) std::printf("      expected %d, got %d\n", kN * 11, ran.load());
    }

    std::printf("coroutine frames use the 64-byte size class\n");
    {
        auto* alloc = JLib::TaskScheduler::Instance().GetAllocator();

        {
            long long last = alloc->SmallLiveCount();
            int stable = 0;
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (stable < 20 && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                const long long now = alloc->SmallLiveCount();
                stable = (now == last) ? stable + 1 : 0;
                last = now;
            }
        }

        const long long bigBase   = alloc->LiveCount();
        const long long smallBase = alloc->SmallLiveCount();

        constexpr int kN = 200;
        std::vector<JLib::Coro> held;
        held.reserve(kN);
        for (int i = 0; i < kN; ++i) held.push_back(TinyFrame(i));

        const long long bigHeld   = alloc->LiveCount()      - bigBase;
        const long long smallHeld = alloc->SmallLiveCount() - smallBase;
        std::printf("    %d live frames -> %lld small slots, %lld 256-byte slots\n",
                    kN, smallHeld, bigHeld);

        constexpr long long kSlack = 4;
        Check(smallHeld >= kN - kSlack, "every small frame came from the 64-byte class");
        
        Check(bigHeld < kN, "they did not consume 256-byte slots");

        held.clear();
        const long long leaked = alloc->SmallLiveCount() - smallBase;
        Check(leaked <= kSlack && leaked >= -kSlack,
              "and every one was returned on destruction");
        if (leaked > kSlack || leaked < -kSlack)
            std::printf("      counter drifted by %lld (slack %lld)\n", leaked, kSlack);
    }

    TestCoroutineCancellableAwaiters(JLib::TaskScheduler::Instance());

    std::printf("\n%s\n", g_failures == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_failures == 0 ? 0 : 1;
}
