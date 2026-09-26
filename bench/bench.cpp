// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// SchedulerBench: named workloads, each run as fiber tasks and as coroutines where both apply.
// C++20, built against the coroutine library. Run with --help for the options.

#include "bench_common.h"

#include <TaskScheduler.h>
#include <Thread.h>
#include <TaskDAG.h>
#include <Coroutine.h>
#include <Stats.h>
#include <Timer.h>
#include <IoReactor.h>
#include <IoAsync.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <mutex>
#include <thread>
#include <vector>

#if !defined(_WIN32)
#include <fcntl.h>
#include <unistd.h>
#endif

#if !defined(JLIBSCHED_VERSION_STRING)
#define JLIBSCHED_VERSION_STRING "unknown"
#endif

using namespace JLib;
using Clock = std::chrono::steady_clock;

namespace {

enum class Kind { Fiber, Coro, None };
const char* KindName(Kind k) { return k == Kind::Fiber ? "fiber" : k == Kind::Coro ? "coro" : "-"; }

TaskScheduler& S() { return TaskScheduler::Instance(); }

// Correctness checks are FATAL, not advisory: a green table beside an ignorable stderr note is
// exactly the failure mode a benchmark must not have. Both channels get the message (the table
// gets pasted; stderr goes to logs) and main exits nonzero, so exit code 0 means every check held.
bool g_checkFailed = false;
bool g_ioOpenFailed = false;   // CaseIo: temp file unusable -> skip with a reason, print no row

// ---- workloads -------------------------------------------------------------------------------
// Each returns the number of operations it performed; the harness times it.

void Nop(void*) {}

std::uint64_t PushAndWait(void (*fn)(void*), void* arg, int n) {
    WaitGroup wg;
    wg.n.store(n);
    for (int i = 0; i < n; ++i) {
        Task* t = S().CreateTask(fn, arg, TaskType::Fiber);
        t->waitGroup = &wg;
        S().Push(t);
    }
    S().WaitFor(wg);
    return (std::uint64_t)n;
}

Coro CoroNop() { co_return; }

// spawn: N empty tasks pushed one at a time.
constexpr int kSpawnN = 200000;
std::uint64_t CaseSpawn(Kind k) {
    if (k == Kind::Fiber) return PushAndWait(&Nop, nullptr, kSpawnN);
    WaitGroup wg;
    for (int i = 0; i < kSpawnN; ++i) Spawn(CoroNop(), &wg);
    S().WaitFor(wg);
    return kSpawnN;
}

std::uint64_t CaseSpawnLambda(Kind) {
    WaitGroup wg;
    wg.n.store(kSpawnN);
    for (int i = 0; i < kSpawnN; ++i) {
        Task* t = S().CreateTask([] {});
        t->waitGroup = &wg;
        S().Push(t);
    }
    S().WaitFor(wg);
    return kSpawnN;
}

// batch: N empty tasks in batches of 1024.
std::uint64_t Batch(TaskScheduler::BatchSpread spread) {
    constexpr int kBatch = 1024;
    std::vector<Task*> tasks(kBatch);
    WaitGroup wg;
    wg.n.store(kSpawnN);
    for (int done = 0; done < kSpawnN; done += kBatch) {
        const int n = std::min(kBatch, kSpawnN - done);
        for (int i = 0; i < n; ++i) {
            tasks[i] = S().CreateTask(&Nop, nullptr, TaskType::Fiber);
            tasks[i]->waitGroup = &wg;
        }
        S().PushBatch(tasks.data(), (size_t)n, spread);
    }
    S().WaitFor(wg);
    return kSpawnN;
}
std::uint64_t CaseBatchWide(Kind)   { return Batch(TaskScheduler::BatchSpread::Wide); }
std::uint64_t CaseBatchNarrow(Kind) { return Batch(TaskScheduler::BatchSpread::Narrow); }

// pfor. The unit is a CHUNK -- one grain piece, which is the thing ParallelFor actually pushes --
// so this is in the same units as the batch cases. The body does real work (64 LCG steps and a
// store per item): with an empty body the only question answered is whether split/join works.
// Read the grain sweep as "time should flatten as the grain grows", not as ops/s climbing, and
// read the speedup against the same body run serially.
constexpr int kPforN = bench::kPforCoarseN;
std::vector<std::uint64_t> g_pforOut((size_t)bench::kPforN);
int    g_pforGrain = 0;      // 0 = let the scheduler choose
double g_pforSerialMs = 0;   // the same body in one thread
double g_pforSpeedup = 0;

// One call = one chunk. Counted rather than derived from the grain: ParallelFor chunks through a
// shared cursor, so the pieces it actually runs are not simply n/grain.
std::atomic<std::uint64_t> g_pforChunks{ 0 };
void PforBody(int b, int e) {
    g_pforChunks.fetch_add(1, std::memory_order_relaxed);
    for (int i = b; i < e; ++i) g_pforOut[i] = bench::LcgSteps((std::uint64_t)i, bench::kPforCoarseIters);
}

int PforAutoGrain() {
    const size_t w = S().GetWorkerCount() ? S().GetWorkerCount() : 1;
    const int g = (int)((size_t)kPforN / (8 * w));
    return g < 1 ? 1 : g;
}

std::uint64_t CasePfor(Kind) {
    const int grain = g_pforGrain > 0 ? g_pforGrain : PforAutoGrain();
    g_pforChunks.store(0, std::memory_order_relaxed);
    S().ParallelFor(0, kPforN, grain, [](int b, int e) { PforBody(b, e); });
    return g_pforChunks.load(std::memory_order_relaxed);
}
std::uint64_t CasePforG1(Kind k)   { g_pforGrain = 1;   return CasePfor(k); }
std::uint64_t CasePforG16(Kind k)  { g_pforGrain = 16;  return CasePfor(k); }
std::uint64_t CasePforG256(Kind k) { g_pforGrain = 256; return CasePfor(k); }
std::uint64_t CasePforAuto(Kind k) { g_pforGrain = 0;   return CasePfor(k); }

// A MANUAL range split: the array cut into a fixed number of chunks, submitted as tasks. This is
// the shape a hand-written range loop has, and it is the only way to test a specific chunk count,
// since ParallelFor's grain is advisory (its cursor hands out ~8 pieces per worker whatever the
// grain says). Same body and same baseline as pfor, so the rows are directly comparable.
int g_rangeChunks = 32;
std::uint64_t CaseRange(Kind) {
    const int chunks = g_rangeChunks;
    const int per = (kPforN + chunks - 1) / chunks;
    g_pforChunks.store(0, std::memory_order_relaxed);
    std::vector<Task*> tasks;
    tasks.reserve((size_t)chunks);
    WaitGroup wg;
    wg.n.store(chunks);
    for (int c = 0; c < chunks; ++c) {
        const int b = c * per;
        const int e = std::min(b + per, kPforN);
        Task* t = S().CreateTask([b, e] { PforBody(b, e); });
        t->waitGroup = &wg;
        tasks.push_back(t);
    }
    S().PushBatch(tasks.data(), tasks.size(), TaskScheduler::BatchSpread::Wide);
    S().WaitFor(wg);
    return g_pforChunks.load(std::memory_order_relaxed);
}
std::uint64_t CaseRange32(Kind k)  { g_rangeChunks = 32; return CaseRange(k); }

// PushArray: the same fixed split, but the library builds the chunk tasks itself -- one WaitGroup
// increment for the whole set instead of one per task, and a single PushBatch. This is the shape a
// caller uses when the chunk size is already known, and it skips ParallelFor's probing entirely.
std::uint64_t CasePushArray(Kind) {
    const int chunks = g_rangeChunks;
    const int per = (kPforN + chunks - 1) / chunks;
    g_pforChunks.store(0, std::memory_order_relaxed);
    WaitGroup wg;
    S().PushArray(0, (size_t)kPforN, (size_t)per, [](size_t i) {
        g_pforOut[i] = bench::LcgSteps((std::uint64_t)i, bench::kPforCoarseIters);
    }, &wg);
    S().WaitFor(wg);
    return (std::uint64_t)chunks;
}
std::uint64_t CasePushArray32(Kind k) { g_rangeChunks = 32; return CasePushArray(k); }

// The shape PushArray is actually for: MANY SMALL ITEMS. One WaitGroup increment for the whole set
// and one PushBatch, against 200k separate CreateTask+Push calls (spawn_lambda) or hand-built
// batches (batch_wide). Same trivial body in all three.
std::vector<std::uint64_t> g_fineOut((size_t)kSpawnN);
std::uint64_t CasePushArrayFine(Kind) {
    WaitGroup wg;
    const size_t n = S().PushArray(0, (size_t)kSpawnN, 1,
                                   [](size_t i) { g_fineOut[i] = i; }, &wg);
    S().WaitFor(wg);
    return (std::uint64_t)n;
}
std::uint64_t CasePushArray8w(Kind k) {
    const size_t w = S().GetWorkerCount() ? S().GetWorkerCount() : 1;
    g_rangeChunks = (int)(8 * w);
    return CasePushArray(k);
}
std::uint64_t CaseRange8w(Kind k)  {
    const size_t w = S().GetWorkerCount() ? S().GetWorkerCount() : 1;
    g_rangeChunks = (int)(8 * w);
    return CaseRange(k);
}

// Median of three runs of the body in one thread: the baseline every pfor row is measured against.
// MIN of several runs after a warmup: the fastest run is the one least disturbed by frequency ramp
// or a migration onto a slower core. Must be called BEFORE Init -- measured beside a live pool this
// is not a baseline, it is the loop plus whatever the idle pool does.
void MeasurePforSerial() {
    if (g_pforSerialMs > 0) return;
    PforBody(0, kPforN);   // warmup
    double best = 1e300;
    for (int r = 0; r < 7; ++r) {
        const auto t0 = Clock::now();
        PforBody(0, kPforN);   // one chunk, one thread
        const double ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
        if (ms < best) best = ms;
    }
    g_pforSerialMs = best;
}

// fib: recursive fork-join; every internal node suspends on its children.
constexpr int kFibN = 35, kFibCutoff = 20;
std::uint64_t FibSerial(int n) { return n < 2 ? (std::uint64_t)n : FibSerial(n - 1) + FibSerial(n - 2); }
std::atomic<std::uint64_t> g_fibNodes{ 0 };

struct FibArgs { int n; std::uint64_t result; };
void FibTask(void* p) {
    FibArgs* a = static_cast<FibArgs*>(p);
    g_fibNodes.fetch_add(1, std::memory_order_relaxed);
    if (a->n <= kFibCutoff) { a->result = FibSerial(a->n); return; }
    FibArgs x{ a->n - 1, 0 }, y{ a->n - 2, 0 };
    WaitGroup wg;
    wg.n.store(2);
    Task* tx = S().CreateTask(&FibTask, &x, TaskType::Fiber); tx->waitGroup = &wg; S().Push(tx);
    Task* ty = S().CreateTask(&FibTask, &y, TaskType::Fiber); ty->waitGroup = &wg; S().Push(ty);
    S().WaitFor(wg);
    a->result = x.result + y.result;
}
Coro FibCoro(int n, std::uint64_t* out) {
    g_fibNodes.fetch_add(1, std::memory_order_relaxed);
    if (n <= kFibCutoff) { *out = FibSerial(n); co_return; }
    std::uint64_t x = 0, y = 0;
    WaitGroup wg;
    Spawn(FibCoro(n - 1, &x), &wg);
    Spawn(FibCoro(n - 2, &y), &wg);
    co_await WaitAsync(wg);
    *out = x + y;
}
std::uint64_t CaseFib(Kind k) {
    g_fibNodes = 0;
    std::uint64_t result = 0;
    if (k == Kind::Fiber) {
        FibArgs a{ kFibN, 0 };
        PushAndWait(&FibTask, &a, 1);
        result = a.result;
    } else {
        WaitGroup wg;
        Spawn(FibCoro(kFibN, &result), &wg);
        S().WaitFor(wg);
    }
    if (result != bench::kFibExpected) {
        g_checkFailed = true;
        std::fprintf(stderr, "CHECK FAILED: fib wrong result %llu (want %llu)\n",
                     (unsigned long long)result, (unsigned long long)bench::kFibExpected);
        std::printf("  *** CHECK FAILED: fib wrong result %llu (want %llu)\n",
                    (unsigned long long)result, (unsigned long long)bench::kFibExpected);
    }
    return g_fibNodes.load();
}

// pingpong: each task repeatedly waits on a lambda child (one suspend/resume per iteration).
constexpr int kPingTasks = 256, kPingIters = 200;
void PingTask(void*) {
    for (int i = 0; i < kPingIters; ++i) {
        WaitGroup wg;
        wg.n.store(1);
        Task* t = S().CreateTask([] {});
        t->waitGroup = &wg;
        S().Push(t);
        S().WaitFor(wg);
    }
}
Coro PingCoro() {
    for (int i = 0; i < kPingIters; ++i) {
        WaitGroup wg;
        wg.n.store(1);
        Task* t = S().CreateTask([] {});
        t->waitGroup = &wg;
        S().Push(t);
        co_await WaitAsync(wg);
    }
}
std::uint64_t CasePingPong(Kind k) {
    if (k == Kind::Fiber) PushAndWait(&PingTask, nullptr, kPingTasks);
    else {
        WaitGroup wg;
        for (int i = 0; i < kPingTasks; ++i) Spawn(PingCoro(), &wg);
        S().WaitFor(wg);
    }
    return (std::uint64_t)kPingTasks * kPingIters;
}

// yield: each task yields repeatedly.
constexpr int kYieldTasks = 64, kYieldIters = 2000;
void YieldTask(void*) { for (int i = 0; i < kYieldIters; ++i) if (Fiber* f = FiberFromStack()) f->Yield(); }
Coro YieldCoro() { for (int i = 0; i < kYieldIters; ++i) co_await Reschedule{}; }
std::uint64_t CaseYield(Kind k) {
    if (k == Kind::Fiber) PushAndWait(&YieldTask, nullptr, kYieldTasks);
    else {
        WaitGroup wg;
        for (int i = 0; i < kYieldTasks; ++i) Spawn(YieldCoro(), &wg);
        S().WaitFor(wg);
    }
    return (std::uint64_t)kYieldTasks * kYieldIters;
}

// mutex: many tasks contend on one lock with a short critical section.
constexpr int kMutexTasks = 64, kMutexIters = 2000;
SchedulerMutex g_mutex;
std::uint64_t g_mutexCount = 0;
void MutexTask(void*) {
    for (int i = 0; i < kMutexIters; ++i) { g_mutex.Lock(); ++g_mutexCount; g_mutex.Unlock(); }
}
Coro MutexCoro() {
    for (int i = 0; i < kMutexIters; ++i) { co_await LockAsync(g_mutex); ++g_mutexCount; g_mutex.Unlock(); }
}
std::uint64_t CaseMutex(Kind k) {
    g_mutexCount = 0;
    if (k == Kind::Fiber) PushAndWait(&MutexTask, nullptr, kMutexTasks);
    else {
        WaitGroup wg;
        for (int i = 0; i < kMutexTasks; ++i) Spawn(MutexCoro(), &wg);
        S().WaitFor(wg);
    }
    const std::uint64_t want = (std::uint64_t)kMutexTasks * kMutexIters;
    if (g_mutexCount != want) {   // dropped or double-counted increments -- no green row over this
        g_checkFailed = true;
        std::fprintf(stderr, "CHECK FAILED: mutex count %llu, want %llu\n",
                     (unsigned long long)g_mutexCount, (unsigned long long)want);
        std::printf("  *** CHECK FAILED: mutex count %llu, want %llu\n",
                    (unsigned long long)g_mutexCount, (unsigned long long)want);
    }
    return want;
}

// The crossover: at what critical-section length does SUSPENDING the waiter beat BLOCKING it?
// Blocking wins while the hold is short -- no suspend, no resume, no requeue, and the OS lock
// adaptive-spins -- but a blocked worker runs nothing, so a long hold costs a whole core. Same
// tasks, same iterations, same hold: only the lock differs. The hold is a busy wait, because a
// sleep would round to the OS tick and measure the clock instead of the lock.
constexpr int kLockTasks = 64;
int  g_holdUs = 0;
bool g_lockBlocking = false;
std::mutex g_stdMutex;
std::uint64_t g_lockCount = 0;

int LockIters() {   // keep the serialised total near a tenth of a second at every hold
    return g_holdUs <= 0 ? 2000 : std::max(20, 2000 / (1 + g_holdUs));
}
void HoldFor(int us) {
    if (us <= 0) return;
    const auto end = Clock::now() + std::chrono::microseconds(us);
    while (Clock::now() < end) { }
}
void LockTask(void*) {
    const int iters = LockIters();
    for (int i = 0; i < iters; ++i) {
        if (g_lockBlocking) {
            std::lock_guard<std::mutex> l(g_stdMutex);
            ++g_lockCount;
            HoldFor(g_holdUs);
        } else {
            g_mutex.Lock();
            ++g_lockCount;
            HoldFor(g_holdUs);
            g_mutex.Unlock();
        }
    }
}
std::uint64_t CaseLock(Kind) {
    g_lockCount = 0;
    const int iters = LockIters();
    PushAndWait(&LockTask, nullptr, kLockTasks);
    return (std::uint64_t)kLockTasks * iters;
}
std::uint64_t CaseLockSusp0(Kind k)    { g_holdUs = 0;   g_lockBlocking = false; return CaseLock(k); }
std::uint64_t CaseLockBlock0(Kind k)   { g_holdUs = 0;   g_lockBlocking = true;  return CaseLock(k); }
std::uint64_t CaseLockSusp1(Kind k)    { g_holdUs = 1;   g_lockBlocking = false; return CaseLock(k); }
std::uint64_t CaseLockBlock1(Kind k)   { g_holdUs = 1;   g_lockBlocking = true;  return CaseLock(k); }
std::uint64_t CaseLockSusp10(Kind k)   { g_holdUs = 10;  g_lockBlocking = false; return CaseLock(k); }
std::uint64_t CaseLockBlock10(Kind k)  { g_holdUs = 10;  g_lockBlocking = true;  return CaseLock(k); }
std::uint64_t CaseLockSusp100(Kind k)  { g_holdUs = 100; g_lockBlocking = false; return CaseLock(k); }
std::uint64_t CaseLockBlock100(Kind k) { g_holdUs = 100; g_lockBlocking = true;  return CaseLock(k); }

// UNCONTENDED: one task, one lock, nobody else. The sweep above cannot measure this -- there every
// acquire is contended, so the fast path is never taken and the shape of the lock WORD is invisible.
// It is the acquire that costs nothing but its own atomics, which is the common case in real code
// and the only one that says whether the fast path is worth anything. std::mutex beside it as the
// floor: that is what an uncontended OS mutex costs on this machine.
//
// No hold, and a single task, so nothing here ever suspends or touches the waiter list.
constexpr int kUncontIters = 2000000;
std::uint64_t g_uncontSink = 0;

void UncontTask(void*) {
    for (int i = 0; i < kUncontIters; ++i) {
        if (g_lockBlocking) { g_stdMutex.lock(); ++g_uncontSink; g_stdMutex.unlock(); }
        else                { g_mutex.Lock();    ++g_uncontSink; g_mutex.Unlock();    }
    }
}
std::uint64_t CaseLockUncont(Kind) {
    g_uncontSink = 0; g_lockBlocking = false;
    PushAndWait(&UncontTask, nullptr, 1);
    return (std::uint64_t)kUncontIters;
}
std::uint64_t CaseLockUncontStd(Kind) {
    g_uncontSink = 0; g_lockBlocking = true;
    PushAndWait(&UncontTask, nullptr, 1);
    return (std::uint64_t)kUncontIters;
}

// Try_Lock on a lock nobody holds, then release: the same fast path reached through the other door.
// Worth its own case because Try_Lock used to take the spinlock to read a bool, so a FAILED try
// cost two RMWs -- and a failed try is the whole point of Try_Lock.
void UncontTryTask(void*) {
    for (int i = 0; i < kUncontIters; ++i) {
        if (g_mutex.Try_Lock()) { ++g_uncontSink; g_mutex.Unlock(); }
    }
}
std::uint64_t CaseLockUncontTry(Kind) {
    g_uncontSink = 0;
    PushAndWait(&UncontTryTask, nullptr, 1);
    return (std::uint64_t)kUncontIters;
}

// The case the pure-contention sweep above CANNOT show. There, every task wants the same lock, so
// a suspended waiter frees a worker that has nothing to run but another waiter -- suspension can
// only lose. The question that matters for a frame is what happens to the REST OF THE WORK while a
// lock is held: with more waiters than workers and a blocking lock, every worker ends up asleep in
// the kernel and the pool is dead; with a suspending lock the waiters step aside and the ordinary
// work keeps flowing. So: contenders on one lock, plus a stream of independent compute tasks, and
// the number reported is when the INDEPENDENT work finished.
constexpr int kMixCompute = 4000;
std::atomic<int> g_mixDone{ 0 };
double g_mixComputeMs = 0, g_mixTotalMs = 0;
void MixCompute(void*) {
    volatile std::uint64_t x = bench::LcgSteps(7, 400);   // ~2 us of ordinary work
    (void)x;
    g_mixDone.fetch_add(1, std::memory_order_relaxed);
}
std::uint64_t CaseLockMix(Kind) {
    g_lockCount = 0;
    g_mixDone.store(0, std::memory_order_relaxed);
    WaitGroup contend, compute;
    contend.n.store(kLockTasks);
    compute.n.store(kMixCompute);
    const auto t0 = Clock::now();
    for (int i = 0; i < kLockTasks; ++i) {
        Task* t = S().CreateTask(&LockTask, nullptr, TaskType::Fiber);
        t->waitGroup = &contend;
        S().Push(t);
    }
    for (int i = 0; i < kMixCompute; ++i) {
        Task* t = S().CreateTask(&MixCompute, nullptr, TaskType::Fiber);
        t->waitGroup = &compute;
        S().Push(t);
    }
    S().WaitFor(compute);
    g_mixComputeMs = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
    S().WaitFor(contend);
    g_mixTotalMs = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
    return kMixCompute;
}
std::uint64_t CaseMixSusp(Kind k)  { g_holdUs = 100; g_lockBlocking = false; return CaseLockMix(k); }
std::uint64_t CaseMixBlock(Kind k) { g_holdUs = 100; g_lockBlocking = true;  return CaseLockMix(k); }

// dag: root -> N middles -> join.
constexpr int kDagN = 4096;
std::uint64_t CaseDag(Kind) {
    WaitGroup wg;
    wg.n.store(kDagN + 2);
    TaskDAG dag(S());
    auto make = [&wg]() { Task* t = S().CreateTask(&Nop, nullptr, TaskType::Fiber); t->waitGroup = &wg; return t; };
    auto* root = dag.CreateNode(make());
    auto* join = dag.CreateNode(make());
    for (int i = 0; i < kDagN; ++i) {
        auto* mid = dag.CreateNode(make());
        dag.AddDependency(mid, root);
        dag.AddDependency(join, mid);
    }
    dag.Submit();
    S().WaitFor(wg);
    return kDagN + 2;
}

// wake latency: push one task into an idle pool and time until it starts. With main out of the
// pool the caller only polls, so this is the pool's own pickup time; with main in the pool it
// waits in WaitFor like any in-pool main, and may pick the task up itself.
constexpr int kLatencySamples = 2000;
// The gap before each sample decides WHICH path is measured: longer than the hunter's hot window
// and the pool has cooled (and workers parked), shorter and the burst is still hot. A frame has
// both, so both are reported.
int g_latencyGapUs = bench::kLatencyGapUs;
std::vector<double> g_latUs;
std::atomic<std::int64_t> g_startNs{ 0 };
std::int64_t NowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count(); }
void LatTask(void*) { g_startNs.store(NowNs(), std::memory_order_release); }
Coro LatCoro() { g_startNs.store(NowNs(), std::memory_order_release); co_return; }
double g_latP50 = 0, g_latP99 = 0;
std::uint64_t CaseLatency(Kind k) {
    const bool mainInPool = TaskScheduler::GetMainMode() == MainMode::InPool;
    const int gapUs = g_latencyGapUs;
    g_latUs.clear();
    for (int i = 0; i < kLatencySamples; ++i) {
        // A gap long enough for the workers to park. In-pool main never sleeps: it keeps running
        // whatever work reaches its slot until the gap is over.
        const auto gapEnd = Clock::now() + std::chrono::microseconds(gapUs);
        if (mainInPool) while (Clock::now() < gapEnd) { TaskScheduler::MainWorker(); std::this_thread::yield(); }
        else bench::PreciseSleepUs(gapUs);   // sleep_for would round to the OS tick
        g_startNs.store(0, std::memory_order_release);
        WaitGroup wg;
        const std::int64_t t0 = NowNs();
        if (k == Kind::Fiber) {
            Task* t = S().CreateTask(&LatTask, nullptr, TaskType::Fiber);
            if (mainInPool) { wg.n.store(1); t->waitGroup = &wg; }
            S().Push(t);
        }
        else Spawn(LatCoro(), mainInPool ? &wg : nullptr);
        if (mainInPool) S().WaitFor(wg);
        else while (g_startNs.load(std::memory_order_acquire) == 0) std::this_thread::yield();
        g_latUs.push_back((double)(g_startNs.load(std::memory_order_acquire) - t0) / 1000.0);
    }
    std::sort(g_latUs.begin(), g_latUs.end());
    g_latP50 = g_latUs[g_latUs.size() / 2];
    g_latP99 = g_latUs[g_latUs.size() * 99 / 100];
    return kLatencySamples;
}

// periodic (timers): a 1 ms periodic task for 250 ms; reports fires against the grid.
// The task stops itself after kRunMs and releases the waiter, so main waits in WaitFor (in-pool
// main keeps working) rather than sleeping.
constexpr std::int64_t kPeriodicRunMs = 400;
std::int64_t g_periodicNs = 4'000'000;
std::atomic<std::uint64_t> g_fires{ 0 };
std::int64_t g_periodicEnd = 0;
WaitGroup g_periodicDone;
double g_periodicRatio = 0;
std::uint64_t g_periodicSkipped = 0;
bool OnTick(void*, std::uint64_t) {
    g_fires.fetch_add(1, std::memory_order_relaxed);
    if (NowNs() < g_periodicEnd) return true;
    g_periodicDone.Done();
    return false;
}
std::uint64_t CaseLatencyHot(Kind k)  { g_latencyGapUs = 50;  const auto n = CaseLatency(k); g_latencyGapUs = bench::kLatencyGapUs; return n; }
std::uint64_t CasePeriodicTick(Kind);
std::uint64_t CasePeriodic(Kind) {
    g_fires = 0;
    g_periodicDone.n.store(1);
    const std::int64_t start = NowNs();
    g_periodicEnd = start + kPeriodicRunMs * 1'000'000;
    Periodic p = Periodic::Start(g_periodicNs, &OnTick, nullptr);
    S().WaitFor(g_periodicDone);
    p.Join();
    const double elapsedMs = (double)(NowNs() - start) / 1e6;
    const std::uint64_t fires = g_fires.load();
    g_periodicSkipped = p.Skipped();
    g_periodicRatio = (double)(fires + g_periodicSkipped) * (double)(g_periodicNs / 1000000) / elapsedMs;
    return fires;
}
// The pathological case: an interval equal to the wheel's own tick.
std::uint64_t CasePeriodicTick(Kind k) {
    g_periodicNs = 1'000'000;
    const std::uint64_t n = CasePeriodic(k);
    g_periodicNs = 4'000'000;
    return n;
}

// io (reactor): coroutines reading a temp file.
constexpr int kIoCoros = 64, kIoReads = 64;
void* g_ioHandle = nullptr;
std::atomic<std::uint64_t> g_ioOk{ 0 };
Coro IoReader() {
    char buf[64];
    for (int i = 0; i < kIoReads; ++i) {
        const IoResult r = co_await ReadAsync(g_ioHandle, buf, sizeof buf, (std::uint64_t)(i * 64));
        if (r.Ok()) g_ioOk.fetch_add(1, std::memory_order_relaxed);
    }
}
bool OpenIoFile() {
    if (g_ioHandle) return true;
    std::string path;
#if defined(_WIN32)
    char dir[MAX_PATH];
    ::GetTempPathA(MAX_PATH, dir);
    path = std::string(dir) + "jlib_bench_io.bin";
#else
    path = "/tmp/jlib_bench_io.bin";
#endif
    if (FILE* f = std::fopen(path.c_str(), "wb")) {
        for (int i = 0; i < kIoReads * 64; ++i) std::fputc(i & 0xFF, f);
        std::fclose(f);
    }
#if defined(_WIN32)
    HANDLE h = ::CreateFileA(path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                             FILE_FLAG_OVERLAPPED, nullptr);
    if (h == INVALID_HANDLE_VALUE) return false;
    g_ioHandle = h;
#else
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    g_ioHandle = reinterpret_cast<void*>((std::intptr_t)fd);
#endif
    return IoReactor::Instance().Register(g_ioHandle);
}
std::uint64_t CaseIo(Kind) {
    if (!OpenIoFile()) {
        g_ioOpenFailed = true;   // open fails at warmup, before any timed rep; RunOne skips on this
        std::fprintf(stderr, "io: could not open the temp file\n");
        return 0;
    }
    g_ioOk = 0;
    WaitGroup wg;
    for (int i = 0; i < kIoCoros; ++i) Spawn(IoReader(), &wg);
    S().WaitFor(wg);
    return g_ioOk.load();
}

// ---- harness ---------------------------------------------------------------------------------

struct Case {
    const char* name;
    std::uint64_t (*run)(Kind);
    bool fiber, coro;          // which kinds apply; neither = kind-independent
    bool needsTimers, needsIo;
    const char* what;
};

const Case kCases[] = {
    { "spawn",        &CaseSpawn,       true,  true,  false, false, "200k empty tasks, pushed one by one" },
    { "spawn_lambda", &CaseSpawnLambda, false, false, false, false, "200k empty lambdas, pushed one by one" },
    { "batch_wide",   &CaseBatchWide,   false, false, false, false, "200k empty tasks, PushBatch(Wide) in 1024s" },
    { "batch_narrow", &CaseBatchNarrow, false, false, false, false, "200k empty tasks, PushBatch(Narrow) in 1024s" },
    { "pfor",         &CasePforAuto,    false, false, false, false, "ParallelFor, 65k items x 64 LCG steps, grain n/8w (unit: chunks)" },
    { "pfor_g1",      &CasePforG1,      false, false, false, false, "the same, grain 1" },
    { "pfor_g16",     &CasePforG16,     false, false, false, false, "the same, grain 16" },
    { "pfor_g256",    &CasePforG256,    false, false, false, false, "the same, grain 256" },
    { "range32",      &CaseRange32,     false, false, false, false, "the same body, split by hand into 32 chunks" },
    { "range8w",      &CaseRange8w,     false, false, false, false, "the same body, split by hand into 8 per worker" },
    { "pusharray32",  &CasePushArray32, false, false, false, false, "the same body via PushArray, 32 chunks" },
    { "pusharray8w",  &CasePushArray8w, false, false, false, false, "the same body via PushArray, 8 per worker" },
    { "pusharray_fine", &CasePushArrayFine, false, false, false, false, "200k tiny items via PushArray: one wg add, one batch" },
    { "fib",          &CaseFib,         true,  true,  false, false, "fib(35) fork-join, serial below 20" },
    { "pingpong",     &CasePingPong,    true,  true,  false, false, "256 tasks x 200 waits on a lambda child" },
    { "yield",        &CaseYield,       true,  true,  false, false, "64 tasks x 2000 yields" },
    { "mutex",        &CaseMutex,       true,  true,  false, false, "64 tasks x 2000 contended lock/unlock" },
    { "dag",          &CaseDag,         false, false, false, false, "DAG root -> 4096 -> join, build + run" },
    { "lock_s0",      &CaseLockSusp0,   false, false, false, false, "64 tasks contend: suspending lock, no hold" },
    { "lock_b0",      &CaseLockBlock0,  false, false, false, false, "the same, blocking lock" },
    { "lock_s1",      &CaseLockSusp1,   false, false, false, false, "suspending lock, 1 us hold" },
    { "lock_b1",      &CaseLockBlock1,  false, false, false, false, "blocking lock, 1 us hold" },
    { "lock_s10",     &CaseLockSusp10,  false, false, false, false, "suspending lock, 10 us hold" },
    { "lock_b10",     &CaseLockBlock10, false, false, false, false, "blocking lock, 10 us hold" },
    { "lock_s100",    &CaseLockSusp100, false, false, false, false, "suspending lock, 100 us hold" },
    { "lock_b100",    &CaseLockBlock100,false, false, false, false, "blocking lock, 100 us hold" },
    { "lock_u",       &CaseLockUncont,   false, false, false, false, "UNCONTENDED: 1 task x 2M lock/unlock, SchedulerMutex" },
    { "lock_ub",      &CaseLockUncontStd,false, false, false, false, "the same, std::mutex (the floor)" },
    { "lock_utry",    &CaseLockUncontTry,false, false, false, false, "UNCONTENDED: 1 task x 2M Try_Lock/Unlock" },
    { "mix_susp",     &CaseMixSusp,     false, false, false, false, "64 contenders (100 us hold) + 4000 independent tasks: suspending lock" },
    { "mix_block",    &CaseMixBlock,    false, false, false, false, "the same, blocking lock" },
    { "latency",      &CaseLatency,     true,  true,  false, false, "push-to-start into an idle pool (p50/p99)" },
    { "latency_hot",  &CaseLatencyHot,  true,  true,  false, false, "push-to-start with a 50 us gap: the pool is still hot" },
    { "periodic",     &CasePeriodic,    false, false, true,  false, "4 ms periodic for 400 ms (fires vs grid)" },
    { "periodic_tick",&CasePeriodicTick,false, false, true,  false, "1 ms periodic: the interval IS the wheel tick" },
    { "io",           &CaseIo,          false, true,  true,  true,  "64 coroutines x 64 file reads" },
};

struct Options {
    Mode        mode = Mode::Migrate;
    MainMode    main = MainMode::OutOfPool;
    size_t      threads = 0;
    static constexpr size_t k = 0;   // K is gone; kept so the CSV keeps its column
    bool        timers = false;
    bool        io = false;
    bool        fibers = true, coros = true;
    int         reps = 5;
    bool        stats = false;
    bool        handoff = false;
    bool        matrix = false;
    bool        header = true;
    std::string csv;
    std::vector<std::string> only;
};

void Usage() {
    std::printf(
        "SchedulerBench [options]\n"
        "  --mode migrate|pinned       scheduler Mode (default migrate)\n"
        "  --main outofpool|inpool     MainMode (default outofpool)\n"
        "  --matrix                    run every Mode x MainMode, one process each\n"
        "  --threads N                 pool size (default: the scheduler's choice)\n"
        "  --handoff                   Unlock switches straight into the next waiter (off by default)\n"
        "  --timers                    start the timer thread (enables: periodic)\n"
        "  --io                        start the I/O reactor after Init (enables: io)\n"
        "  --kind fiber|coro|both      which task kind to run where both apply (default both)\n"
        "  --cases a,b,...             only these cases\n"
        "  --reps N                    timed repetitions per case (default 5, after one warmup)\n"
        "  --csv FILE                  append one row per result\n"
        "  --stats                     print the stats snapshot after each case (stats build)\n"
        "  --list                      list the cases\n");
}

bool Want(const Options& o, const char* name) {
    if (o.only.empty()) return true;
    for (const auto& s : o.only) if (s == name) return true;
    return false;
}

std::vector<std::string> Split(const std::string& s) {
    std::vector<std::string> out;
    size_t b = 0;
    while (b <= s.size()) {
        size_t e = s.find(',', b);
        if (e == std::string::npos) e = s.size();
        if (e > b) out.push_back(s.substr(b, e - b));
        b = e + 1;
    }
    return out;
}

const char* ModeName(Mode m) { return m == Mode::Pinned ? "pinned" : "migrate"; }
const char* MainName(MainMode m) { return m == MainMode::InPool ? "inpool" : "outofpool"; }

void RunOne(const Options& o, const Case& c, Kind kind, std::FILE* csv) {
    c.run(kind);   // warmup
    if (c.run == &CaseIo && g_ioOpenFailed) {   // skip with a reason rather than print a 0-ops row
        std::printf("  %-13s skipped (could not open the temp file)\n", c.name);
        return;
    }
    if (o.stats) Stats::Reset();

    std::vector<double> ms;
    std::uint64_t ops = 0;
    for (int r = 0; r < o.reps; ++r) {
        const auto t0 = Clock::now();
        ops = c.run(kind);
        ms.push_back(std::chrono::duration<double, std::milli>(Clock::now() - t0).count());
    }
    std::sort(ms.begin(), ms.end());
    const double med = ms[ms.size() / 2];
    const double opsPerSec = med > 0 ? (double)ops * 1000.0 / med : 0.0;

    char extra[96] = "";
    const bool isPfor = std::strncmp(c.name, "pfor", 4) == 0 || std::strncmp(c.name, "range", 5) == 0
                        || std::strncmp(c.name, "pusharray", 9) == 0;
    if (isPfor) {
        MeasurePforSerial();
        const bool manual = std::strncmp(c.name, "range", 5) == 0 || std::strncmp(c.name, "pusharray", 9) == 0;
        const int grain = manual ? (kPforN + g_rangeChunks - 1) / g_rangeChunks
                                 : (g_pforGrain > 0 ? g_pforGrain : PforAutoGrain());
        g_pforSpeedup = ms[ms.size() / 2] > 0 ? g_pforSerialMs / ms[ms.size() / 2] : 0.0;
        std::snprintf(extra, sizeof extra, "grain %d -> %llu chunks, %.1fx vs serial (%.2f ms)",
                      grain, (unsigned long long)ops, g_pforSpeedup, g_pforSerialMs);
    }
    if (c.run == &CaseLatency || c.run == &CaseLatencyHot)  std::snprintf(extra, sizeof extra, "p50 %.1f us  p99 %.1f us", g_latP50, g_latP99);
    if (c.run == &CaseMixSusp || c.run == &CaseMixBlock)
        std::snprintf(extra, sizeof extra, "independent work done at %.1f ms, all done %.1f ms",
                      g_mixComputeMs, g_mixTotalMs);
    if (c.run == &CasePeriodic || c.run == &CasePeriodicTick) std::snprintf(extra, sizeof extra, "%llu fires, %llu skipped, %.2f/ms",
                                               (unsigned long long)ops, (unsigned long long)g_periodicSkipped, g_periodicRatio);

    const bool rate = c.run != &CaseLatency && c.run != &CaseLatencyHot && c.run != &CasePeriodic && c.run != &CasePeriodicTick;   // those report their own metric
    char opsText[32] = "-";
    if (rate) std::snprintf(opsText, sizeof opsText, "%.0f", opsPerSec);
    std::printf("  %-13s %-6s %12.3f %12.3f %12.3f %14s  %s\n",
                c.name, KindName(kind), med, ms.front(), ms.back(), opsText, extra);

    if (csv) {
        std::fprintf(csv, "%s,%s,%s,%s,%zu,%zu,%d,%d,%s,%s,%llu,%d,%.4f,%.4f,%.4f,%.1f,%.2f,%.2f\n",
                     JLIBSCHED_VERSION_STRING, Stats::Enabled() ? "stats" : "release",
                     ModeName(o.mode), MainName(o.main), S().GetWorkerCount(), Options::k,
                     (int)TaskScheduler::TimersEnabled(), (int)(o.io && IoReactor::IsAvailable()),
                     c.name, KindName(kind), (unsigned long long)ops, o.reps, med, ms.front(), ms.back(),
                     opsPerSec,
                     c.run == &CaseLatency ? g_latP50 : 0.0, c.run == &CaseLatency ? g_latP99 : 0.0);
        std::fflush(csv);
    }
    if (o.stats) Stats::Print();
}

int RunMatrix(int argc, char** argv) {
    std::string self = std::string("\"") + argv[0] + "\"";
    std::string rest;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--matrix") continue;
        if (a == "--mode" || a == "--main") { ++i; continue; }
        rest += " \"" + a + "\"";
    }
    int worst = 0;
    bool first = true;
    for (const char* mode : { "migrate", "pinned" })
        for (const char* main : { "outofpool", "inpool" }) {
            std::string cmd = self + " --mode " + mode + " --main " + main + rest + (first ? "" : " --no-header");
#if defined(_WIN32)
            cmd = "\"" + cmd + "\"";   // cmd.exe strips one outer pair
#endif
            std::fflush(stdout);
            const int rc = std::system(cmd.c_str());
            if (rc != 0) worst = rc;
            first = false;
        }
    return worst;
}

}   // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : std::string(); };
        if (a == "--help" || a == "-h") { Usage(); return 0; }
        else if (a == "--list") {
            for (const Case& c : kCases)
                std::printf("  %-13s %-10s %s%s\n", c.name,
                            c.fiber && c.coro ? "fiber+coro" : c.coro ? "coro" : "-", c.what,
                            c.needsIo ? " [--io]" : c.needsTimers ? " [--timers]" : "");
            return 0;
        }
        else if (a == "--mode")    { const auto v = next(); o.mode = v == "pinned" ? Mode::Pinned : Mode::Migrate; }
        else if (a == "--main")    { const auto v = next(); o.main = v == "inpool" ? MainMode::InPool : MainMode::OutOfPool; }
        else if (a == "--matrix")  o.matrix = true;
        else if (a == "--threads") o.threads = (size_t)std::atoi(next().c_str());
        else if (a == "--handoff") o.handoff = true;
        else if (a == "--timers")  o.timers = true;
        else if (a == "--io")      o.io = true;
        else if (a == "--kind")    { const auto v = next(); o.fibers = v != "coro"; o.coros = v != "fiber"; }
        else if (a == "--cases")   o.only = Split(next());
        else if (a == "--reps")    o.reps = std::max(1, std::atoi(next().c_str()));
        else if (a == "--csv")     o.csv = next();
        else if (a == "--stats")   o.stats = true;
        else if (a == "--no-header") o.header = false;
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); Usage(); return 2; }
    }
    if (o.matrix) return RunMatrix(argc, argv);

    // BEFORE Init: a serial baseline measured next to a live pool is the loop plus whatever the
    // idle pool is doing, which is not what "vs serial" should mean.
    MeasurePforSerial();

    TaskScheduler::Config cfg;
    cfg.mode       = o.mode;
    cfg.main       = o.main;
    cfg.workers    = o.threads;
    cfg.timers     = o.timers;
    // pusharray_fine submits 200k individual chunk tasks and spawn/batch push 200k more. The
    // library's defaults are sized for ordinary use; growing a slab mid-run puts an allocation
    // inside the measurement, so the bench asks for what it actually needs.
    cfg.slab.slots64  = 256 * 1024;
    cfg.slab.slots80  = 256 * 1024;
    cfg.slab.slots512 = 8 * 1024;
    cfg.tunables.lockHandoff = o.handoff;   // --handoff: Unlock switches straight into the waiter
    TaskScheduler::Init(cfg);
    if (o.io && IoReactor::IsAvailable()) IoReactor::Instance().Start();   // I/O is opt-in

    std::FILE* csv = nullptr;
    if (!o.csv.empty()) {
        std::FILE* probe = std::fopen(o.csv.c_str(), "rb");
        bool empty = true;
        if (probe) { empty = std::fgetc(probe) == EOF; std::fclose(probe); }
        csv = std::fopen(o.csv.c_str(), "ab");
        if (csv && empty)
            std::fprintf(csv, "version,build,mode,main,workers,k,timers,io,case,kind,ops,reps,"
                              "median_ms,min_ms,max_ms,ops_per_s,lat_p50_us,lat_p99_us\n");
    }

    const bool timers = TaskScheduler::TimersEnabled();
    const bool io = o.io && IoReactor::IsAvailable();
    if (o.header)
        std::printf("JLib::Scheduler %s bench (%s)\n", JLIBSCHED_VERSION_STRING,
                    Stats::Enabled() ? "stats build -- timings are not comparable with a release build" : "release build");
    std::printf("\nmode=%s main=%s workers=%zu k=%zu timers=%s io=%s\n",
                ModeName(o.mode), MainName(o.main), S().GetWorkerCount(), Options::k,
                timers ? "on" : "off", io ? "on" : "off");
    std::printf("  %-13s %-6s %12s %12s %12s %14s\n", "case", "kind", "median ms", "min ms", "max ms", "ops/s");

    for (const Case& c : kCases) {
        if (!Want(o, c.name)) continue;
        if ((c.needsTimers && !timers) || (c.needsIo && !io)) {
            std::printf("  %-13s skipped (needs %s)\n", c.name, c.needsIo ? "--io" : "--timers");
            continue;
        }
        if (!c.fiber && !c.coro) { RunOne(o, c, Kind::None, csv); continue; }
        if (c.fiber && o.fibers) RunOne(o, c, Kind::Fiber, csv);
        if (c.coro && o.coros)   RunOne(o, c, Kind::Coro, csv);
    }

    if (csv) std::fclose(csv);
    if (g_checkFailed) std::printf("\nrun FAILED: a correctness check did not hold\n");
    std::fflush(stdout);
    std::_Exit(g_checkFailed ? 1 : 0);   // the pool is left running: no teardown to time or to hang on
}
