// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// ONE POOL PER PROCESS. --lib names the library to measure and nothing else is constructed: two
// pools in one process oversubscribe the machine and both arms get slower, and this scheduler
// cannot be torn down and restarted, so alternating arms inside a process is impossible anyway.
// Alternate PROCESSES instead -- tools/compare.ps1 runs A B B A and keeps the medians.
//
// The workload sizes and the reporting come from bench/bench_common.h, so a case here is the same
// case SchedulerBench runs.
//
// WHAT TO EXPECT, so nobody reads a row as a loss that was never a contest.
//
// enkiTS wins ranges. Its range is a STACK-BASED object with tiny allocations, so a parallel-for
// never becomes N heap tasks the way it does here. That is the right design for a game that needs
// task sets and range sets; pfor and fib are calibration against it, not a target.
//
// Cilk wins parallel ALGORITHMS, and is deliberately not an arm: it is a compiler, not a library.
// Its modified cactus stack means there is no task object to pay for at all -- fork/join are stack
// continuations, with work stealing that is provably optimal. No library-level scheduler catches
// that, and a table pretending to measure against it would say nothing.
//
// marl is the true peer: every task runs on a fiber, so pingpong and mutex against it are real.
//
// This scheduler is for TASK GRAPHS IN A GAME -- the FTL shape: dependencies, tasks that suspend
// and resume, pinning, I/O. The rows that speak to that are the ones a range primitive cannot
// express at all: pingpong and mutex (a task suspends and the worker keeps working), and the wake
// latency of an idle pool.
//
// There is deliberately NO bulk case here. enkiTS and Taskflow express bulk work as a RANGE
// (one task set / one for_each_index), where JLib's PushBatch submits N independent tasks: the
// two are different operations and the range form reads as billions of "tasks" per second. Range
// submission is what pfor measures; PushBatch Wide vs Narrow lives in SchedulerBench, where it is
// compared against this scheduler's own Push.
//
//   SchedulerCompare --lib jlib|marl|enki|taskflow [--threads N] [--cases a,b] [--reps N] [--csv f]

#include "../bench_common.h"

#include <TaskScheduler.h>
#include <Thread.h>

#if defined(JLIBSCHED_HAVE_MARL)
#include <marl/scheduler.h>
#include <marl/waitgroup.h>
#include <marl/mutex.h>
#include <marl/conditionvariable.h>
#endif
#if defined(JLIBSCHED_HAVE_ENKI)
#include <TaskScheduler_enki.h>
#endif
#if defined(JLIBSCHED_HAVE_TASKFLOW)
#include <taskflow/taskflow.hpp>
#include <taskflow/algorithm/for_each.hpp>
#endif

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#if !defined(JLIBSCHED_VERSION_STRING)
#define JLIBSCHED_VERSION_STRING "unknown"
#endif

using namespace bench;

namespace {

enum class Lib { JLib, Marl, Enki, Taskflow };

const char* LibName(Lib l) {
    switch (l) {
        case Lib::JLib:     return "jlib";
        case Lib::Marl:     return "marl";
        case Lib::Enki:     return "enki";
        default:            return "taskflow";
    }
}

size_t g_threads = 0;
std::vector<std::uint64_t> g_out;

// pfor is measured in CHUNKS -- the grain pieces a library actually runs -- so it is in the same
// units as bulk. One body call is one chunk, counted rather than derived: a library may chunk
// through a cursor and ignore the grain it was given. Every arm uses the same grain rule and the
// same body, and is read against the same serial baseline.
std::atomic<std::uint64_t> g_chunks{ 0 };
double g_pforSerialMs = 0;     // measured with NO pool alive: the honest denominator
double g_pforWithPoolMs = 0;   // the same loop with the pool up and idle: a diagnostic, not a baseline
int PforGrain(size_t workers) {
    const int g = (int)((size_t)kPforCoarseN / (8 * (workers ? workers : 1)));
    return g < 1 ? 1 : g;
}
void PforBody(int b, int e) {
    g_chunks.fetch_add(1, std::memory_order_relaxed);
    for (int i = b; i < e; ++i) g_out[i] = LcgSteps((std::uint64_t)i, kPforCoarseIters);
}
// The body in one thread, median of three. This must run BEFORE any pool is constructed: the same
// loop measured next to a live pool is not a baseline, it is the loop plus whatever that pool does
// while idle -- and the arms differ there (this scheduler keeps a hunter spinning by design, the
// others park). Measuring it after the pool made JLib's "serial" 5x the others' and turned every
// speedup into an artifact of the denominator.
// MIN of several runs after a warmup, not a median: the fastest run is the one least disturbed by
// frequency ramp, a migration onto a slower core, or another process. A median keeps that noise in
// the denominator, and on a P/E machine that alone moved this number by ~40%.
double MeasureSerialOnce() {
    PforBody(0, kPforCoarseN);   // warmup: page state and clocks
    double best = 1e300;
    for (int r = 0; r < 7; ++r) {
        const auto t0 = Clock::now();
        PforBody(0, kPforCoarseN);
        const double ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
        if (ms < best) best = ms;
    }
    g_chunks.store(0, std::memory_order_relaxed);
    return best;
}
void MeasurePforSerial() { if (g_pforSerialMs <= 0) g_pforSerialMs = MeasureSerialOnce(); }

// ---- JLib ------------------------------------------------------------------------------------

std::uint64_t JlibSpawn() {
    auto& s = JLib::TaskScheduler::Instance();
    JLib::WaitGroup wg;
    wg.n.store(kSpawnN);
    for (int i = 0; i < kSpawnN; ++i) {
        JLib::Task* t = s.CreateTask([] {});
        t->waitGroup = &wg;
        s.Push(t);
    }
    s.WaitFor(wg);
    return kSpawnN;
}

std::uint64_t JlibBulk() {
    auto& s = JLib::TaskScheduler::Instance();
    std::vector<JLib::Task*> tasks(kBatchChunk);
    JLib::WaitGroup wg;
    wg.n.store(kSpawnN);
    for (int done = 0; done < kSpawnN; done += kBatchChunk) {
        const int n = std::min(kBatchChunk, kSpawnN - done);
        for (int i = 0; i < n; ++i) {
            const int idx = done + i;
            tasks[i] = s.CreateTask([idx] { g_out[idx] = (std::uint64_t)idx; });
            tasks[i]->waitGroup = &wg;
        }
        s.PushBatch(tasks.data(), (size_t)n, JLib::TaskScheduler::BatchSpread::Wide);
    }
    s.WaitFor(wg);
    return kSpawnN;
}

std::uint64_t JlibPfor() {
    auto& s = JLib::TaskScheduler::Instance();
    g_chunks.store(0, std::memory_order_relaxed);
    s.ParallelFor(0, kPforCoarseN, PforGrain(s.GetWorkerCount()),
                  [](int b, int e) { PforBody(b, e); });
    return g_chunks.load(std::memory_order_relaxed);
}

struct JlibFibArgs { int n; std::uint64_t result; };
void JlibFibTask(void* p) {
    JlibFibArgs* a = static_cast<JlibFibArgs*>(p);
    if (a->n <= kFibCutoff) { a->result = FibSerial(a->n); return; }
    auto& s = JLib::TaskScheduler::Instance();
    JlibFibArgs x{ a->n - 1, 0 }, y{ a->n - 2, 0 };
    JLib::WaitGroup wg;
    wg.n.store(2);
    JLib::Task* tx = s.CreateTask(&JlibFibTask, &x); tx->waitGroup = &wg; s.Push(tx);
    JLib::Task* ty = s.CreateTask(&JlibFibTask, &y); ty->waitGroup = &wg; s.Push(ty);
    s.WaitFor(wg);
    a->result = x.result + y.result;
}
std::uint64_t JlibFib() {
    auto& s = JLib::TaskScheduler::Instance();
    JlibFibArgs a{ kFibN, 0 };
    JLib::WaitGroup wg;
    wg.n.store(1);
    JLib::Task* t = s.CreateTask(&JlibFibTask, &a);
    t->waitGroup = &wg;
    s.Push(t);
    s.WaitFor(wg);
    if (a.result != kFibExpected) std::fprintf(stderr, "jlib fib: wrong result\n");
    return 1;
}

void JlibPingTask(void*) {
    auto& s = JLib::TaskScheduler::Instance();
    for (int i = 0; i < kPingIters; ++i) {
        JLib::WaitGroup wg;
        wg.n.store(1);
        JLib::Task* t = s.CreateTask([] {});
        t->waitGroup = &wg;
        s.Push(t);
        s.WaitFor(wg);
    }
}
std::uint64_t JlibPingPong() {
    auto& s = JLib::TaskScheduler::Instance();
    JLib::WaitGroup wg;
    wg.n.store(kPingTasks);
    for (int i = 0; i < kPingTasks; ++i) {
        JLib::Task* t = s.CreateTask(&JlibPingTask, nullptr);
        t->waitGroup = &wg;
        s.Push(t);
    }
    s.WaitFor(wg);
    return (std::uint64_t)kPingTasks * kPingIters;
}

JLib::SchedulerMutex g_jlibMutex;
std::uint64_t g_mutexCount = 0;
void JlibMutexTask(void*) {
    for (int i = 0; i < kMutexIters; ++i) { g_jlibMutex.Lock(); ++g_mutexCount; g_jlibMutex.Unlock(); }
}
std::uint64_t JlibMutex() {
    auto& s = JLib::TaskScheduler::Instance();
    g_mutexCount = 0;
    JLib::WaitGroup wg;
    wg.n.store(kMutexTasks);
    for (int i = 0; i < kMutexTasks; ++i) {
        JLib::Task* t = s.CreateTask(&JlibMutexTask, nullptr);
        t->waitGroup = &wg;
        s.Push(t);
    }
    s.WaitFor(wg);
    return (std::uint64_t)kMutexTasks * kMutexIters;
}

std::mutex g_stdMutex;
void JlibMutexBlockingTask(void*) {
    for (int i = 0; i < kMutexIters; ++i) { std::lock_guard<std::mutex> l(g_stdMutex); ++g_mutexCount; }
}
std::uint64_t JlibMutexBlocking() {   // a std::mutex inside a task: the waiter blocks the worker
    auto& s = JLib::TaskScheduler::Instance();
    g_mutexCount = 0;
    JLib::WaitGroup wg;
    wg.n.store(kMutexTasks);
    for (int i = 0; i < kMutexTasks; ++i) {
        JLib::Task* t = s.CreateTask(&JlibMutexBlockingTask, nullptr);
        t->waitGroup = &wg;
        s.Push(t);
    }
    s.WaitFor(wg);
    return (std::uint64_t)kMutexTasks * kMutexIters;
}

std::atomic<std::int64_t> g_startNs{ 0 };
void LatStamp() { g_startNs.store(NowNs(), std::memory_order_release); }

std::vector<double> g_latUs;
double g_latP50 = 0, g_latP99 = 0;
void LatencyFinish() {
    std::sort(g_latUs.begin(), g_latUs.end());
    g_latP50 = g_latUs[g_latUs.size() / 2];
    g_latP99 = g_latUs[g_latUs.size() * 99 / 100];
}

std::uint64_t JlibLatency() {
    auto& s = JLib::TaskScheduler::Instance();
    g_latUs.clear();
    for (int i = 0; i < kLatencySamples; ++i) {
        PreciseSleepUs(kLatencyGapUs);   // sleep_for would round up to the OS timer tick
        g_startNs.store(0, std::memory_order_release);
        const std::int64_t t0 = NowNs();
        s.Push(s.CreateTask([] { LatStamp(); }));
        while (g_startNs.load(std::memory_order_acquire) == 0) std::this_thread::yield();
        g_latUs.push_back((double)(g_startNs.load(std::memory_order_acquire) - t0) / 1000.0);
    }
    LatencyFinish();
    return kLatencySamples;
}

// ---- marl ------------------------------------------------------------------------------------
#if defined(JLIBSCHED_HAVE_MARL)

std::uint64_t MarlSpawn() {
    marl::WaitGroup wg(kSpawnN);
    for (int i = 0; i < kSpawnN; ++i) marl::schedule([wg] { wg.done(); });
    wg.wait();
    return kSpawnN;
}

std::uint64_t MarlPfor() {
    const int grain = PforGrain(g_threads);
    const int chunks = (kPforCoarseN + grain - 1) / grain;
    g_chunks.store(0, std::memory_order_relaxed);
    marl::WaitGroup wg(chunks);
    for (int c = 0; c < chunks; ++c) {
        const int b = c * grain;
        const int e = std::min(b + grain, kPforCoarseN);
        marl::schedule([wg, b, e] { PforBody(b, e); wg.done(); });
    }
    wg.wait();
    return g_chunks.load(std::memory_order_relaxed);
}

void MarlFibRec(int n, std::uint64_t* out) {
    if (n <= kFibCutoff) { *out = FibSerial(n); return; }
    std::uint64_t a = 0, b = 0;
    marl::WaitGroup wg(2);
    marl::schedule([wg, n, &a] { MarlFibRec(n - 1, &a); wg.done(); });
    marl::schedule([wg, n, &b] { MarlFibRec(n - 2, &b); wg.done(); });
    wg.wait();
    *out = a + b;
}
std::uint64_t MarlFibCase() {
    std::uint64_t result = 0;
    marl::WaitGroup wg(1);
    marl::schedule([wg, &result] { MarlFibRec(kFibN, &result); wg.done(); });
    wg.wait();
    if (result != kFibExpected) std::fprintf(stderr, "marl fib: wrong result\n");
    return 1;
}

std::uint64_t MarlPingPong() {
    marl::WaitGroup all(kPingTasks);
    for (int t = 0; t < kPingTasks; ++t) {
        marl::schedule([all] {
            for (int i = 0; i < kPingIters; ++i) {
                marl::WaitGroup one(1);
                marl::schedule([one] { one.done(); });
                one.wait();
            }
            all.done();
        });
    }
    all.wait();
    return (std::uint64_t)kPingTasks * kPingIters;
}

marl::mutex g_marlMutex;

// marl SHIPS ONLY A BLOCKING MUTEX. marl::mutex holds a std::mutex, so its waiter blocks the WORKER
// THREAD; calling any marl lock "async" would be wrong. Its fiber-aware primitives are Event,
// WaitGroup and ConditionVariable (that last one keeps a list of Scheduler::Fiber* and suspends
// them), so the lock below is one BUILT HERE from those primitives purely to give SchedulerMutex a
// like-for-like row. No marl user would write it, and marl should not be described as having it.
//
// What a suspending lock is FOR: a longer wait with other work available. It cannot win on a short
// uncontended hold (one CAS vs suspend+wake), and it is not meant to -- see mix_susp/mix_block in
// SchedulerBench, where the rest of the work finishes 69x sooner while the lock is held.
marl::mutex             g_marlCvMutex;
marl::ConditionVariable g_marlCv;
bool g_marlHeld = false;

void MarlFiberLock() {
    marl::lock lock(g_marlCvMutex);
    g_marlCv.wait(lock, [] { return !g_marlHeld; });
    g_marlHeld = true;
}
void MarlFiberUnlock() {
    { marl::lock lock(g_marlCvMutex); g_marlHeld = false; }
    g_marlCv.notify_one();
}

std::uint64_t MarlMutex() {           // fiber-aware: the waiter suspends
    g_mutexCount = 0;
    marl::WaitGroup wg(kMutexTasks);
    for (int t = 0; t < kMutexTasks; ++t) {
        marl::schedule([wg] {
            for (int i = 0; i < kMutexIters; ++i) {
                MarlFiberLock();
                ++g_mutexCount;
                MarlFiberUnlock();
            }
            wg.done();
        });
    }
    wg.wait();
    return (std::uint64_t)kMutexTasks * kMutexIters;
}

std::uint64_t MarlMutexBlocking() {   // marl::mutex: the waiter blocks the worker thread
    g_mutexCount = 0;
    marl::WaitGroup wg(kMutexTasks);
    for (int t = 0; t < kMutexTasks; ++t) {
        marl::schedule([wg] {
            for (int i = 0; i < kMutexIters; ++i) {
                marl::lock lock(g_marlMutex);
                ++g_mutexCount;
            }
            wg.done();
        });
    }
    wg.wait();
    return (std::uint64_t)kMutexTasks * kMutexIters;
}

std::uint64_t MarlLatency() {
    g_latUs.clear();
    for (int i = 0; i < kLatencySamples; ++i) {
        PreciseSleepUs(kLatencyGapUs);   // sleep_for would round up to the OS timer tick
        g_startNs.store(0, std::memory_order_release);
        const std::int64_t t0 = NowNs();
        marl::schedule([] { LatStamp(); });
        while (g_startNs.load(std::memory_order_acquire) == 0) std::this_thread::yield();
        g_latUs.push_back((double)(g_startNs.load(std::memory_order_acquire) - t0) / 1000.0);
    }
    LatencyFinish();
    return kLatencySamples;
}
#endif

// ---- enkiTS ----------------------------------------------------------------------------------
#if defined(JLIBSCHED_HAVE_ENKI)

enki::TaskScheduler* g_enki = nullptr;

struct EnkiEmpty : enki::ITaskSet {
    EnkiEmpty() { m_SetSize = 1; }
    void ExecuteRange(enki::TaskSetPartition, std::uint32_t) override {}
};

std::uint64_t EnkiSpawn() {
    std::vector<EnkiEmpty> sets(kSpawnN);
    for (int i = 0; i < kSpawnN; ++i) g_enki->AddTaskSetToPipe(&sets[i]);
    for (int i = 0; i < kSpawnN; ++i) g_enki->WaitforTask(&sets[i]);
    return kSpawnN;
}

// enkiTS's own shape for bulk: one task set of N items, split across workers.
struct EnkiBulk : enki::ITaskSet {
    explicit EnkiBulk(std::uint32_t n) { m_SetSize = n; }
    void ExecuteRange(enki::TaskSetPartition r, std::uint32_t) override {
        for (std::uint32_t i = r.start; i < r.end; ++i) g_out[i] = (std::uint64_t)i;
    }
};
std::uint64_t EnkiBulk_() {
    EnkiBulk bulk((std::uint32_t)kSpawnN);
    g_enki->AddTaskSetToPipe(&bulk);
    g_enki->WaitforTask(&bulk);
    return kSpawnN;
}

struct EnkiRange : enki::ITaskSet {
    EnkiRange(std::uint32_t n, std::uint32_t grain) { m_SetSize = n; m_MinRange = grain; }
    void ExecuteRange(enki::TaskSetPartition r, std::uint32_t) override {
        PforBody((int)r.start, (int)r.end);
    }
};
std::uint64_t EnkiPfor() {
    g_chunks.store(0, std::memory_order_relaxed);
    EnkiRange range((std::uint32_t)kPforCoarseN, (std::uint32_t)PforGrain(g_threads));
    g_enki->AddTaskSetToPipe(&range);
    g_enki->WaitforTask(&range);
    return g_chunks.load(std::memory_order_relaxed);
}

// WaitforTask runs other tasks while it waits, so a nested wait does not stall the worker.
struct EnkiFib : enki::ITaskSet {
    int n; std::uint64_t out = 0;
    explicit EnkiFib(int n_) : n(n_) { m_SetSize = 1; }
    void ExecuteRange(enki::TaskSetPartition, std::uint32_t) override {
        if (n <= kFibCutoff) { out = FibSerial(n); return; }
        EnkiFib a(n - 1), b(n - 2);
        g_enki->AddTaskSetToPipe(&a);
        g_enki->AddTaskSetToPipe(&b);
        g_enki->WaitforTask(&a);
        g_enki->WaitforTask(&b);
        out = a.out + b.out;
    }
};
std::uint64_t EnkiFibCase() {
    EnkiFib f(kFibN);
    g_enki->AddTaskSetToPipe(&f);
    g_enki->WaitforTask(&f);
    if (f.out != kFibExpected) std::fprintf(stderr, "enki fib: wrong result\n");
    return 1;
}

struct EnkiStamp : enki::ITaskSet {
    EnkiStamp() { m_SetSize = 1; }
    void ExecuteRange(enki::TaskSetPartition, std::uint32_t) override { LatStamp(); }
};
std::uint64_t EnkiLatency() {
    g_latUs.clear();
    for (int i = 0; i < kLatencySamples; ++i) {
        PreciseSleepUs(kLatencyGapUs);   // sleep_for would round up to the OS timer tick
        g_startNs.store(0, std::memory_order_release);
        EnkiStamp stamp;
        const std::int64_t t0 = NowNs();
        g_enki->AddTaskSetToPipe(&stamp);
        while (g_startNs.load(std::memory_order_acquire) == 0) std::this_thread::yield();
        g_latUs.push_back((double)(g_startNs.load(std::memory_order_acquire) - t0) / 1000.0);
        g_enki->WaitforTask(&stamp);
    }
    LatencyFinish();
    return kLatencySamples;
}
#endif

// ---- Taskflow --------------------------------------------------------------------------------
#if defined(JLIBSCHED_HAVE_TASKFLOW)

tf::Executor* g_tf = nullptr;

std::uint64_t TfSpawn() {
    for (int i = 0; i < kSpawnN; ++i) g_tf->silent_async([] {});
    g_tf->wait_for_all();
    return kSpawnN;
}

std::uint64_t TfBulk() {
    tf::Taskflow flow;
    flow.for_each_index(0, kSpawnN, 1, [](int i) { g_out[i] = (std::uint64_t)i; });
    g_tf->run(flow).wait();
    return kSpawnN;
}

std::uint64_t TfPfor() {
    const int grain = PforGrain(g_threads);
    g_chunks.store(0, std::memory_order_relaxed);
    tf::Taskflow flow;
    for (int b = 0; b < kPforCoarseN; b += grain) {
        const int e = std::min(b + grain, kPforCoarseN);
        flow.emplace([b, e] { PforBody(b, e); });
    }
    g_tf->run(flow).wait();
    return g_chunks.load(std::memory_order_relaxed);
}

std::uint64_t TfFib(int n, tf::Subflow& sbf) {
    if (n <= kFibCutoff) return FibSerial(n);
    std::uint64_t a = 0, b = 0;
    sbf.emplace([&a, n](tf::Subflow& s) { a = TfFib(n - 1, s); });
    sbf.emplace([&b, n](tf::Subflow& s) { b = TfFib(n - 2, s); });
    sbf.join();
    return a + b;
}
std::uint64_t TfFibCase() {
    std::uint64_t result = 0;
    tf::Taskflow flow;
    flow.emplace([&result](tf::Subflow& sbf) { result = TfFib(kFibN, sbf); });
    g_tf->run(flow).wait();
    if (result != kFibExpected) std::fprintf(stderr, "taskflow fib: wrong result\n");
    return 1;
}

std::uint64_t TfLatency() {
    g_latUs.clear();
    for (int i = 0; i < kLatencySamples; ++i) {
        PreciseSleepUs(kLatencyGapUs);   // sleep_for would round up to the OS timer tick
        g_startNs.store(0, std::memory_order_release);
        const std::int64_t t0 = NowNs();
        g_tf->silent_async([] { LatStamp(); });
        while (g_startNs.load(std::memory_order_acquire) == 0) std::this_thread::yield();
        g_latUs.push_back((double)(g_startNs.load(std::memory_order_acquire) - t0) / 1000.0);
    }
    g_tf->wait_for_all();
    LatencyFinish();
    return kLatencySamples;
}
#endif

// ---- driver ----------------------------------------------------------------------------------

struct CaseDef {
    const char* name;
    std::uint64_t (*jlib)();
    std::uint64_t (*marl)();
    std::uint64_t (*enki)();
    std::uint64_t (*tf)();
    bool rate;
    const char* note;   // why an arm is missing, printed as n/a
};

std::uint64_t Missing() { return 0; }

const CaseDef kCases[] = {
#if defined(JLIBSCHED_HAVE_MARL)
    { "spawn", &JlibSpawn, &MarlSpawn,
#else
    { "spawn", &JlibSpawn, nullptr,
#endif
#if defined(JLIBSCHED_HAVE_ENKI)
      &EnkiSpawn,
#else
      nullptr,
#endif
#if defined(JLIBSCHED_HAVE_TASKFLOW)
      &TfSpawn,
#else
      nullptr,
#endif
      true, "" },

    { "pfor", &JlibPfor,
#if defined(JLIBSCHED_HAVE_MARL)
      &MarlPfor,
#else
      nullptr,
#endif
#if defined(JLIBSCHED_HAVE_ENKI)
      &EnkiPfor,
#else
      nullptr,
#endif
#if defined(JLIBSCHED_HAVE_TASKFLOW)
      &TfPfor,
#else
      nullptr,
#endif
      true, "" },

    { "fib", &JlibFib,
#if defined(JLIBSCHED_HAVE_MARL)
      &MarlFibCase,
#else
      nullptr,
#endif
#if defined(JLIBSCHED_HAVE_ENKI)
      &EnkiFibCase,
#else
      nullptr,
#endif
#if defined(JLIBSCHED_HAVE_TASKFLOW)
      &TfFibCase,
#else
      nullptr,
#endif
      false, "" },

    { "pingpong", &JlibPingPong,
#if defined(JLIBSCHED_HAVE_MARL)
      &MarlPingPong,
#else
      nullptr,
#endif
      nullptr, nullptr, true, "needs fibers: a blocking wait would stall a worker" },

    { "lock_suspend", &JlibMutex,
#if defined(JLIBSCHED_HAVE_MARL)
      &MarlMutex,
#else
      nullptr,
#endif
      nullptr, nullptr, true, "needs a lock whose waiter suspends (marl ships no such lock)" },

    { "lock_block", &JlibMutexBlocking,
#if defined(JLIBSCHED_HAVE_MARL)
      &MarlMutexBlocking,
#else
      nullptr,
#endif
      nullptr, nullptr, true, "a blocking lock inside a task (the waiter stalls its worker)" },


    { "latency", &JlibLatency,
#if defined(JLIBSCHED_HAVE_MARL)
      &MarlLatency,
#else
      nullptr,
#endif
#if defined(JLIBSCHED_HAVE_ENKI)
      &EnkiLatency,
#else
      nullptr,
#endif
#if defined(JLIBSCHED_HAVE_TASKFLOW)
      &TfLatency,
#else
      nullptr,
#endif
      false, "" },
};

std::uint64_t (*Fn(const CaseDef& c, Lib lib))() {
    switch (lib) {
        case Lib::JLib:     return c.jlib;
        case Lib::Marl:     return c.marl;
        case Lib::Enki:     return c.enki;
        default:            return c.tf;
    }
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

}   // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    Lib lib = Lib::JLib;
    int reps = 5;
    std::string csvPath;
    std::vector<std::string> only;
    bool header = true;

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : std::string(); };
        if (a == "--lib") {
            const std::string v = next();
            if (v == "jlib") lib = Lib::JLib;
            else if (v == "marl") lib = Lib::Marl;
            else if (v == "enki") lib = Lib::Enki;
            else if (v == "taskflow") lib = Lib::Taskflow;
            else { std::fprintf(stderr, "unknown --lib %s\n", v.c_str()); return 2; }
        }
        else if (a == "--threads") g_threads = (size_t)std::atoi(next().c_str());
        else if (a == "--cases")   only = Split(next());
        else if (a == "--reps")    reps = std::max(1, std::atoi(next().c_str()));
        else if (a == "--csv")     csvPath = next();
        else if (a == "--no-header") header = false;
        else if (a == "--help") {
            std::printf("SchedulerCompare --lib jlib|marl|enki|taskflow [--threads N] [--cases a,b]"
                        " [--reps N] [--csv FILE]\n"
                        "One pool per process: only the named library is constructed.\n");
            return 0;
        }
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); return 2; }
    }

#if !defined(JLIBSCHED_HAVE_MARL)
    if (lib == Lib::Marl) { std::fprintf(stderr, "built without marl (-DJLIBSCHED_MARL_DIR=...)\n"); return 2; }
#endif
#if !defined(JLIBSCHED_HAVE_ENKI)
    if (lib == Lib::Enki) { std::fprintf(stderr, "built without enkiTS (-DJLIBSCHED_ENKITS_DIR=...)\n"); return 2; }
#endif
#if !defined(JLIBSCHED_HAVE_TASKFLOW)
    if (lib == Lib::Taskflow) { std::fprintf(stderr, "built without Taskflow (-DJLIBSCHED_TASKFLOW_DIR=...)\n"); return 2; }
#endif

    g_out.assign(kPforN, 0);
    MeasurePforSerial();   // no pool exists yet -- this is the denominator every arm is read against

    // Exactly one pool, sized the way this scheduler sizes itself: hardware threads MINUS ONE,
    // because the calling thread is the other one. Every arm gets the same count, so nothing is
    // measured oversubscribed.
    size_t workers = g_threads;
    if (workers == 0) {
        const unsigned hw = std::thread::hardware_concurrency();
        workers = hw > 1 ? hw - 1 : 1;
    }
    g_threads = workers;   // the grain rule needs the real worker count

    std::unique_ptr<std::thread> keepAlive;
    if (lib == Lib::JLib) {
        JLib::TaskScheduler::Init(JLib::Mode::Migrate, JLib::MainMode::OutOfPool, workers);
        workers = JLib::TaskScheduler::Instance().GetWorkerCount();
    }
#if defined(JLIBSCHED_HAVE_MARL)
    marl::Scheduler* marlSched = nullptr;
    if (lib == Lib::Marl) {
        marl::Scheduler::Config cfg;
        cfg.setWorkerThreadCount((int)workers);
        marlSched = new marl::Scheduler(cfg);
        marlSched->bind();
    }
#endif
#if defined(JLIBSCHED_HAVE_ENKI)
    enki::TaskScheduler enkiSched;
    if (lib == Lib::Enki) {
        enki::TaskSchedulerConfig cfg;
        cfg.numTaskThreadsToCreate = (std::uint32_t)workers;
        enkiSched.Initialize(cfg);
        g_enki = &enkiSched;
    }
#endif
#if defined(JLIBSCHED_HAVE_TASKFLOW)
    std::unique_ptr<tf::Executor> tfExec;
    if (lib == Lib::Taskflow) {
        tfExec = std::make_unique<tf::Executor>(workers);
        g_tf = tfExec.get();
    }
#endif

    if (header)
        std::printf("JLib::Scheduler %s comparison -- one pool per process\n", JLIBSCHED_VERSION_STRING);
    std::printf("\nlib=%s workers=%zu (+ the calling thread)\n", LibName(lib), workers);
    g_pforWithPoolMs = MeasureSerialOnce();   // the same loop, now with the pool up and idle
    std::printf("serial baseline %.2f ms (no pool), %.2f ms with the pool idle\n",
                g_pforSerialMs, g_pforWithPoolMs);
    PrintHeader();

    Csv csv;
    csv.Open(csvPath, "version,lib,workers,case,ops,reps,median_ms,min_ms,max_ms,ops_per_s,"
                      "lat_p50_us,lat_p99_us");

    for (const CaseDef& c : kCases) {
        if (!only.empty() && std::find(only.begin(), only.end(), c.name) == only.end()) continue;
        auto fn = Fn(c, lib);
        if (!fn) { PrintSkip(c.name, c.note && *c.note ? c.note : "not built"); continue; }

        std::uint64_t ops = 0;
        const Timing t = TimeReps(reps, fn, ops);
        char extra[96] = "";
        const bool isLatency = std::strcmp(c.name, "latency") == 0;
        if (std::strcmp(c.name, "pfor") == 0) {
            MeasurePforSerial();
            std::snprintf(extra, sizeof extra, "grain %d -> %llu chunks, %.1fx vs serial (%.2f ms)",
                          PforGrain(workers), (unsigned long long)ops,
                          t.Median() > 0 ? g_pforSerialMs / t.Median() : 0.0, g_pforSerialMs);
        }
        if (isLatency) std::snprintf(extra, sizeof extra, "p50 %.1f us  p99 %.1f us", g_latP50, g_latP99);
        if (std::strcmp(c.name, "fib") == 0) std::snprintf(extra, sizeof extra, "fib(%d), serial below %d", kFibN, kFibCutoff);
        PrintRow(c.name, "-", t, ops, c.rate, extra);

        if (csv) {
            std::fprintf(csv.File(), "%s,%s,%zu,%s,%llu,%d,%.4f,%.4f,%.4f,%.1f,%.2f,%.2f\n",
                         JLIBSCHED_VERSION_STRING, LibName(lib), workers, c.name,
                         (unsigned long long)ops, reps, t.Median(), t.Min(), t.Max(),
                         c.rate && t.Median() > 0 ? (double)ops * 1000.0 / t.Median() : 0.0,
                         isLatency ? g_latP50 : 0.0, isLatency ? g_latP99 : 0.0);
            csv.Flush();
        }
    }

#if defined(JLIBSCHED_HAVE_MARL)
    if (lib == Lib::Marl && marlSched) marl::Scheduler::unbind();
#endif
    std::fflush(stdout);
    std::_Exit(0);   // no teardown to time, and this scheduler does not restart anyway
}
