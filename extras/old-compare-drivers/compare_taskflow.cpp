// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <TaskScheduler.h>
#include <taskflow/taskflow.hpp>

#include <taskflow/algorithm/for_each.hpp>

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <vector>
#include <atomic>
#include <thread>
#include <algorithm>

using Clock = std::chrono::steady_clock;
static double Ms(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

static inline uint64_t Spin(uint64_t seed, int iters) {
    uint64_t x = seed | 1;
    for (int i = 0; i < iters; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; }
    return x;
}
static std::atomic<uint64_t> g_sink{ 0 };

static double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}
static double MedOr(const std::vector<double>& v) { return v.empty() ? -1.0 : Median(v); }
static void Report(const char* who, const std::vector<double>& runs, const char* unit) {
    if (runs.empty()) { printf("    %-16s %9s\n", who, "--"); return; }
    const double med = Median(runs);
    const double lo = *std::min_element(runs.begin(), runs.end());
    const double hi = *std::max_element(runs.begin(), runs.end());
    printf("    %-16s %9.3f %-2s  (%.3f to %.3f, %3.0f%% spread)\n",
           who, med, unit, lo, hi, med > 0 ? 100.0 * (hi - lo) / med : 0.0);
}

static bool g_doJ = true;
static bool g_doT = true;

static void Cell(double v, int width, int prec = 1) {
    if (v < 0) printf(" %*s", width, "--");
    else       printf(" %*.*f", width, prec, v);
}

static void BenchLatency(JLib::TaskScheduler& jl, tf::Executor& ex) {
    constexpr int kPings = 20000;
    printf("  round-trip latency (%d serial submit-and-wait round-trips)\n", kPings);

    std::vector<double> a, b;
    if (g_doJ) {
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < kPings; ++i) {
                JLib::WaitGroup wg;
                wg.n.store(1, std::memory_order_relaxed);
                JLib::Task* t = jl.CreateTask(+[](void*) {
                    g_sink.fetch_add(1, std::memory_order_relaxed);
                }, nullptr);
                if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
                t->waitGroup = &wg;
                jl.Push(t);
                jl.WaitFor(wg);
            }
            a.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
        }
    }
    if (g_doT) {
        
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < kPings; ++i) {
                auto fu = ex.async([] { g_sink.fetch_add(1, std::memory_order_relaxed); });
                fu.get();
            }
            b.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
        }
    }
    Report("JLib", a, "us");
    Report("Taskflow", b, "us");
    printf("\n");
}

static constexpr int kRuns      = 7;
static constexpr int kWorkIters = 200;

static void BenchThroughput(JLib::TaskScheduler& jl, tf::Executor& ex) {
    printf("  independent-task throughput -- ns per task, lower is better\n\n");
    printf("     %8s %11s %11s %12s %13s\n",
           "tasks", "JLib Push", "JLib Batch", "JLib arr/32", "TF silent_async");

    const int counts[] = { 256, 1024, 8192, 20000 };
    std::vector<JLib::Task*> batch;
    for (int n : counts) {
        std::vector<double> a, ab, pa, tfv;

        if (g_doJ) {
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg; wg.n.store(n, std::memory_order_relaxed);
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    JLib::Task* t = jl.CreateTask(+[](void* p) {
                        g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                    }, (void*)(intptr_t)i);
                    if (!t) return;
                    t->waitGroup = &wg; jl.Push(t);
                }
                jl.WaitFor(wg);
                a.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            batch.resize(n);
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg; wg.n.store(n, std::memory_order_relaxed);
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    batch[i] = jl.CreateTask(+[](void* p) {
                        g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                    }, (void*)(intptr_t)i);
                    if (!batch[i]) return;
                    batch[i]->waitGroup = &wg;
                }
                jl.PushBatch(batch.data(), (size_t)n);
                jl.WaitFor(wg);
                ab.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg;
                auto t0 = Clock::now();
                jl.PushArray(0, (size_t)n, 32, [](size_t i) {
                    g_sink.fetch_add(Spin((uint64_t)i, kWorkIters), std::memory_order_relaxed);
                }, &wg);
                jl.WaitFor(wg);
                pa.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
        }
        if (g_doT) {
            for (int r = 0; r < kRuns; ++r) {
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i)
                    ex.silent_async([i] {
                        g_sink.fetch_add(Spin((uint64_t)i, kWorkIters), std::memory_order_relaxed);
                    });
                ex.wait_for_all();
                tfv.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
        }

        printf("     %8d", n);
        Cell(MedOr(a), 11); Cell(MedOr(ab), 11); Cell(MedOr(pa), 12); Cell(MedOr(tfv), 13);
        printf("\n");
    }
    printf("\n");
}

static void BenchBulk(JLib::TaskScheduler& jl, tf::Executor& ex) {
    constexpr int kItems = 20000;
    printf("  bulk parallel-for (%d items, native shape on both sides)\n", kItems);

    std::vector<double> a, b;
    if (g_doJ) {
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            jl.ParallelFor(0, kItems, 256, [](int lo, int hi) {
                uint64_t acc = 0;
                for (int i = lo; i < hi; ++i) acc ^= Spin((uint64_t)i, kWorkIters);
                g_sink.fetch_add(acc, std::memory_order_relaxed);
            });
            a.push_back(Ms(t0, Clock::now()));
        }
    }
    if (g_doT) {
        tf::Taskflow flow;
        flow.for_each_index(0, kItems, 1, [](int i) {
            g_sink.fetch_add(Spin((uint64_t)i, kWorkIters), std::memory_order_relaxed);
        });
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            ex.run(flow).wait();
            b.push_back(Ms(t0, Clock::now()));
        }
    }
    Report("JLib", a, "ms");
    Report("Taskflow", b, "ms");
    printf("\n");
}

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);   

    size_t pool = 0;
    bool noSleep = false;
    for (int a = 1; a < argc; ++a) {
        if (strcmp(argv[a], "nosleep") == 0)   { noSleep = true; continue; }
        if (strcmp(argv[a], "--only=jlib") == 0) { g_doT = false; continue; }
        if (strcmp(argv[a], "--only=tf") == 0)   { g_doJ = false; continue; }
        pool = (size_t)strtoul(argv[a], nullptr, 10);
    }
    if (noSleep && g_doT) {
        g_doT = false;
        printf("note: nosleep implies --only=jlib; a spinning pool cannot share a process with\n"
               "      another scheduler being timed. Run --only=tf separately for its column.\n\n");
    }
    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::None);
    JLib::TaskScheduler::Init(pool);
    JLib::TaskScheduler& jl = JLib::TaskScheduler::Instance();

    if (noSleep) {
        JLib::TaskScheduler::SetAwakeFloor(jl.GetWorkerCount());
        JLib::TaskScheduler::SetAwakeFloorMax(jl.GetWorkerCount());
        printf("nosleep: awake floor held at %zu of %zu workers\n\n",
               JLib::TaskScheduler::GetAwakeFloor(), jl.GetWorkerCount());
    }
    if (!g_doJ) JLib::detail::TeardownForTesting(jl);   

    const uint32_t workers = (uint32_t)(pool ? pool : std::thread::hardware_concurrency() - 1);
    tf::Executor ex(workers + 1);   

    printf("\nJLib::Scheduler vs Taskflow %d.%d.%d  (workers=%u, affinity=none, JLib idle=%s, measuring=%s)\n",
           TF_VERSION / 100000, TF_VERSION / 100 % 1000, TF_VERSION % 100,
           workers, noSleep ? "nosleep" : "sleep",
           (g_doJ && g_doT) ? "both" : (g_doJ ? "JLib only" : "Taskflow only"));
    printf("================================================================\n\n");

    if (g_doJ) {
        JLib::WaitGroup wg; wg.n.store(4096, std::memory_order_relaxed);
        for (int i = 0; i < 4096; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
    }
    if (g_doT) {
        tf::Taskflow warm;
        warm.for_each_index(0, 4096, 1, [](int) {});
        ex.run(warm).wait();
    }

    BenchThroughput(jl, ex);
    BenchBulk(jl, ex);
    BenchLatency(jl, ex);

    printf("(sink %llu -- printed only so none of the work can be optimised away)\n",
           (unsigned long long)g_sink.load());

    if (g_doJ) JLib::detail::TeardownForTesting(jl);
    return 0;
}
