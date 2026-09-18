// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <TaskScheduler.h>
#include <TaskScheduler_enki.h>     

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
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
static void Report(const char* who, const std::vector<double>& runs, const char* unit = "ms") {
    const double med = Median(runs);
    const double lo = *std::min_element(runs.begin(), runs.end());
    const double hi = *std::max_element(runs.begin(), runs.end());
    printf("    %-16s %9.3f %-2s  (%.3f to %.3f, %3.0f%% spread)\n",
           who, med, unit, lo, hi, med > 0 ? 100.0 * (hi - lo) / med : 0.0);
}

static constexpr int kRuns      = 7;    
static constexpr int kWorkIters = 200;  

static bool g_doJ = true;
static bool g_doE = true;

static double MedOr(const std::vector<double>& v) { return v.empty() ? -1.0 : Median(v); }
static void Cell(double v, int width, int prec = 1) {
    if (v < 0) printf(" %*s", width, "--");
    else       printf(" %*.*f", width, prec, v);
}

struct EnkiOne : enki::ITaskSet {
    int id = 0;
    void ExecuteRange(enki::TaskSetPartition, uint32_t) override {
        g_sink.fetch_add(Spin((uint64_t)id, kWorkIters), std::memory_order_relaxed);
    }
};
struct EnkiSentinel : enki::ITaskSet {
    EnkiSentinel() { m_SetSize = 1; }
    void ExecuteRange(enki::TaskSetPartition, uint32_t) override {}
};
struct EnkiRange : enki::ITaskSet {
    void ExecuteRange(enki::TaskSetPartition r, uint32_t) override {
        uint64_t acc = 0;
        for (uint32_t i = r.start; i < r.end; ++i) acc ^= Spin(i, kWorkIters);
        g_sink.fetch_add(acc, std::memory_order_relaxed);
    }
};

static void BenchThroughput(JLib::TaskScheduler& jl, enki::TaskScheduler& enki_) {
    printf("  1. independent-task throughput -- ns per task, lower is better\n");
    printf("     (N-sets column is wake amplification, not pipe backpressure -- see comment above)\n\n");
    printf("     %8s %10s %10s %10s %13s %11s\n",
           "tasks", "JLib Push", "JLib Batch", "JLib arr/32", "enkiTS N sets", "enkiTS 1 set");

    const int counts[] = { 64, 256, 1024, 8192, 20000 };
    std::vector<JLib::Task*> batch;
    for (int n : counts) {
        std::vector<double> a, ab, b, c;

      if (g_doJ) {
        for (int r = 0; r < kRuns; ++r) {
            JLib::WaitGroup wg;
            wg.n.store(n, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < n; ++i) {
                JLib::Task* t = jl.CreateTask(+[](void* p) {
                    g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                }, (void*)(intptr_t)i);
                if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
                t->waitGroup = &wg;
                jl.Push(t);
            }
            jl.WaitFor(wg);                                   
            a.push_back(Ms(t0, Clock::now()) * 1e6 / n);      
        }
      }

        batch.resize(n);
      if (g_doJ) {
        for (int r = 0; r < kRuns; ++r) {
            JLib::WaitGroup wg;
            wg.n.store(n, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < n; ++i) {
                batch[i] = jl.CreateTask(+[](void* p) {
                    g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                }, (void*)(intptr_t)i);
                if (!batch[i]) { printf("     JLib: CreateTask returned null\n"); return; }
                batch[i]->waitGroup = &wg;
            }
            jl.PushBatch(batch.data(), (size_t)n);
            jl.WaitFor(wg);
            ab.push_back(Ms(t0, Clock::now()) * 1e6 / n);
        }
      }

        std::vector<double> pa;
      if (g_doJ) {
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

      if (g_doE) {
        std::vector<EnkiOne> sets(n);
        for (int i = 0; i < n; ++i) { sets[i].id = i; sets[i].m_SetSize = 1; }
        EnkiSentinel sentinel;
        std::vector<enki::Dependency> deps(n);
        for (int i = 0; i < n; ++i) sentinel.SetDependency(deps[i], &sets[i]);
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < n; ++i) enki_.AddTaskSetToPipe(&sets[i]);
            enki_.WaitforTask(&sentinel);     
            b.push_back(Ms(t0, Clock::now()) * 1e6 / n);
        }
      }

      if (g_doE) {
        EnkiRange one;
        one.m_SetSize = (uint32_t)n;
        one.m_MinRange = 1;
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            enki_.AddTaskSetToPipe(&one);
            enki_.WaitforTask(&one);
            c.push_back(Ms(t0, Clock::now()) * 1e6 / n);
        }
      }

        printf("     %8d", n);
        Cell(MedOr(a), 10); Cell(MedOr(ab), 10); Cell(MedOr(pa), 10);
        Cell(MedOr(b), 13); Cell(MedOr(c), 11);
        printf("\n");
    }
    printf("\n");
}

static void BenchBulk(JLib::TaskScheduler& jl, enki::TaskScheduler& enki_) {
    constexpr int kItems = 20000;
    printf("  2. bulk parallel-for (%d items, native shape on both sides)\n", kItems);

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

  if (g_doE) {
    EnkiRange bulk;
    bulk.m_SetSize = kItems;
    bulk.m_MinRange = 256;
    for (int r = 0; r < kRuns; ++r) {
        auto t0 = Clock::now();
        enki_.AddTaskSetToPipe(&bulk);
        enki_.WaitforTask(&bulk);
        b.push_back(Ms(t0, Clock::now()));
    }
  }

    if (g_doJ) Report("JLib ParallelFor",   a);

    if (g_doE) Report("enkiTS",             b);
    printf("\n");
}

struct EnkiPing : enki::ITaskSet {
    void ExecuteRange(enki::TaskSetPartition, uint32_t) override {
        g_sink.fetch_add(1, std::memory_order_relaxed);
    }
};

static void BenchLatency(JLib::TaskScheduler& jl, enki::TaskScheduler& enki_) {
    constexpr int kPings = 20000;
    printf("  3. round-trip latency (%d serial submit-and-wait round-trips)\n", kPings);

    std::vector<double> a, b;
  if (g_doJ) {
    for (int r = 0; r < 3; ++r) {
        auto t0 = Clock::now();
        for (int i = 0; i < kPings; ++i) {
            JLib::WaitGroup wg;
            wg.n.store(1, std::memory_order_relaxed);
            JLib::Task* t = jl.CreateTask(+[](void*) { g_sink.fetch_add(1, std::memory_order_relaxed); }, nullptr);
            if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
            t->waitGroup = &wg;
            jl.Push(t);
            jl.WaitFor(wg);
        }
        a.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
    }
  }

  if (g_doE) {
    EnkiPing ping;
    ping.m_SetSize = 1;
    for (int r = 0; r < 3; ++r) {
        auto t0 = Clock::now();
        for (int i = 0; i < kPings; ++i) {
            enki_.AddTaskSetToPipe(&ping);
            enki_.WaitforTask(&ping);
        }
        b.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
    }
  }

    if (g_doJ) Report("JLib", a, "us");
    if (g_doE) Report("enkiTS", b, "us");
    printf("\n");
}

static constexpr int kBatch      = 256;
static constexpr int kBatches    = 10;
static constexpr int kBlockEvery = 4;         
static constexpr int kHeavyIters = 50000;     

static std::atomic<int>  g_blockersLeft{ 0 };
static std::atomic<bool> g_released{ false };
static std::mutex              g_cvMutex;
static std::condition_variable g_cv;

static void Releaser(JLib::TaskScheduler* jl, int durationUs, bool jlibSide) {
    const auto deadline = Clock::now() + std::chrono::microseconds(durationUs);
    while (Clock::now() < deadline) std::this_thread::yield();
    if (jlibSide) {
        g_released.store(true, std::memory_order_release);
        jl->GetEvent("compare_io").SignalAll();
    } else {
        { std::lock_guard<std::mutex> g(g_cvMutex); g_released.store(true, std::memory_order_release); }
        g_cv.notify_all();
    }
}

struct EnkiBlock : enki::ITaskSet {
    void ExecuteRange(enki::TaskSetPartition r, uint32_t) override {
        for (uint32_t i = r.start; i < r.end; ++i) {
            if (i % kBlockEvery == 0) {
                std::unique_lock<std::mutex> lk(g_cvMutex);
                g_cv.wait(lk, [] { return g_released.load(std::memory_order_acquire); });
            } else {
                g_sink.fetch_add(Spin(i, kHeavyIters), std::memory_order_relaxed);
            }
        }
    }
};

static void BenchBlocking(JLib::TaskScheduler& jl, enki::TaskScheduler& enki_) {
    printf("  4. blocking crossover -- 25%% of tasks wait on an external signal\n");
    printf("     %d batches of %d; ms for all %d tasks, lower is better\n\n",
           kBatches, kBatch, kBatches * kBatch);
    printf("     %8s %10s %10s %8s %10s %10s %8s\n",
           "block us", "JLib", "enkiTS", "ratio", "JLib d", "enkiTS d", "d ratio");

    const int durations[] = { 0, 50, 150, 300, 600, 2000 };
    double jlBase = 0, enBase = 0;

    for (int d : durations) {
        std::vector<double> a, b;

      if (g_doJ) {
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int batch = 0; batch < kBatches; ++batch) {
                g_released.store(d == 0, std::memory_order_release);
                g_blockersLeft.store(d ? kBatch / kBlockEvery : 0, std::memory_order_release);
                std::thread rel;
                if (d) rel = std::thread(Releaser, &jl, d, true);

                JLib::WaitGroup wg;
                wg.n.store(kBatch, std::memory_order_relaxed);
                for (int i = 0; i < kBatch; ++i) {
                    
                    const bool blocks = (i % kBlockEvery) == 0;
                    
                    JLib::Task* t = blocks
                        
                        ? jl.CreateTask(+[](void*) {
                              JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
                              s.WaitOnEventArmed("compare_io", [&s] {
                                  if (g_released.load(std::memory_order_acquire))
                                      s.GetEvent("compare_io").SignalAll();
                              });
                              g_blockersLeft.fetch_sub(1, std::memory_order_acq_rel);
                          }, nullptr, JLib::Lane::Normal, JLib::TaskType::Fiber)
                        : jl.CreateTask(+[](void* p) {
                              g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kHeavyIters),
                                               std::memory_order_relaxed);
                          }, (void*)(intptr_t)i);
                    if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
                    t->waitGroup = &wg;
                    jl.Push(t);
                }
                jl.WaitFor(wg);
                if (rel.joinable()) rel.join();
            }
            a.push_back(Ms(t0, Clock::now()));
        }
      }

      if (g_doE) {
        EnkiBlock set;
        set.m_SetSize = kBatch;
        set.m_MinRange = 8;
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int batch = 0; batch < kBatches; ++batch) {
                g_released.store(d == 0, std::memory_order_release);
                std::thread rel;
                if (d) rel = std::thread(Releaser, &jl, d, false);
                enki_.AddTaskSetToPipe(&set);
                enki_.WaitforTask(&set);
                if (rel.joinable()) rel.join();
            }
            b.push_back(Ms(t0, Clock::now()));
        }
      }

        const double ja = MedOr(a), eb = MedOr(b);
        if (d == 0) { jlBase = (ja < 0 ? 0 : ja); enBase = (eb < 0 ? 0 : eb); }
        const double jd = (ja < 0) ? -1.0 : ja - jlBase;
        const double ed = (eb < 0) ? -1.0 : eb - enBase;
        
        printf("     %8d", d);
        Cell(ja, 10, 2); Cell(eb, 10, 2);
        if (ja > 0 && eb > 0) printf(" %6.2fx", eb / ja); else printf("       -");
        Cell(jd, 10, 2); Cell(ed, 10, 2);
        if (jd > 0.5 && ed >= 0) printf(" %7.2fx\n", ed / jd); else printf("       -\n");
    }
    printf("\n     ratio > 1.00 means JLib is faster. The delta columns subtract each library's own\n");
    printf("     D=0 baseline, cancelling JLib's per-task submission overhead (see the header).\n\n");
}

static void BenchBreakdown(JLib::TaskScheduler& jl) {
    if (!g_doJ) return;   
    printf("  5. JLib per-task cost breakdown -- ns per task, empty bodies\n\n");
    printf("     %8s %10s %10s %10s %14s %8s %8s %8s\n",
           "tasks", "create", "dispatch", "total", "+shared sink", "arr/8", "arr/32", "arr/128");

    const int counts[] = { 256, 1024, 8192, 20000 };
    std::vector<JLib::Task*> batch;
    for (int n : counts) {
        std::vector<double> cr, di, sk;
        for (int pass = 0; pass < 2; ++pass) {
            
            for (int r = 0; r < kRuns; ++r) {
                batch.resize(n);
                JLib::WaitGroup wg;
                wg.n.store(n, std::memory_order_relaxed);

                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    batch[i] = pass == 0
                        ? (JLib::Task*)jl.CreateTask(+[](void*) {}, nullptr)
                        : (JLib::Task*)jl.CreateTask(+[](void*) {
                              g_sink.fetch_add(1, std::memory_order_relaxed);
                          }, nullptr);
                    if (!batch[i]) { printf("     JLib: CreateTask returned null\n"); return; }
                    batch[i]->waitGroup = &wg;
                }
                auto t1 = Clock::now();
                jl.PushBatch(batch.data(), (size_t)n);
                jl.WaitFor(wg);
                auto t2 = Clock::now();

                if (pass == 0) {
                    cr.push_back(Ms(t0, t1) * 1e6 / n);
                    di.push_back(Ms(t1, t2) * 1e6 / n);
                } else {
                    sk.push_back(Ms(t0, t2) * 1e6 / n);
                }
            }
        }
        
        double pa[3] = { 0, 0, 0 };
        const size_t chunks[3] = { 8, 32, 128 };
        for (int ci = 0; ci < 3; ++ci) {
            std::vector<double> v;
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg;
                auto t0 = Clock::now();
                jl.PushArray(0, (size_t)n, chunks[ci], [](size_t) {}, &wg);
                jl.WaitFor(wg);
                v.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            pa[ci] = Median(v);
        }

        const double c = Median(cr), d = Median(di);
        printf("     %8d %10.1f %10.1f %10.1f %14.1f %8.1f %8.1f %8.1f\n",
               n, c, d, c + d, Median(sk), pa[0], pa[1], pa[2]);
    }
    printf("\n");
}

int main(int argc, char** argv) {
    
    setvbuf(stdout, nullptr, _IONBF, 0);   
    
    size_t pool = 0;
    bool noSleep = false;
    for (int a = 1; a < argc; ++a) {
        if (strcmp(argv[a], "nosleep") == 0)    { noSleep = true; continue; }
        if (strcmp(argv[a], "--only=jlib") == 0) { g_doE = false; continue; }
        if (strcmp(argv[a], "--only=enki") == 0) { g_doJ = false; continue; }
        pool = (size_t)strtoul(argv[a], nullptr, 10);
    }
    
    if (noSleep && g_doE) {
        g_doE = false;
        printf("note: nosleep implies --only=jlib; a spinning pool cannot share a process with\n"
               "      another scheduler being timed. Run --only=enki separately for its column.\n\n");
    }

    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::None);
    
    JLib::TaskScheduler::Init(pool);

    if (noSleep) {
        JLib::TaskScheduler::SetAwakeFloor(JLib::TaskScheduler::Instance().GetWorkerCount());
        JLib::TaskScheduler::SetAwakeFloorMax(JLib::TaskScheduler::Instance().GetWorkerCount());
        printf("nosleep: awake floor held at %zu of %zu workers\n\n",
               JLib::TaskScheduler::GetAwakeFloor(),
               JLib::TaskScheduler::Instance().GetWorkerCount());
    }
    JLib::TaskScheduler& jl = JLib::TaskScheduler::Instance();
    if (!g_doJ) JLib::detail::TeardownForTesting(jl);

    const uint32_t workers = (uint32_t)(pool ? pool : std::thread::hardware_concurrency() - 1);
    enki::TaskScheduler enki_;
    if (g_doE) enki_.Initialize(workers + 1);

    printf("\nJLib::Scheduler vs enkiTS   (workers=%u, affinity=none, JLib idle=%s, measuring=%s)\n",
           workers, noSleep ? "nosleep" : "sleep",
           (g_doJ && g_doE) ? "both" : (g_doJ ? "JLib only" : "enkiTS only"));
    printf("================================================================\n");
    printf("Calibration, not a contest. Predictions are in the file header.\n\n");

    if (g_doJ) {
        JLib::WaitGroup wg; wg.n.store(4096, std::memory_order_relaxed);
        for (int i = 0; i < 4096; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
    }
    if (g_doE) {
        EnkiRange w; w.m_SetSize = 4096; w.m_MinRange = 64;
        enki_.AddTaskSetToPipe(&w); enki_.WaitforTask(&w);
    }

    BenchThroughput(jl, enki_);
    BenchBulk(jl, enki_);
    BenchLatency(jl, enki_);
    BenchBlocking(jl, enki_);
    BenchBreakdown(jl);

    printf("(sink %llu -- printed only so none of the work can be optimised away)\n",
           (unsigned long long)g_sink.load());

    if (g_doE) enki_.WaitforAllAndShutdown();
    if (g_doJ) JLib::detail::TeardownForTesting(jl);
    return 0;
}
