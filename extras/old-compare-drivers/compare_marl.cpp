// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <TaskScheduler.h>
#include <Thread.h>          
#include <marl/scheduler.h>
#include <marl/waitgroup.h>
#include <marl/event.h>

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

static std::atomic<unsigned char> g_blkSeen[64];
static inline void NoteBlkWorker() {
    if (JLib::Thread* w = JLib::Thread::Current()) {
        const int q = w->qIndex;
        if (q >= 0 && q < 64) g_blkSeen[q].store(1, std::memory_order_relaxed);
    }
}
static void ResetBlkSeen() { for (auto& c : g_blkSeen) c.store(0, std::memory_order_relaxed); }
static size_t BlkParticipants() {
    size_t n = 0;
    for (auto& c : g_blkSeen) if (c.load(std::memory_order_relaxed)) ++n;
    return n;
}

static double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}
static double MedOr(const std::vector<double>& v) { return v.empty() ? -1.0 : Median(v); }
static void Cell(double v, int width, int prec = 1) {
    if (v < 0) printf(" %*s", width, "--");
    else       printf(" %*.*f", width, prec, v);
}
static void Report(const char* who, const std::vector<double>& runs, const char* unit) {
    if (runs.empty()) { printf("    %-16s %9s\n", who, "--"); return; }
    const double med = Median(runs);
    const double lo = *std::min_element(runs.begin(), runs.end());
    const double hi = *std::max_element(runs.begin(), runs.end());
    printf("    %-16s %9.3f %-2s  (%.3f to %.3f, %3.0f%% spread)\n",
           who, med, unit, lo, hi, med > 0 ? 100.0 * (hi - lo) / med : 0.0);
}

static marl::Scheduler* g_marlSched = nullptr;
static bool g_doJ = true;
static bool g_doM = true;

static bool g_noGrow = false;
static bool g_noHelp = false;

static long g_floorMax = -1;

static JLib::TaskType g_jlType = JLib::TaskType::Fiber;
static constexpr int kRuns      = 7;
static constexpr int kWorkIters = 200;

static void BenchThroughput(JLib::TaskScheduler& jl) {
    printf("  independent-task throughput -- ns per task, lower is better\n\n");
    printf("     %8s %11s %11s %12s %11s\n",
           "tasks", "JLib Push", "JLib Batch", "JLib arr/32", "marl");

    const int counts[] = { 256, 1024, 8192, 20000 };
    std::vector<JLib::Task*> batch;
    for (int n : counts) {
        std::vector<double> a, ab, pa, m;
        double helped = 0.0;   

        if (g_doJ) {
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg; wg.n.store(n, std::memory_order_relaxed);
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    JLib::Task* t = jl.CreateTask(+[](void* p) {
                        g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                    }, (void*)(intptr_t)i, JLib::Lane::Normal, g_jlType);
                    if (!t) return;
                    t->waitGroup = &wg; jl.Push(t);
                }
                jl.WaitFor(wg);
                
                helped += (double)JLib::TaskScheduler::LastBareWaitHelped();
                a.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            batch.resize(n);
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg; wg.n.store(n, std::memory_order_relaxed);
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    batch[i] = jl.CreateTask(+[](void* p) {
                        g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                    }, (void*)(intptr_t)i, JLib::Lane::Normal, g_jlType);
                    if (!batch[i]) return;
                    batch[i]->waitGroup = &wg;
                }
                jl.PushBatch(batch.data(), (size_t)n);
                jl.WaitFor(wg);
                ab.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            
            if (g_jlType != JLib::TaskType::Fiber) {
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
        }
        if (g_doM) {
            for (int r = 0; r < kRuns; ++r) {
                marl::WaitGroup wg(n);
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    marl::schedule([wg, i] {
                        g_sink.fetch_add(Spin((uint64_t)i, kWorkIters), std::memory_order_relaxed);
                        wg.done();
                    });
                }
                wg.wait();
                m.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
        }

        printf("     %8d", n);
        Cell(MedOr(a), 11); Cell(MedOr(ab), 11); Cell(MedOr(pa), 12); Cell(MedOr(m), 11);
        
        if (g_doJ) printf("   main ran %.0f%%", 100.0 * helped / ((double)n * kRuns));
        printf("\n");
    }
    printf("\n");
}

static void BenchLatency(JLib::TaskScheduler& jl) {
    constexpr int kPings = 20000;
    printf("  round-trip latency (%d serial submit-and-wait round-trips)\n", kPings);

    std::vector<double> a, b;
    
    if (g_doJ) JLib::TaskScheduler::ForceAwakeFloorToBase();
    const size_t floorBefore = JLib::TaskScheduler::GetAwakeFloor();
    if (g_doJ) {
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < kPings; ++i) {
                JLib::WaitGroup wg; wg.n.store(1, std::memory_order_relaxed);
                JLib::Task* t = jl.CreateTask(+[](void*) {
                    g_sink.fetch_add(1, std::memory_order_relaxed);
                }, nullptr, JLib::Lane::Normal, g_jlType);
                if (!t) return;
                t->waitGroup = &wg; jl.Push(t); jl.WaitFor(wg);
            }
            a.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
        }
    }
    if (g_doM) {
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < kPings; ++i) {
                marl::WaitGroup wg(1);
                marl::schedule([wg] {
                    g_sink.fetch_add(1, std::memory_order_relaxed);
                    wg.done();
                });
                wg.wait();
            }
            b.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
        }
    }
    Report("JLib", a, "us");
    Report("marl", b, "us");
    
    if (g_doJ) {
        const size_t floorAfter = JLib::TaskScheduler::GetAwakeFloor();
        printf("     floor during this row: %zu -> %zu (base %zu)%s\n",
               floorBefore, floorAfter, JLib::TaskScheduler::GetAwakeFloorBase(),
               (floorBefore == floorAfter) ? "" : "   <- MOVED; this row is not one configuration");
    }
    printf("\n");
}

static constexpr int kBatch      = 256;
static constexpr int kBatches    = 10;
static constexpr int kBlockEvery = 4;       
static constexpr int kHeavyIters = 50000;   

static std::atomic<bool> g_released{ false };

static JLib::Event* g_ioEvent = nullptr;

static void ReleaseAfter(int durationUs, JLib::TaskScheduler* jl, marl::Event* ev) {
    const auto deadline = Clock::now() + std::chrono::microseconds(durationUs);
    while (Clock::now() < deadline) std::this_thread::yield();
    g_released.store(true, std::memory_order_release);
    if (jl) g_ioEvent->SignalAll();
    if (ev) ev->signal();
}

static constexpr int kVictimIters  = 8000;    
static constexpr int kVictimSpin   = 20000;
static constexpr int kTickUs       = 2000;
static constexpr int kTasksPerTick = 8;

static constexpr int kVictimThreads = 4;

static double RunVictim() {
    auto t0 = Clock::now();
    std::vector<std::thread> v;
    v.reserve(kVictimThreads);
    for (int t = 0; t < kVictimThreads; ++t)
        v.emplace_back([t] {
            uint64_t x = (uint64_t)t + 1;
            for (int i = 0; i < kVictimIters; ++i) x = Spin(x, kVictimSpin);
            g_sink.fetch_add(x, std::memory_order_relaxed);
        });
    for (auto& th : v) th.join();
    return Ms(t0, Clock::now());
}

static void BenchIdleTax(JLib::TaskScheduler& jl) {
    
    printf("  idle-policy tax -- what the pool costs a CO-RESIDENT thread, ms (lower is better)\n");
    printf("     [measuring: %s -- the other arm needs its own --only= run]\n\n",
           g_doJ ? "JLib" : (g_doM ? "marl" : "neither"));

    const double base = std::min(RunVictim(), RunVictim());

    JLib::TaskScheduler::ResetAwakeFloorPeak();
    size_t floorLiveAtEnd = 0;
    std::atomic<bool> stop{ false };
    double withPool = -1.0;
    if (g_doJ) {
        std::thread ticker([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                JLib::WaitGroup wg; wg.n.store(kTasksPerTick, std::memory_order_relaxed);
                for (int i = 0; i < kTasksPerTick; ++i) {
                    JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
                    if (!t) break;
                    t->waitGroup = &wg; jl.Push(t);
                }
                jl.WaitFor(wg);
                std::this_thread::sleep_for(std::chrono::microseconds(kTickUs));
            }
        });
        withPool = RunVictim();
        floorLiveAtEnd = JLib::TaskScheduler::GetAwakeFloor();   
        stop.store(true, std::memory_order_relaxed);
        ticker.join();
    }
    else if (g_doM) {
        std::thread ticker([&] {
            
            marl::Scheduler* s = g_marlSched;
            if (!s) return;                 
            s->bind();
            while (!stop.load(std::memory_order_relaxed)) {
                marl::WaitGroup wg(kTasksPerTick);
                for (int i = 0; i < kTasksPerTick; ++i) marl::schedule([wg] { wg.done(); });
                wg.wait();
                std::this_thread::sleep_for(std::chrono::microseconds(kTickUs));
            }
            marl::Scheduler::unbind();
        });
        withPool = RunVictim();
        stop.store(true, std::memory_order_relaxed);
        ticker.join();
    }

    if (g_doJ)
        printf("     JLib floor during the row: peak %zu, live at end %zu (base %zu, tick %d us)\n",
               JLib::TaskScheduler::GetAwakeFloorPeak(), floorLiveAtEnd,
               JLib::TaskScheduler::GetAwakeFloorBase(), kTickUs);
    printf("     victim alone                       %8.1f ms\n", base);
    printf("     victim with a ticking pool         %8.1f ms\n", withPool);
    if (base > 0 && withPool > 0)
        printf("     TAX                                %8.1f ms  (%.1f%%)\n",
               withPool - base, 100.0 * (withPool - base) / base);
    printf("\n     A pool that parks should read near zero here. A pool that spins after every tick\n"
           "     charges the victim for cores it is not using. This row is why the blocking\n"
           "     crossover must not be quoted on its own.\n\n");
}

static void BenchBlocking(JLib::TaskScheduler& jl) {
    {
        
        char capbuf[40];
        if (g_floorMax >= 0) snprintf(capbuf, sizeof capbuf, "%ld (floormax=)", g_floorMax);
        else                 snprintf(capbuf, sizeof capbuf, "default policy");
        printf("  blocking crossover -- 25%% of tasks wait on an external signal"
               "   [growth: %s, cap %s]\n", g_noGrow ? "OFF" : "on", capbuf);
    }
    printf("     %d batches of %d; ms for all %d tasks, lower is better\n\n",
           kBatches, kBatch, kBatches * kBatch);
    printf("     %8s %11s %11s %9s\n", "block us", "JLib", "marl", "ratio");

    const int durations[] = { 0, 50, 150, 300, 600, 2000 };
    
    unsigned long long blkWakes = 0;
    for (int d : durations) {
        std::vector<double> a, b, blkPart;

        if (g_doJ) {
            JLib::TaskScheduler::ResetWakeCount();
            JLib::TaskScheduler::ResetAwakeFloorPeak();
            for (int r = 0; r < 3; ++r) {
                
                ResetBlkSeen();
                auto t0 = Clock::now();
                for (int batch = 0; batch < kBatches; ++batch) {
                    g_released.store(d == 0, std::memory_order_release);
                    std::thread rel;
                    if (d) rel = std::thread(ReleaseAfter, d, &jl, nullptr);

                    static JLib::Task* blk[kBatch];
                    JLib::WaitGroup wg;
                    wg.n.store(kBatch, std::memory_order_relaxed);
                    for (int i = 0; i < kBatch; ++i) {
                        
                        const bool blocks = (i % kBlockEvery) == 0;
                        JLib::Task* t = blocks
                            
                            ? jl.CreateTask(+[](void*) {
                                  NoteBlkWorker();
                                  JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
                                  s.WaitOnEventArmed(*g_ioEvent, [] {
                                      if (g_released.load(std::memory_order_acquire))
                                          g_ioEvent->SignalAll();
                                  });
                              }, nullptr, JLib::Lane::Normal, JLib::TaskType::Fiber)
                            : jl.CreateTask(+[](void* p) {
                                  NoteBlkWorker();
                                  g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kHeavyIters),
                                                   std::memory_order_relaxed);
                              }, (void*)(intptr_t)i, JLib::Lane::Normal, g_jlType);
                        if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
                        t->waitGroup = &wg;
                        jl.Push(t);
                    }
                    jl.WaitFor(wg);
                    if (rel.joinable()) rel.join();
                }
                a.push_back(Ms(t0, Clock::now()));
                blkPart.push_back((double)BlkParticipants());
            }
        }
        if (g_doM) {
            for (int r = 0; r < 3; ++r) {
                auto t0 = Clock::now();
                for (int batch = 0; batch < kBatches; ++batch) {
                    
                    marl::Event ev(marl::Event::Mode::Manual, d == 0);
                    g_released.store(d == 0, std::memory_order_release);
                    std::thread rel;
                    if (d) rel = std::thread(ReleaseAfter, d, nullptr, &ev);

                    marl::WaitGroup wg(kBatch);
                    for (int i = 0; i < kBatch; ++i) {
                        const bool blocks = (i % kBlockEvery) == 0;
                        if (blocks) {
                            marl::schedule([wg, ev] { ev.wait(); wg.done(); });
                        } else {
                            marl::schedule([wg, i] {
                                g_sink.fetch_add(Spin((uint64_t)i, kHeavyIters),
                                                 std::memory_order_relaxed);
                                wg.done();
                            });
                        }
                    }
                    wg.wait();
                    if (rel.joinable()) rel.join();
                }
                b.push_back(Ms(t0, Clock::now()));
            }
        }

        if (g_doJ) blkWakes = (unsigned long long)JLib::TaskScheduler::GetWakeCount();
        const size_t blkPeakF = g_doJ ? JLib::TaskScheduler::GetAwakeFloorPeak() : 0;
        const double ja = MedOr(a), mb = MedOr(b);
        printf("     %8d", d);
        Cell(ja, 11, 2); Cell(mb, 11, 2);
        if (ja > 0 && mb > 0) printf(" %8.2fx", mb / ja); else printf("        -");
        
        if (g_doJ) printf("   wakes %llu / 7680   peakF %zu   ran %.0f",
                          blkWakes, blkPeakF, MedOr(blkPart));
        printf("\n");
    }
    printf("\n     ratio > 1.00 means JLib is faster.\n\n");
}

static void BenchFiberBreakdown(JLib::TaskScheduler& jl) {
    if (!g_doJ) return;
    
    constexpr int N = 1000;
    printf("  fiber cost breakdown -- ns per task, empty bodies, JLib only\n\n");

    std::vector<double> a, b, c;

    auto runA = [&]() -> double {
        if (g_jlType == JLib::TaskType::Fiber) return -1.0;   
        JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
        return Ms(t0, Clock::now()) * 1e6 / N;
    };
    auto runB = [&]() -> double {
        JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr, JLib::Lane::Normal, JLib::TaskType::Fiber);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
        return Ms(t0, Clock::now()) * 1e6 / N;
    };
    auto runC = [&]() -> double {
        g_released.store(true, std::memory_order_release);
        JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {
                JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
                s.WaitOnEventArmed(*g_ioEvent, [] { g_ioEvent->SignalAll(); });
            }, nullptr, JLib::Lane::Normal, JLib::TaskType::Fiber);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
        return Ms(t0, Clock::now()) * 1e6 / N;
    };

    (void)runA(); (void)runB(); (void)runC();   

    for (int r = 0; r < 6; ++r) {               
        for (int s = 0; s < 3; ++s) {
            switch ((r + s) % 3) {
                case 0: { const double v = runA(); if (v >= 0) a.push_back(v); } break;
                case 1:   b.push_back(runB()); break;
                default:  c.push_back(runC()); break;
            }
        }
    }

#if 0
    for (int r = 0; r < 5; ++r) {
        
        if (g_jlType != JLib::TaskType::Fiber) {
            JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) {
                JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
                t->waitGroup = &wg; jl.Push(t);
            }
            jl.WaitFor(wg);
            a.push_back(Ms(t0, Clock::now()) * 1e6 / N);
        }
        {   
            JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) {
                JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr,
                                              JLib::Lane::Normal, JLib::TaskType::Fiber);
                t->waitGroup = &wg; jl.Push(t);
            }
            jl.WaitFor(wg);
            b.push_back(Ms(t0, Clock::now()) * 1e6 / N);
        }
        {   
            g_released.store(true, std::memory_order_release);
            JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) {
                JLib::Task* t = jl.CreateTask(+[](void*) {
                    JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
                    s.WaitOnEventArmed(*g_ioEvent, [] { g_ioEvent->SignalAll(); });
                }, nullptr, JLib::Lane::Normal, JLib::TaskType::Fiber);
                t->waitGroup = &wg; jl.Push(t);
            }
            jl.WaitFor(wg);
            c.push_back(Ms(t0, Clock::now()) * 1e6 / N);
        }
    }

#endif
    
    const double ma = MedOr(a), mb = MedOr(b), mc = MedOr(c);

    if (ma >= 0) {
        printf("     %-34s %8.0f ns\n", "A  no fiber (baseline)", ma);
        printf("     %-34s %8.0f ns   (+%.0f for the fiber)\n",
               "B  fiber, never suspends", mb, mb - ma);
    } else {
        printf("     %-34s %8s      (no Native path in the Fiber-task arm)\n",
               "A  no fiber (baseline)", "--");
        printf("     %-34s %8.0f ns\n", "B  fiber, never suspends", mb);
    }

    printf("     %-34s %8.0f ns   (+%.0f for the EVENT park/resume)\n",
           "C  fiber, suspend + resume", mc, mc - mb);
    printf("\n");
}
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);

    size_t pool = 0;
    bool noSleep = false;
    
    bool fiberOnly = false;
    
    size_t hot = 0;
    
    long floorArg = -1;
    long floorMaxArg = -1;   
    for (int i = 1; i < argc; ++i) {
        if (strncmp(argv[i], "hot=", 4) == 0)     { hot = (size_t)strtoul(argv[i] + 4, nullptr, 10); continue; }
        
        if (strcmp(argv[i], "widesteer") == 0) {
            JLib::TaskScheduler::SetPlacementFollowsGrownFloor(true);
            continue;
        }
        if (strncmp(argv[i], "spinyield=", 10) == 0) {
            JLib::TaskScheduler::SetSpinYieldMask((unsigned)strtoul(argv[i] + 10, nullptr, 10));
            continue;
        }
        
        if (strncmp(argv[i], "floor=", 6) == 0) {
            floorArg = (long)strtol(argv[i] + 6, nullptr, 10);
            continue;
        }
        
        if (strncmp(argv[i], "floormax=", 9) == 0) {
            floorMaxArg = (long)strtol(argv[i] + 9, nullptr, 10);
            continue;
        }
        
        if (strcmp(argv[i], "nohelp") == 0) {
            JLib::TaskScheduler::SetBareWaitHelp(false);
            g_noHelp = true;
            continue;
        }
        if (strcmp(argv[i], "nogrow") == 0) {
            JLib::TaskScheduler::SetFloorGrowthEnabled(false);
            g_noGrow = true;
            continue;
        }
        if (strcmp(argv[i], "nosleep") == 0)      { noSleep = true; continue; }
        if (strcmp(argv[i], "fiberonly") == 0)    { fiberOnly = true; continue; }
        if (strcmp(argv[i], "--only=jlib") == 0)  { g_doM = false; continue; }
        if (strcmp(argv[i], "--only=marl") == 0)  { g_doJ = false; continue; }
        pool = (size_t)strtoul(argv[i], nullptr, 10);
    }
    if (noSleep && g_doM) {
        g_doM = false;
        printf("note: nosleep implies --only=jlib; a spinning pool cannot share a process with\n"
               "      another scheduler being timed. Run --only=marl separately.\n\n");
    }

    if (hot) {
        JLib::TaskScheduler::SetHotWorkers(hot);
        JLib::TaskScheduler::SetHotThreadPolicy(JLib::TaskScheduler::HotThreadPolicy::Elevated);
    }

    if (fiberOnly && g_doM) {
        g_doM = false;
        printf("note: fiberonly implies --only=jlib -- its park never sleeps, and a spinning pool\n"
               "      cannot share a process with another scheduler being timed. Run --only=marl\n"
               "      separately and compare the two pastes.\n\n");
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
    
    if (fiberOnly) g_jlType = JLib::TaskType::Fiber;
    
    if (floorMaxArg >= 0) {
        JLib::TaskScheduler::SetAwakeFloorMax((size_t)floorMaxArg);
        g_floorMax = floorMaxArg;
    }
    if (floorArg >= 0) {
        JLib::TaskScheduler::SetAwakeFloor((size_t)floorArg);
        if (floorArg == 0) JLib::TaskScheduler::SetFloorGrowthEnabled(false);
    }

    if (g_doJ && g_doM && floorArg > 0) {
        printf("WARNING: floor=%ld with BOTH arms. JLib's floor workers do not park, so marl is\n"
               "         timed while %ld cores are held spinning -- its column is not comparable to\n"
               "         a run with a different floor. Use --only=jlib and --only=marl separately\n"
               "         and compare the pastes.\n\n", floorArg, floorArg);
    }

    JLib::TaskScheduler& jl = JLib::TaskScheduler::Instance();
    if (g_doJ) g_ioEvent = &jl.GetEvent("compare_io");   
    if (!g_doJ) JLib::detail::TeardownForTesting(jl);   

    const uint32_t workers = (uint32_t)(pool ? pool : std::thread::hardware_concurrency() - 1);

    marl::Scheduler::Config cfg;
    cfg.setWorkerThreadCount((int)workers);
    marl::Scheduler marlSched(cfg);
    g_marlSched = &marlSched;
    if (g_doM) marlSched.bind();

    printf("\nJLib::Scheduler vs marl  (workers=%u, affinity=none, JLib idle=%s, measuring=%s)\n",
           workers, noSleep ? "nosleep" : "sleep",
           (g_doJ && g_doM) ? "both" : (g_doJ ? "JLib only" : "marl only"));
    printf("================================================================\n");
    printf("marl is ARCHIVED (last commit 2026-04-27) -- calibration, not a recommendation.\n\n");

    if (g_doJ) {
        JLib::WaitGroup wg; wg.n.store(4096, std::memory_order_relaxed);
        for (int i = 0; i < 4096; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr, JLib::Lane::Normal,
                                          g_jlType);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
    }
    if (g_doM) {
        marl::WaitGroup wg(4096);
        for (int i = 0; i < 4096; ++i) marl::schedule([wg] { wg.done(); });
        wg.wait();
    }

    BenchThroughput(jl);
    BenchLatency(jl);
    BenchBlocking(jl);
    BenchIdleTax(jl);
    BenchFiberBreakdown(jl);

    printf("(sink %llu -- printed only so none of the work can be optimised away)\n",
           (unsigned long long)g_sink.load());

    if (g_doM) marl::Scheduler::unbind();

    if (g_doJ) {
        std::atomic<bool> joined{ false };
        std::thread watchdog([&] {
            for (int i = 0; i < 100 && !joined.load(std::memory_order_acquire); ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (!joined.load(std::memory_order_acquire)) {
                printf("\n!!! Join() has not returned after 10s -- pool state follows !!!\n");
                jl.DumpPoolState("Join watchdog");
                fflush(stdout);
            }
            });
        JLib::detail::TeardownForTesting(jl);
        joined.store(true, std::memory_order_release);
        watchdog.join();
    }
    return 0;
}
