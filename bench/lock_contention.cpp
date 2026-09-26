// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "TaskScheduler.h"

#if !defined(JLIBSCHED_TUNABLE_FAST_SPIN)
#error "SchedulerLockBench requires -DJLIBSCHED_TUNABLE_FAST_SPIN=ON. Without it the spin bound is a \
compile-time constant, the arms cannot be interleaved in one process, and the measurement is \
swamped by process-to-process drift -- see the header comment."
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static inline int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}

static volatile uint64_t g_sink = 0;

static inline void BusySpin(uint64_t iters) {
    uint64_t x = 0x9E3779B97F4A7C15ull ^ iters;
    for (uint64_t i = 0; i < iters; ++i) {
        x = x * 6364136223846793005ull + 1442695040888963407ull;
    }
    g_sink = x;
}

static double g_itersPerNs = 1.0;

static void Calibrate() {
    
    BusySpin(2'000'000);
    const int64_t t0 = NowNs();
    BusySpin(20'000'000);
    const int64_t t1 = NowNs();
    const double ns = static_cast<double>(t1 - t0);
    g_itersPerNs = ns > 0.0 ? (20'000'000.0 / ns) : 1.0;
}

static inline uint64_t ItersForNs(int ns) {
    if (ns <= 0) return 0;
    const double it = g_itersPerNs * static_cast<double>(ns);
    return it < 1.0 ? 1u : static_cast<uint64_t>(it);
}

static double Median(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return (n % 2) ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

static uint32_t Pct(const std::vector<uint32_t>& sorted, double p) {
    if (sorted.empty()) return 0;
    size_t idx = static_cast<size_t>(p * static_cast<double>(sorted.size() - 1) + 0.5);
    if (idx >= sorted.size()) idx = sorted.size() - 1;
    return sorted[idx];
}

struct BackgroundLoad {
    std::atomic<uint64_t> done{ 0 };
    std::atomic<uint64_t> pushed{ 0 };
    std::atomic<bool>     stop{ false };
    std::atomic<uint64_t> failedAllocs{ 0 };
    std::thread           feeder;
    bool                  active = false;

    void Start(JLib::TaskScheduler& sched, size_t inFlightTarget) {
        active = true;
        const uint64_t work = ItersForNs(10'000);
        feeder = std::thread([this, &sched, inFlightTarget, work] {
            while (!stop.load(std::memory_order_relaxed)) {
                const uint64_t out = pushed.load(std::memory_order_relaxed)
                                   - done.load(std::memory_order_relaxed);
                if (out >= inFlightTarget) {
                    std::this_thread::yield();
                    continue;
                }
                auto* t = sched.CreateTask([this, work] {
                    BusySpin(work);
                    done.fetch_add(1, std::memory_order_release);
                });
                if (!t) {                                   // slab exhausted -- record, don't spin hot
                    failedAllocs.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::yield();
                    continue;
                }
                pushed.fetch_add(1, std::memory_order_relaxed);
                sched.Push(t);
            }
        });
    }

    void Stop(JLib::TaskScheduler& sched) {
        if (!active) return;
        stop.store(true, std::memory_order_relaxed);
        if (feeder.joinable()) feeder.join();
        const int64_t deadline = NowNs() + 60LL * 1000 * 1000 * 1000;
        while (done.load(std::memory_order_acquire) < pushed.load(std::memory_order_acquire)) {
            if (NowNs() > deadline) {
                std::fprintf(stderr, "\nFATAL: background load failed to drain (%llu pushed, %llu done)\n",
                             (unsigned long long)pushed.load(), (unsigned long long)done.load());
                std::abort();
            }
            sched.TryRunStolenNativeTask();
        }
        active = false;
    }
};

struct Scenario {
    const char* name;
    int  csNs;          
    int  contenders;
    bool fiberCallers;  
    bool bgLoad;
};

struct ArmSpec { const char* label; int spin; };

static const ArmSpec kArms[] = {
    { "0",     0 },
    { "16",    16 },
    { "64",    64 },
    { "64ctl", 64 },
    { "256",   256 },
    { "1024",  1024 },
};
static constexpr size_t kNumArms = sizeof(kArms) / sizeof(kArms[0]);

struct RunResult {
    uint64_t acquisitions = 0;
    uint64_t bgDone       = 0;
    double   seconds      = 0.0;
    uint32_t p50 = 0, p99 = 0;
};

struct RunState {
    JLib::SchedulerMutex  mtx;
    std::atomic<bool>     stop{ false };
    std::atomic<uint64_t> acquisitions{ 0 };
    std::atomic<bool>     go{ false };
    uint64_t              csIters = 0;
    bool                  measureLatency = false;
};

static void ContendLoop(RunState& st, std::vector<uint32_t>& latOut) {
    while (!st.go.load(std::memory_order_acquire)) std::this_thread::yield();

    uint64_t local = 0;
    if (st.measureLatency) {
        while (!st.stop.load(std::memory_order_relaxed)) {
            const int64_t t0 = NowNs();
            st.mtx.Lock();
            const int64_t t1 = NowNs();
            BusySpin(st.csIters);
            st.mtx.Unlock();
            ++local;
            
            if (latOut.size() < (1u << 19)) {
                const int64_t d = t1 - t0;
                latOut.push_back(static_cast<uint32_t>(d < 0 ? 0 : d));
            }
        }
    }
    else {
        while (!st.stop.load(std::memory_order_relaxed)) {
            st.mtx.Lock();
            BusySpin(st.csIters);
            st.mtx.Unlock();
            ++local;
        }
    }
    st.acquisitions.fetch_add(local, std::memory_order_relaxed);
}

static RunResult RunOne(JLib::TaskScheduler& sched, const Scenario& sc,
                        int windowMs, bool measureLatency, BackgroundLoad& bg) {
    RunState st;
    st.csIters = ItersForNs(sc.csNs);
    st.measureLatency = measureLatency;

    std::vector<std::vector<uint32_t>> lat(static_cast<size_t>(sc.contenders));
    if (measureLatency) for (auto& v : lat) v.reserve(1 << 14);

    std::vector<std::thread> threads;
    JLib::WaitGroup wg;
    // Fiber tasks, not lambdas: the contenders suspend on SchedulerMutex, and a lambda task may not.
    struct Contender { RunState* st; std::vector<uint32_t>* lat; };
    std::vector<Contender> contenders(static_cast<size_t>(sc.contenders));
    if (sc.fiberCallers) {
        wg.n.store(sc.contenders, std::memory_order_relaxed);
        for (int i = 0; i < sc.contenders; ++i) {
            contenders[static_cast<size_t>(i)] = { &st, &lat[static_cast<size_t>(i)] };
            auto* t = sched.CreateTask(
                +[](void* p) { auto* c = static_cast<Contender*>(p); ContendLoop(*c->st, *c->lat); },
                &contenders[static_cast<size_t>(i)], JLib::TaskType::Fiber);
            if (!t) {
                std::fprintf(stderr, "FATAL: could not allocate fiber contender task\n");
                std::abort();
            }
            t->waitGroup = &wg;
            sched.Push(t);
        }
    }
    else {
        threads.reserve(static_cast<size_t>(sc.contenders));
        for (int i = 0; i < sc.contenders; ++i) {
            threads.emplace_back([&st, &lat, i] { ContendLoop(st, lat[static_cast<size_t>(i)]); });
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    const uint64_t bgAtStart = bg.done.load(std::memory_order_acquire);
    const int64_t  t0 = NowNs();
    st.go.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(windowMs));
    st.stop.store(true, std::memory_order_relaxed);
    const int64_t  t1 = NowNs();
    const uint64_t bgAtEnd = bg.done.load(std::memory_order_acquire);

    if (sc.fiberCallers) sched.WaitFor(wg);
    else                 for (auto& th : threads) th.join();

    RunResult r;
    r.acquisitions = st.acquisitions.load(std::memory_order_relaxed);
    r.bgDone  = bgAtEnd - bgAtStart;
    r.seconds = static_cast<double>(t1 - t0) / 1e9;

    if (measureLatency) {
        std::vector<uint32_t> all;
        for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
        std::sort(all.begin(), all.end());
        r.p50 = Pct(all, 0.50);
        r.p99 = Pct(all, 0.99);
    }
    return r;
}

struct ArmSamples {
    std::vector<double> acqPerSec, bgPerSec, p50, p99;
};

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    const size_t poolSize = (argc > 1) ? static_cast<size_t>(std::atoi(argv[1])) : 0;
    const int    windowMs = (argc > 2) ? std::atoi(argv[2]) : 120;
    const int    rounds   = (argc > 3) ? std::atoi(argv[3]) : 7;

    Calibrate();
    JLib::TaskScheduler::Init(poolSize);
    auto& sched = JLib::TaskScheduler::Instance();
    const size_t workers = sched.GetWorkerCount();

    std::printf("JLib::Scheduler lock contention -- workers=%zu  window=%dms  rounds=%d  "
                "calib=%.2f iters/ns\n", workers, windowMs, rounds, g_itersPerNs);
    std::printf("arms rotate INSIDE this process; '64' and '64ctl' are the same value "
                "(their gap is the noise floor)\n\n");

    const Scenario scenarios[] = {
        
        { "uncontended",      200,   1, false, false },
        { "uncontended",      200,   1, false, true  },
        
        { "tiny-cs(0ns)",     0,     2, false, false },
        { "tiny-cs(0ns)",     0,     2, false, true  },
        { "tiny-cs(0ns)",     0,     8, false, true  },
        { "tiny-cs(0ns)",     0,    16, false, true  },
        { "short-cs(200ns)",  200,   2, false, false },
        { "short-cs(200ns)",  200,   2, false, true  },
        { "short-cs(200ns)",  200,   8, false, true  },
        { "short-cs(200ns)",  200,  16, false, true  },
        
        { "moderate-cs(2us)", 2000,  8, false, false },
        { "moderate-cs(2us)", 2000,  8, false, true  },
        { "long-cs(50us)",    50000, 8, false, false },
        { "long-cs(50us)",    50000, 8, false, true  },
        
        { "tiny-cs(0ns)",     0,     8, true,  true  },
    };
    const size_t kNumScenarios = sizeof(scenarios) / sizeof(scenarios[0]);

    for (size_t si = 0; si < kNumScenarios; ++si) {
        const Scenario& sc = scenarios[si];

        BackgroundLoad bg;
        if (sc.bgLoad) {
            bg.Start(sched, workers * 4);
            
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        ArmSamples samples[kNumArms];

        for (int round = 0; round < rounds; ++round) {
            for (size_t k = 0; k < kNumArms; ++k) {
                
                const size_t ai = (k + static_cast<size_t>(round)) % kNumArms;
                sched.SetFastSpinTries(kArms[ai].spin);

                const RunResult lat = RunOne(sched, sc, windowMs,  true,  bg);
                const RunResult thr = RunOne(sched, sc, windowMs,  false, bg);

                samples[ai].acqPerSec.push_back(thr.seconds > 0 ? thr.acquisitions / thr.seconds : 0.0);
                samples[ai].bgPerSec .push_back(thr.seconds > 0 ? thr.bgDone / thr.seconds : 0.0);
                samples[ai].p50      .push_back(lat.p50);
                samples[ai].p99      .push_back(lat.p99);
            }
        }

        if (sc.bgLoad) bg.Stop(sched);
        sched.SetFastSpinTries(64);

        std::printf("%s  contenders=%d  caller=%s  bg=%s\n",
                    sc.name, sc.contenders, sc.fiberCallers ? "fiber" : "bare",
                    sc.bgLoad ? "on" : "off");
        std::printf("  %-7s %10s %10s %12s %12s   %8s %8s\n",
                    "arm", "p50ns", "p99ns", "acq/s", "bgtask/s", "acqR", "bgR");

        double base[4] = { 0, 0, 0, 0 };   
        for (size_t ai = 0; ai < kNumArms; ++ai) {
            if (std::string(kArms[ai].label) != "64") continue;
            base[0] = Median(samples[ai].acqPerSec);
            base[1] = Median(samples[ai].bgPerSec);
            base[2] = Median(samples[ai].p50);
            base[3] = Median(samples[ai].p99);
        }

        double ctlAcqRatio = 1.0, ctlBgRatio = 1.0;
        for (size_t ai = 0; ai < kNumArms; ++ai) {
            const double a = Median(samples[ai].acqPerSec);
            const double b = Median(samples[ai].bgPerSec);
            const double m50 = Median(samples[ai].p50);
            const double m99 = Median(samples[ai].p99);
            const double ar = base[0] > 0 ? a / base[0] : 0.0;
            const double br = base[1] > 0 ? b / base[1] : 0.0;

            if (std::string(kArms[ai].label) == "64ctl") { ctlAcqRatio = ar; ctlBgRatio = br; }

            char brBuf[16];
            if (sc.bgLoad) std::snprintf(brBuf, sizeof brBuf, "%8.3f", br);
            else           std::snprintf(brBuf, sizeof brBuf, "%8s", "-");
            std::printf("  %-7s %10.0f %10.0f %12.0f %12.0f   %8.3f %s\n",
                        kArms[ai].label, m50, m99, a, b, ar, brBuf);
            
            std::printf("CSV,%zu,%s,%d,%d,%s,%s,%s,%.0f,%.0f,%.0f,%.0f\n",
                        workers, sc.name, sc.csNs, sc.contenders,
                        sc.fiberCallers ? "fiber" : "bare", sc.bgLoad ? "on" : "off",
                        kArms[ai].label, m50, m99, a, b);
        }

        if (sc.bgLoad) {
            std::printf("  [noise floor, 64ctl vs 64:  acq %+.1f%%   bg %+.1f%%]\n\n",
                        (ctlAcqRatio - 1.0) * 100.0, (ctlBgRatio - 1.0) * 100.0);
        }
        else {
            std::printf("  [noise floor, 64ctl vs 64:  acq %+.1f%%]\n\n",
                        (ctlAcqRatio - 1.0) * 100.0);
        }
    }

    return 0;
}
