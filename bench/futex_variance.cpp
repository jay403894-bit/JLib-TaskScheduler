// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <windows.h>
#include <synchapi.h>
#include <timeapi.h>   
#pragma comment(lib, "Synchronization.lib")   
#pragma comment(lib, "winmm.lib")             
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <condition_variable>
#include <mutex>

using Clock = std::chrono::steady_clock;
static int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

struct CondvarWaiter {
    std::mutex m;
    std::condition_variable cv;
    bool ready = false;

    void Wait() {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [this] { return ready; });
        ready = false;
    }
    void Signal() {
        { std::lock_guard<std::mutex> lock(m); ready = true; }
        cv.notify_one();
    }
    static const char* Name() { return "condvar"; }
};

struct FutexWaiter {
    std::atomic<LONG> flag{ 0 };

    void Wait() {
        LONG expected = 0;
        
        while (flag.load(std::memory_order_acquire) == 0) {
            WaitOnAddress(&flag, &expected, sizeof(LONG), INFINITE);
        }
        flag.store(0, std::memory_order_relaxed);
    }
    void Signal() {
        flag.store(1, std::memory_order_release);
        WakeByAddressSingle(&flag);
    }
    static const char* Name() { return "futex"; }
};

template <typename WaiterT>
static std::vector<double> RunRound(int reps, int parkDelayUs) {
    WaiterT waiter;
    std::atomic<bool> aboutToWait{ false };
    std::atomic<int64_t> wakeTimeNs{ 0 };

    std::thread waiterThread([&] {
        for (int i = 0; i < reps; ++i) {
            aboutToWait.store(true, std::memory_order_release);
            waiter.Wait();
            wakeTimeNs.store(NowNs(), std::memory_order_release);
        }
        });

    std::vector<double> latenciesUs;
    latenciesUs.reserve(reps);
    for (int i = 0; i < reps; ++i) {
        while (!aboutToWait.load(std::memory_order_acquire)) std::this_thread::yield();
        aboutToWait.store(false, std::memory_order_relaxed);
        if (parkDelayUs > 0) std::this_thread::sleep_for(std::chrono::microseconds(parkDelayUs));

        const int64_t t0 = NowNs();
        waiter.Signal();

        int64_t t1;
        while ((t1 = wakeTimeNs.exchange(0, std::memory_order_acq_rel)) == 0) std::this_thread::yield();
        latenciesUs.push_back((double)(t1 - t0) / 1000.0);
    }

    waiterThread.join();
    return latenciesUs;
}

struct Stats {
    double median = 0, p90 = 0, p99 = 0, max = 0, mean = 0, stddev = 0;
};
static Stats Summarize(std::vector<double> v) {
    Stats s;
    if (v.empty()) return s;
    std::sort(v.begin(), v.end());
    auto pct = [&](double p) { size_t i = (size_t)(p * (v.size() - 1)); return v[i]; };
    s.median = pct(0.50);
    s.p90 = pct(0.90);
    s.p99 = pct(0.99);
    s.max = v.back();
    double sum = 0; for (double x : v) sum += x;
    s.mean = sum / v.size();
    double sq = 0; for (double x : v) sq += (x - s.mean) * (x - s.mean);
    s.stddev = std::sqrt(sq / v.size());
    return s;
}

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);   
    
    timeBeginPeriod(1);

    const int reps = argc > 1 ? std::atoi(argv[1]) : 500;
    const int rounds = argc > 2 ? std::atoi(argv[2]) : 10;
    const int parkDelayUs = argc > 3 ? std::atoi(argv[3]) : 200;

    printf("futex_variance: %d reps/round x %d rounds, %d us park delay, arms alternated per round\n",
        reps, rounds, parkDelayUs);
    printf("(park delay is realism, not correctness -- WaitOnAddress cannot lose a signal to this race)\n\n");

    std::vector<double> condvarAll, futexAll;
    for (int r = 0; r < rounds; ++r) {
        auto c = RunRound<CondvarWaiter>(reps, parkDelayUs);
        auto f = RunRound<FutexWaiter>(reps, parkDelayUs);
        condvarAll.insert(condvarAll.end(), c.begin(), c.end());
        futexAll.insert(futexAll.end(), f.begin(), f.end());

        Stats cs = Summarize(c), fs = Summarize(f);
        printf("round %2d  condvar: median=%6.2f p90=%6.2f p99=%6.2f max=%7.2f stddev=%6.2f us"
            "   futex: median=%6.2f p90=%6.2f p99=%6.2f max=%7.2f stddev=%6.2f us\n",
            r, cs.median, cs.p90, cs.p99, cs.max, cs.stddev,
            fs.median, fs.p90, fs.p99, fs.max, fs.stddev);
    }

    Stats cAll = Summarize(condvarAll), fAll = Summarize(futexAll);
    printf("\n=== combined (%zu reps each) ===\n", condvarAll.size());
    printf("condvar: median=%.2f us  p90=%.2f  p99=%.2f  max=%.2f  mean=%.2f  stddev=%.2f (%.1f%% of mean)\n",
        cAll.median, cAll.p90, cAll.p99, cAll.max, cAll.mean, cAll.stddev, 100.0 * cAll.stddev / cAll.mean);
    printf("futex  : median=%.2f us  p90=%.2f  p99=%.2f  max=%.2f  mean=%.2f  stddev=%.2f (%.1f%% of mean)\n",
        fAll.median, fAll.p90, fAll.p99, fAll.max, fAll.mean, fAll.stddev, 100.0 * fAll.stddev / fAll.mean);
    printf("\nratio (condvar/futex, >1.0 means futex is TIGHTER): stddev %.2fx   p99 %.2fx   median %.2fx\n",
        cAll.stddev / fAll.stddev, cAll.p99 / fAll.p99, cAll.median / fAll.median);

    timeEndPeriod(1);
    return 0;
}
