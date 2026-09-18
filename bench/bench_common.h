// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

// Shared by SchedulerBench and the comparison driver: the workload sizes, the timing loop and the
// table/CSV output, so a case means the same thing and is reported the same way in both.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#endif

namespace bench {

using Clock = std::chrono::steady_clock;

// Workload sizes. Changing one changes it for the bench and for every comparison arm.
constexpr int kSpawnN        = 200000;   // empty tasks, one at a time
constexpr int kBatchChunk    = 1024;     // tasks per bulk submission
constexpr int kPforN         = 1 << 20;  // parallel-for items (fine grain)
constexpr int kPforCoarseN   = kPforN;    // sized so a parallel run is milliseconds, not microseconds:
constexpr int kPforCoarseIters = 128;    // below ~1 ms a median is mostly interference, not work
constexpr int kFibN          = 35;
constexpr int kFibCutoff     = 20;       // below this, run serially
constexpr int kPingTasks     = 256;
constexpr int kPingIters     = 200;
constexpr int kYieldTasks    = 64;
constexpr int kYieldIters    = 2000;
constexpr int kMutexTasks    = 64;
constexpr int kMutexIters    = 2000;
constexpr int kDagN          = 4096;
constexpr int kLatencySamples = 2000;
constexpr int kLatencyGapUs  = 500;      // idle gap before each sample, so workers park

inline std::uint64_t FibSerial(int n) {
    return n < 2 ? (std::uint64_t)n : FibSerial(n - 1) + FibSerial(n - 2);
}
inline constexpr std::uint64_t kFibExpected = 9227465;   // fib(35)

inline std::uint64_t LcgSteps(std::uint64_t x, int iters) {
    for (int i = 0; i < iters; ++i) x = x * 6364136223846793005ull + 1442695040888963407ull;
    return x;
}

inline std::int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

// The idle gap before a latency sample. std::this_thread::sleep_for rounds up to the Windows timer
// tick (15.6 ms unless something raised it), which turned a 500 us gap into ~10 ms and made the
// latency case take twenty times longer than the work in it. Same fix as the scheduler's timer
// thread: a high-resolution waitable timer, which affects nothing outside this process.
#if defined(_WIN32)
inline void PreciseSleepUs(int us) {
    static HANDLE timer = [] {
        HANDLE h = ::CreateWaitableTimerExW(nullptr, nullptr, 0x00000002 /*HIGH_RESOLUTION*/,
                                            TIMER_ALL_ACCESS);
        if (!h) h = ::CreateWaitableTimerExW(nullptr, nullptr, 0, TIMER_ALL_ACCESS);
        return h;
    }();
    if (!timer) { std::this_thread::sleep_for(std::chrono::microseconds(us)); return; }
    LARGE_INTEGER due;
    due.QuadPart = -(LONGLONG)us * 10;   // 100 ns units, negative = relative
    ::SetWaitableTimer(timer, &due, 0, nullptr, nullptr, FALSE);
    ::WaitForSingleObject(timer, INFINITE);
}
#else
inline void PreciseSleepUs(int us) { std::this_thread::sleep_for(std::chrono::microseconds(us)); }
#endif

struct Timing {
    std::vector<double> ms;
    double Median() const { return ms[ms.size() / 2]; }
    double Min() const    { return ms.front(); }
    double Max() const    { return ms.back(); }
};

// One warmup run, then `reps` timed runs. The workload returns how many operations it did.
template <typename F>
Timing TimeReps(int reps, F&& run, std::uint64_t& ops) {
    run();
    Timing t;
    for (int r = 0; r < reps; ++r) {
        const auto t0 = Clock::now();
        ops = run();
        t.ms.push_back(std::chrono::duration<double, std::milli>(Clock::now() - t0).count());
    }
    std::sort(t.ms.begin(), t.ms.end());
    return t;
}

inline void PrintHeader() {
    std::printf("  %-13s %-6s %12s %12s %12s %14s\n",
                "case", "kind", "median ms", "min ms", "max ms", "ops/s");
}

// rate=false for cases whose run time is mostly idle gaps or a fixed duration; they report their
// own metric in `extra` instead of a throughput that would mean nothing.
inline void PrintRow(const char* name, const char* kind, const Timing& t, std::uint64_t ops,
                     bool rate, const char* extra = "") {
    const double med = t.Median();
    char opsText[32] = "-";
    if (rate && med > 0) std::snprintf(opsText, sizeof opsText, "%.0f", (double)ops * 1000.0 / med);
    std::printf("  %-13s %-6s %12.3f %12.3f %12.3f %14s  %s\n",
                name, kind, med, t.Min(), t.Max(), opsText, extra);
}

inline void PrintSkip(const char* name, const char* why) {
    std::printf("  %-13s %-6s %12s  %s\n", name, "-", "n/a", why);
}

// Appends rows to a CSV, writing `header` first if the file is new or empty.
class Csv {
public:
    void Open(const std::string& path, const char* header) {
        if (path.empty()) return;
        bool empty = true;
        if (std::FILE* probe = std::fopen(path.c_str(), "rb")) {
            empty = std::fgetc(probe) == EOF;
            std::fclose(probe);
        }
        f_ = std::fopen(path.c_str(), "ab");
        if (f_ && empty) std::fprintf(f_, "%s\n", header);
    }
    ~Csv() { if (f_) std::fclose(f_); }
    std::FILE* File() const { return f_; }
    explicit operator bool() const { return f_ != nullptr; }
    void Flush() { if (f_) std::fflush(f_); }
private:
    std::FILE* f_ = nullptr;
};

}   // namespace bench
