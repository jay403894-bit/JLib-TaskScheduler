// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <vector>

#include "platform.h"

// The stats build (JLIBSCHED_STATS). Every thread owns one row and is its only writer, so a bump is
// a load and a store with no lock prefix, and a reader can snapshot at any time. Cheap events are
// counted. Slow ones (microseconds and up) go into per-thread log2 histograms of raw clock ticks,
// converted to time only when reported. Without JLIBSCHED_STATS every macro is empty and the
// snapshot is all zeros.

namespace JLib {

    enum class Stat : std::uint8_t {
        RunOwnDeque, RunInbox, RunHiPri, RunStolen, RunMainQueue, RunLaneIntake, RunHelper,
        RunLambda, RunFiber, RunCoroutine,
        InboxStaged,
        StealProbes, StealHits,
        StickyProbes, StickyHits, StealRemoteProbes, SeekProbes, SeekHits,
        WorkFlagWrites,
        Blocks, BlocksNoAdopter,
        HuntEntered,
        Parks, WakesSent,
        Pushes, PushBatches, PushBatchTasks,
        TaskHeapAllocs,
        Suspends, Yields,
        ResumePinned, ResumeLocal, ResumePlaced,
        NoFiberRequeue,
        FiberPoolGrowths, FiberPoolLimitHits,
        SlabAllocs, SlabFrees, SlabRefills, SlabFlushes, SlabGrowths,
        Retired, ReclaimPasses,
        PeriodicFires, PeriodicSkipped,
        CoroFrames, CoroFramesHeap,
        Count
    };

    enum class Hist : std::uint8_t {
        TaskSegment,    // one run of a task until it returns, suspends or yields (ticks)
        TaskLife,       // first run to completion, suspensions included (ticks)
        Suspended,      // suspension or yield to the next run (ticks)
        WaitFor,        // one TaskScheduler::WaitFor call on the waiting thread (ticks)
        ReclaimPass,    // one Reclaimer::Gate (ticks)
        StealScan,      // one steal pass, hit or miss, sampled 1 in 64 (ticks)
        TaskBytes,      // task object size at creation (bytes)
        CoroFrameBytes, // coroutine frame size (bytes)
        Count
    };

    inline constexpr std::size_t kStatCount   = (std::size_t)Stat::Count;
    inline constexpr std::size_t kHistCount   = (std::size_t)Hist::Count;
    inline constexpr std::size_t kHistBuckets = 64;   // bucket b holds values in [2^(b-1), 2^b); 0 holds 0

    const char* StatName(Stat s) noexcept;
    const char* HistName(Hist h) noexcept;
    bool        HistIsTime(Hist h) noexcept;

    struct StatsSnapshot {
        struct Row {
            int         qIndex = -1;
            const char* role   = "thread";
            std::uint64_t c[kStatCount] = {};
            std::uint64_t h[kHistCount][kHistBuckets] = {};
        };

        bool             enabled = false;
        double           ticksPerSecond = 0.0;
        std::vector<Row> rows;     // one per live thread that recorded anything, plus exited threads
        Row              total;

        std::uint64_t Count(Stat s) const noexcept { return total.c[(std::size_t)s]; }
        std::uint64_t Samples(Hist h) const noexcept;
        // Upper bound of the bucket holding the p-th percentile (0..1), in the histogram's unit.
        std::uint64_t Percentile(Hist h, double p) const noexcept;
        double        TicksToMicros(std::uint64_t ticks) const noexcept;

        void Print(std::FILE* out = stdout) const;
    };

    namespace Stats {
        bool          Enabled() noexcept;
        // Current totals minus the last Reset(). Rows cannot be cleared from another thread, so
        // Reset records a baseline instead.
        StatsSnapshot Snapshot();
        void          Reset();
        void          Print(std::FILE* out = stdout);
    }

#if defined(JLIBSCHED_STATS)
    namespace detail {

        struct StatRow {
            std::atomic<std::uint64_t> c[kStatCount] = {};
            std::atomic<std::uint64_t> h[kHistCount][kHistBuckets] = {};
            std::uint32_t sample = 0;
            int           qIndex = -1;
            const char*   role   = "thread";
        };

        JLIB_NOINLINE StatRow& StatRowForThread() noexcept;   // TLS: see JLIB_NOINLINE
        // Labels the calling thread's row in reports.
        void StatLabelThread(int qIndex, const char* role) noexcept;

        inline void StatBump(std::atomic<std::uint64_t>& a, std::uint64_t n) noexcept {
            a.store(a.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
        }

        inline unsigned StatBucket(std::uint64_t v) noexcept {
            if (v == 0) return 0;
            const unsigned b = 64u - platform::CountLeadingZeros64(v);
            return b < kHistBuckets ? b : (unsigned)kHistBuckets - 1;
        }

        inline void StatRecord(Hist h, std::uint64_t v) noexcept {
            StatBump(StatRowForThread().h[(std::size_t)h][StatBucket(v)], 1);
        }

        // One in 64 on the calling thread.
        inline bool StatSampleTick() noexcept {
            return (++StatRowForThread().sample & 63u) == 0;
        }

#if JLIB_ARCH_X86_64
        inline std::uint64_t StatTicks() noexcept { return platform::ReadTsc(); }
#else
        std::uint64_t StatTicks() noexcept;   // the ARM virtual counter (src/Stats.cpp)
#endif
    }

    #define JLIB_STAT(name)       ::JLib::detail::StatBump(::JLib::detail::StatRowForThread().c[(std::size_t)::JLib::Stat::name], 1)
    #define JLIB_STAT_N(name, n)  ::JLib::detail::StatBump(::JLib::detail::StatRowForThread().c[(std::size_t)::JLib::Stat::name], (std::uint64_t)(n))
    #define JLIB_STAT_HIST(name, v) ::JLib::detail::StatRecord(::JLib::Hist::name, (std::uint64_t)(v))
    #define JLIB_STAT_TICKS()     ::JLib::detail::StatTicks()
    #define JLIB_STAT_SAMPLE()    ::JLib::detail::StatSampleTick()
    #define JLIB_STAT_ONLY(...)   __VA_ARGS__
#else
    #define JLIB_STAT(name)         ((void)0)
    #define JLIB_STAT_N(name, n)    ((void)0)
    #define JLIB_STAT_HIST(name, v) ((void)0)
    #define JLIB_STAT_TICKS()       (std::uint64_t)0
    #define JLIB_STAT_SAMPLE()      false
    #define JLIB_STAT_ONLY(...)
#endif

}
