// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Stats.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <thread>

namespace JLib {

    const char* StatName(Stat s) noexcept {
        static const char* const k[kStatCount] = {
            "run: own deque", "run: inbox", "run: hi-pri inbox", "run: stolen", "run: main queue",
            "run: injector", "run: main helper",
            "run: lambda", "run: fiber", "run: coroutine",
            "inbox staged to deque",
            "steal probes", "steal hits",
            "sticky steal probes (same victim again)", "sticky steal hits",
            "steal probes into another L3 group",
            "seek probes (flag scan after a cursor miss)", "seek hits",
            "work flag writes (deque empty <-> non-empty)",
            "blocks (BlockBegin)",
            "hunts entered",
            "parks", "wakes sent",
            "pushes", "push batches", "push batch tasks",
            "tasks allocated on the heap (> 512 B)",
            "suspends (incl. yields)", "yields",
            "resume: pinned inbox", "resume: own deque", "resume: placed",
            "requeued for want of a fiber",
            "fiber pool growths", "fiber pool limit hits",
            "slab allocs", "slab frees", "slab refills", "slab flushes", "slab growths",
            "retired", "reclaim passes",
            "periodic fires", "periodic skipped",
            "coroutine frames", "coroutine frames on heap",
        };
        return (std::size_t)s < kStatCount ? k[(std::size_t)s] : "?";
    }

    const char* HistName(Hist h) noexcept {
        static const char* const k[kHistCount] = {
            "task segment", "task life", "suspended", "WaitFor", "reclaim pass",
            "steal scan (1/64)", "task bytes", "coroutine frame bytes",
        };
        return (std::size_t)h < kHistCount ? k[(std::size_t)h] : "?";
    }

    bool HistIsTime(Hist h) noexcept {
        return h != Hist::TaskBytes && h != Hist::CoroFrameBytes;
    }

    std::uint64_t StatsSnapshot::Samples(Hist h) const noexcept {
        std::uint64_t n = 0;
        for (std::uint64_t v : total.h[(std::size_t)h]) n += v;
        return n;
    }

    std::uint64_t StatsSnapshot::Percentile(Hist h, double p) const noexcept {
        const std::uint64_t n = Samples(h);
        if (n == 0) return 0;
        std::uint64_t want = (std::uint64_t)(p * (double)n);
        if (want >= n) want = n - 1;
        std::uint64_t seen = 0;
        for (std::size_t b = 0; b < kHistBuckets; ++b) {
            seen += total.h[(std::size_t)h][b];
            if (seen > want) return b == 0 ? 0 : (std::uint64_t(1) << (b < 64 ? b : 63));
        }
        return std::uint64_t(1) << 63;
    }

    double StatsSnapshot::TicksToMicros(std::uint64_t ticks) const noexcept {
        return ticksPerSecond > 0 ? (double)ticks * 1e6 / ticksPerSecond : 0.0;
    }

#if defined(JLIBSCHED_STATS)

#if !JLIB_ARCH_X86_64
  #if defined(_MSC_VER)
    std::uint64_t detail::StatTicks() noexcept {
        // CNTVCT_EL0: ARM64_SYSREG(3, 3, 14, 0, 2)
        return (std::uint64_t)_ReadStatusReg((1 << 14) | (3 << 11) | (14 << 7) | (0 << 3) | 2);
    }
  #else
    extern "C" std::uint64_t jlib_read_cntvct(void);
    __asm__(".text\n"
            ".global jlib_read_cntvct\n"
            ".type jlib_read_cntvct, %function\n"
            "jlib_read_cntvct:\n"
            "    mrs x0, cntvct_el0\n"
            "    ret\n");
    std::uint64_t detail::StatTicks() noexcept { return jlib_read_cntvct(); }
  #endif
#endif

    namespace {
        using SteadyClock = std::chrono::steady_clock;

        void Accumulate(StatsSnapshot::Row& into, const detail::StatRow& r) {
            for (std::size_t i = 0; i < kStatCount; ++i)
                into.c[i] += r.c[i].load(std::memory_order_relaxed);
            for (std::size_t h = 0; h < kHistCount; ++h)
                for (std::size_t b = 0; b < kHistBuckets; ++b)
                    into.h[h][b] += r.h[h][b].load(std::memory_order_relaxed);
        }
        void Accumulate(StatsSnapshot::Row& into, const StatsSnapshot::Row& r) {
            for (std::size_t i = 0; i < kStatCount; ++i) into.c[i] += r.c[i];
            for (std::size_t h = 0; h < kHistCount; ++h)
                for (std::size_t b = 0; b < kHistBuckets; ++b) into.h[h][b] += r.h[h][b];
        }
        void Subtract(StatsSnapshot::Row& from, const StatsSnapshot::Row& r) {
            for (std::size_t i = 0; i < kStatCount; ++i) from.c[i] -= std::min(from.c[i], r.c[i]);
            for (std::size_t h = 0; h < kHistCount; ++h)
                for (std::size_t b = 0; b < kHistBuckets; ++b)
                    from.h[h][b] -= std::min(from.h[h][b], r.h[h][b]);
        }

        struct LiveRow {
            detail::StatRow*  row;
            StatsSnapshot::Row baseline;   // what Reset saw for this row
        };

        struct Registry {
            std::mutex           m;
            std::vector<LiveRow> live;
            StatsSnapshot::Row   exited;           // folded in when a thread ends
            StatsSnapshot::Row   exitedBaseline;
            std::uint64_t        tick0 = detail::StatTicks();
            SteadyClock::time_point clock0 = SteadyClock::now();
        };
        Registry& Reg() {
            static Registry* r = new Registry();   // outlives every thread_local below
            return *r;
        }

        // Set once this thread's row is gone; later bumps (another thread_local's destructor freeing
        // slab memory, say) land in a shared scratch row that is never reported.
        thread_local bool t_rowDead = false;

        struct RowHolder {
            detail::StatRow* row = nullptr;
            ~RowHolder() {
                t_rowDead = true;
                if (!row) return;
                Registry& g = Reg();
                std::lock_guard<std::mutex> lk(g.m);
                for (auto it = g.live.begin(); it != g.live.end(); ++it) {
                    if (it->row != row) continue;
                    Accumulate(g.exited, *row);
                    Accumulate(g.exitedBaseline, it->baseline);
                    g.live.erase(it);
                    break;
                }
                delete row;
            }
        };

        double TicksPerSecond(Registry& g) {
            auto elapsed = SteadyClock::now() - g.clock0;
            if (elapsed < std::chrono::milliseconds(50)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                elapsed = SteadyClock::now() - g.clock0;
            }
            const std::uint64_t ticks = detail::StatTicks() - g.tick0;
            return (double)ticks / std::chrono::duration<double>(elapsed).count();
        }
    }

    detail::StatRow& detail::StatRowForThread() noexcept {
        if (t_rowDead) {
            static StatRow scratch;
            return scratch;
        }
        static thread_local RowHolder holder;
        if (!holder.row) {
            holder.row = new StatRow();
            Registry& g = Reg();
            std::lock_guard<std::mutex> lk(g.m);
            g.live.push_back(LiveRow{ holder.row, {} });
        }
        return *holder.row;
    }

    void detail::StatLabelThread(int qIndex, const char* role) noexcept {
        StatRow& r = StatRowForThread();
        r.qIndex = qIndex;
        r.role   = role;
    }

    bool Stats::Enabled() noexcept { return true; }

    StatsSnapshot Stats::Snapshot() {
        StatsSnapshot s;
        s.enabled = true;
        Registry& g = Reg();
        s.ticksPerSecond = TicksPerSecond(g);
        std::lock_guard<std::mutex> lk(g.m);
        for (const LiveRow& l : g.live) {
            StatsSnapshot::Row row;
            row.qIndex = l.row->qIndex;
            row.role   = l.row->role;
            Accumulate(row, *l.row);
            Subtract(row, l.baseline);
            Accumulate(s.total, row);
            s.rows.push_back(row);
        }
        StatsSnapshot::Row gone;
        gone.role = "exited threads";
        Accumulate(gone, g.exited);
        Subtract(gone, g.exitedBaseline);
        Accumulate(s.total, gone);
        s.rows.push_back(gone);
        std::sort(s.rows.begin(), s.rows.end(), [](const StatsSnapshot::Row& a, const StatsSnapshot::Row& b) {
            const bool aw = a.qIndex >= 0, bw = b.qIndex >= 0;
            if (aw != bw) return aw;
            return aw ? a.qIndex < b.qIndex : false;
        });
        return s;
    }

    void Stats::Reset() {
        Registry& g = Reg();
        std::lock_guard<std::mutex> lk(g.m);
        for (LiveRow& l : g.live) {
            l.baseline = StatsSnapshot::Row{};
            Accumulate(l.baseline, *l.row);
        }
        g.exitedBaseline = g.exited;
    }

#else

    bool Stats::Enabled() noexcept { return false; }
    StatsSnapshot Stats::Snapshot() { return StatsSnapshot{}; }
    void Stats::Reset() {}

#endif

    void Stats::Print(std::FILE* out) { Snapshot().Print(out); }

    namespace {
        void FormatValue(char* buf, std::size_t n, const StatsSnapshot& s, Hist h, std::uint64_t v) {
            if (!HistIsTime(h)) { std::snprintf(buf, n, "%llu B", (unsigned long long)v); return; }
            const double us = s.TicksToMicros(v);
            if (us < 1000.0)          std::snprintf(buf, n, "%.1f us", us);
            else if (us < 1000000.0)  std::snprintf(buf, n, "%.2f ms", us / 1000.0);
            else                      std::snprintf(buf, n, "%.2f s", us / 1e6);
        }
    }

    void StatsSnapshot::Print(std::FILE* out) const {
        if (!enabled) {
            std::fprintf(out, "[JLib::Scheduler] stats not compiled in -- configure with -DJLIBSCHED_STATS=ON\n");
            return;
        }
        std::fprintf(out, "JLib::Scheduler stats (%zu threads, %.0f ticks/s)\n", rows.size() - 1, ticksPerSecond);

        std::fprintf(out, "  counters\n");
        for (std::size_t i = 0; i < kStatCount; ++i) {
            if (!total.c[i]) continue;
            std::fprintf(out, "    %-30s %14llu\n", StatName((Stat)i), (unsigned long long)total.c[i]);
        }

        // Per-thread spread of the counters that show balance.
        static constexpr Stat kSpread[] = {
            Stat::RunLambda, Stat::RunFiber, Stat::RunCoroutine, Stat::RunInbox,
            Stat::StealHits, Stat::Parks, Stat::Suspends,
        };
        std::fprintf(out, "  %-26s %10s %10s %10s %10s %10s %10s %10s\n", "per thread",
                     "lambda", "fiber", "coro", "inbox", "steal hit", "parks", "suspends");
        for (const Row& r : rows) {
            bool any = false;
            for (Stat s : kSpread) any = any || r.c[(std::size_t)s];
            if (!any) continue;
            char label[40];
            if (r.qIndex >= 0) std::snprintf(label, sizeof label, "%s %d", r.role, r.qIndex);
            else               std::snprintf(label, sizeof label, "%s", r.role);
            std::fprintf(out, "    %-24s", label);
            for (Stat s : kSpread) std::fprintf(out, " %10llu", (unsigned long long)r.c[(std::size_t)s]);
            std::fprintf(out, "\n");
        }

        std::fprintf(out, "  histograms (log2 buckets: each value is its bucket's upper bound)\n");
        std::fprintf(out, "    %-24s %10s %10s %10s %10s %10s\n", "", "samples", "p50", "p90", "p99", "max");
        std::fprintf(out, "    (times are clock ticks converted at report time)\n");
        for (std::size_t i = 0; i < kHistCount; ++i) {
            const Hist h = (Hist)i;
            const std::uint64_t n = Samples(h);
            if (!n) continue;
            char p50[24], p90[24], p99[24], pmax[24];
            FormatValue(p50,  sizeof p50,  *this, h, Percentile(h, 0.50));
            FormatValue(p90,  sizeof p90,  *this, h, Percentile(h, 0.90));
            FormatValue(p99,  sizeof p99,  *this, h, Percentile(h, 0.99));
            FormatValue(pmax, sizeof pmax, *this, h, Percentile(h, 1.0));
            std::fprintf(out, "    %-24s %10llu %10s %10s %10s %10s\n", HistName(h),
                         (unsigned long long)n, p50, p90, p99, pmax);
        }
        std::fflush(out);
    }
}
