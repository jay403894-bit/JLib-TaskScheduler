// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Timer.h"
#include "../include/Stats.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
#include "../include/platform.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#if JLIB_PLATFORM_LINUX
#include <sys/resource.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace JLib {

    int64_t MonotonicNs() noexcept {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    }

    void EjectEvent(void* ctx, CancelToken token) {
        if (ctx) static_cast<Event*>(ctx)->CancelWaiters();
        (void)token;   
    }

    void EjectSemaphore(void* ctx, CancelToken token) {
        if (ctx) static_cast<SchedulerSemaphore*>(ctx)->CancelWaiters(token);
    }

    void EjectConditionVariable(void* ctx, CancelToken token) {
        if (ctx) static_cast<SchedulerConditionVariable*>(ctx)->CancelWaiters(token);
    }

    // How the timer thread sleeps until the next deadline.
    //
    // On Windows a condition variable's timed wait rounds up to the system timer tick -- 15.6 ms
    // unless something raised the global resolution -- so a 1 ms wheel tick fired about every
    // 15 ms and every grid point in between was counted as skipped. A waitable timer created with
    // CREATE_WAITABLE_TIMER_HIGH_RESOLUTION does not round (Windows 10 1803+), and unlike
    // timeBeginPeriod it changes nothing for the rest of the system. The wake event is what lets an
    // arm, a disarm or Stop cut the sleep short; being auto-reset, a wake that arrives just before
    // the wait starts is kept rather than lost.
    //
    // Elsewhere a condition variable already waits to the nanosecond, so this is only a Windows path.
#if JLIB_PLATFORM_WINDOWS && !defined(CREATE_WAITABLE_TIMER_HIGH_RESOLUTION)
#define CREATE_WAITABLE_TIMER_HIGH_RESOLUTION 0x00000002
#endif

    class TimerWaiter {
    public:
#if JLIB_PLATFORM_WINDOWS
        TimerWaiter() {
            timer_ = ::CreateWaitableTimerExW(nullptr, nullptr,
                         CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
            if (!timer_)   // pre-1803: the same timer without the flag, at the system tick
                timer_ = ::CreateWaitableTimerExW(nullptr, nullptr, 0, TIMER_ALL_ACCESS);
            else
                highRes_ = true;
            wake_ = ::CreateEventW(nullptr, FALSE, FALSE, nullptr);
        }
        ~TimerWaiter() {
            if (timer_) ::CloseHandle(timer_);
            if (wake_)  ::CloseHandle(wake_);
        }

        void wait(std::unique_lock<std::mutex>& lk) {
            lk.unlock();
            ::WaitForSingleObject(wake_, INFINITE);
            lk.lock();
        }

        void wait_for(std::unique_lock<std::mutex>& lk, std::chrono::nanoseconds ns) {
            if (ns.count() <= 0) return;
            // 100 ns units, negative = relative.
            LARGE_INTEGER due;
            due.QuadPart = -(LONGLONG)((ns.count() + 99) / 100);
            if (due.QuadPart == 0) due.QuadPart = -1;
            if (!timer_ || !::SetWaitableTimer(timer_, &due, 0, nullptr, nullptr, FALSE)) {
                lk.unlock();
                ::WaitForSingleObject(wake_, (DWORD)std::max<int64_t>(1, ns.count() / 1'000'000));
                lk.lock();
                return;
            }
            HANDLE h[2] = { timer_, wake_ };
            lk.unlock();
            ::WaitForMultipleObjects(2, h, FALSE, INFINITE);
            ::CancelWaitableTimer(timer_);
            lk.lock();
        }

        void notify_one()  { if (wake_) ::SetEvent(wake_); }
        void notify_all()  { notify_one(); }
        bool HighResolution() const { return highRes_; }

    private:
        HANDLE timer_ = nullptr;
        HANDLE wake_  = nullptr;
        bool   highRes_ = false;
#else
        void wait(std::unique_lock<std::mutex>& lk) { cv_.wait(lk); }
        void wait_for(std::unique_lock<std::mutex>& lk, std::chrono::nanoseconds ns) {
            cv_.wait_for(lk, ns);
        }
        void notify_one() { cv_.notify_one(); }
        void notify_all() { cv_.notify_all(); }
        bool HighResolution() const { return true; }

    private:
        std::condition_variable cv_;
#endif
    };

    namespace {

        constexpr int      kLevels    = 4;
        constexpr int      kSlotBits  = 8;
        constexpr uint32_t kSlots     = 1u << kSlotBits;      
        constexpr uint32_t kSlotMask  = kSlots - 1;
        constexpr int      kOccWords  = int(kSlots / 64);     
        constexpr int64_t  kMaxTicks  = int64_t(1) << (kSlotBits * kLevels);   

        constexpr int64_t LevelGranularity(int level) {
            return int64_t(1) << (kSlotBits * level);
        }

    }

    // One periodic task. Fields below `runs` are guarded by the timer mutex.
    struct PeriodicRecord {
        int64_t    interval = 0;          // ns, at least one tick
        int64_t    due      = 0;          // next grid point, MonotonicNs time
        PeriodicFn fn       = nullptr;
        void*      ctx      = nullptr;
        Lane       lane     = Lane::Normal;
        std::atomic<uint64_t> totalSkipped{ 0 };
        WaitGroup  runs;                  // 1 while an instance is in flight (Join waits on it)

        uint32_t   entry          = 0xFFFFFFFFu;   // wheel entry while armed
        uint64_t   pendingSkipped = 0;             // handed to the next run
        bool       inFlight  = false;
        bool       cancelled = false;
        bool       detached  = false;
    };

    struct TimerQueue::Impl {

        struct Entry {
            int64_t    deadlineTick = 0;
            uint32_t   token        = CancelToken::kNone;
            TimerEject eject        = nullptr;
            void*      ctx          = nullptr;
            PeriodicRecord* periodic = nullptr;   // set: a periodic entry (stays armed across fires)

            uint32_t   prev = kNil;
            uint32_t   next = kNil;

            uint32_t   generation = 0;
            uint32_t   nextFree   = 0;   

            int8_t     level = -1;       
            uint32_t   slot  = 0;
        };

        static constexpr uint32_t kNil = 0xFFFFFFFFu;

        mutable std::mutex      m;
        TimerWaiter             cv;   // the timer thread's sleep; any thread may cut it short

        std::vector<Entry> entries;
        uint32_t           freeHead = 0;         

        uint32_t heads[kLevels][kSlots];         
        uint64_t occ[kLevels][kOccWords];        

        int64_t  tickNs     = 1'000'000;         
        int64_t  epochNs    = 0;                 
        int64_t  currentTick = 0;

        size_t   armedCount = 0;
        // The tick the timer thread is sleeping toward: INT64_MAX in its untimed wait, kAwake while
        // it holds the lock or is firing (it recomputes the next event before it sleeps again).
        // An arm notifies only when its entry makes the next event earlier than this.
        static constexpr int64_t kAwake = INT64_MIN;
        int64_t  sleepUntil = kAwake;
        bool     running    = false;
        bool     stopping   = false;
        std::thread worker;

        Impl() {
            for (int l = 0; l < kLevels; ++l) {
                for (uint32_t s = 0; s < kSlots; ++s) heads[l][s] = kNil;
                for (int w = 0; w < kOccWords; ++w) occ[l][w] = 0;
            }
            epochNs = MonotonicNs();
        }

        int64_t NowTick() const { return (MonotonicNs() - epochNs) / tickNs; }

        void MarkOccupied(int l, uint32_t s)  { occ[l][s >> 6] |= (uint64_t(1) << (s & 63)); }
        void MarkEmpty(int l, uint32_t s)     { occ[l][s >> 6] &= ~(uint64_t(1) << (s & 63)); }
        bool LevelEmpty(int l) const {
            for (int w = 0; w < kOccWords; ++w) if (occ[l][w]) return false;
            return true;
        }

        uint32_t NextOccupied(int l, uint32_t from) const {
            const uint32_t startWord = from >> 6;
            const unsigned startBit  = from & 63;

            uint64_t w = occ[l][startWord] & (~uint64_t(0) << startBit);
            if (w) return (startWord << 6) + platform::CountTrailingZeros64(w);

            for (int i = 1; i < kOccWords; ++i) {
                const uint32_t wi = (startWord + uint32_t(i)) % uint32_t(kOccWords);
                w = occ[l][wi];
                if (w) return (wi << 6) + platform::CountTrailingZeros64(w);
            }

            w = occ[l][startWord] & ~(~uint64_t(0) << startBit);
            if (w) return (startWord << 6) + platform::CountTrailingZeros64(w);
            return kSlots;
        }

        void Link(uint32_t idx, int l, uint32_t s) {
            Entry& e = entries[idx];
            e.level = int8_t(l);
            e.slot  = s;
            e.prev  = kNil;
            e.next  = heads[l][s];
            if (e.next != kNil) entries[e.next].prev = idx;
            heads[l][s] = idx;
            MarkOccupied(l, s);
        }

        void Unlink(uint32_t idx) {
            Entry& e = entries[idx];
            if (e.level < 0) return;
            const int l = e.level;
            const uint32_t s = e.slot;

            if (e.prev != kNil) entries[e.prev].next = e.next;
            else                heads[l][s] = e.next;
            if (e.next != kNil) entries[e.next].prev = e.prev;

            e.prev = e.next = kNil;
            e.level = -1;
            if (heads[l][s] == kNil) MarkEmpty(l, s);
        }

        void Place(uint32_t idx, int64_t deadlineTick) {
            const int64_t delta = deadlineTick - currentTick;

            if (delta <= 0) { Link(idx, 0, uint32_t(currentTick & kSlotMask)); return; }

            for (int l = 0; l < kLevels; ++l) {
                if (delta < LevelGranularity(l + 1)) {
                    Link(idx, l, uint32_t((deadlineTick >> (kSlotBits * l)) & kSlotMask));
                    return;
                }
            }
            
            Link(idx, kLevels - 1, uint32_t(((currentTick + kMaxTicks - 1) >> (kSlotBits * (kLevels - 1))) & kSlotMask));
        }

        void Cascade(int level, uint32_t slot) {
            uint32_t idx = heads[level][slot];
            heads[level][slot] = kNil;
            MarkEmpty(level, slot);

            while (idx != kNil) {
                const uint32_t nextIdx = entries[idx].next;
                entries[idx].prev = entries[idx].next = kNil;
                entries[idx].level = -1;
                Place(idx, entries[idx].deadlineTick);
                idx = nextIdx;
            }
        }

        // May grow `entries` and move every Entry: take an Entry& only after the acquire.
        uint32_t AcquireEntry() {
            if (freeHead != 0) {
                const uint32_t i = freeHead - 1;
                freeHead = entries[i].nextFree;
                return i;
            }
            entries.push_back(Entry{});
            return uint32_t(entries.size() - 1);
        }

        void ReleaseEntry(uint32_t i) {
            Entry& e = entries[i];
            e.eject = nullptr;
            e.ctx = nullptr;
            e.periodic = nullptr;
            e.token = CancelToken::kNone;
            e.level = -1;
            e.nextFree = freeHead;
            freeHead = i + 1;
        }

        int64_t NextEventTick() const {
            int64_t best = -1;

            for (int l = 0; l < kLevels; ++l) {
                if (LevelEmpty(l)) continue;

                const int64_t gran = LevelGranularity(l);
                const uint32_t cur = uint32_t((currentTick >> (kSlotBits * l)) & kSlotMask);

                const uint32_t from = (l == 0) ? cur : ((cur + 1) & kSlotMask);
                const uint32_t s = NextOccupied(l, from);
                if (s == kSlots) continue;

                const uint32_t ahead = (s - from) & kSlotMask;
                const int64_t base = (l == 0)
                    ? currentTick
                    : ((currentTick >> (kSlotBits * l)) + 1) << (kSlotBits * l);
                const int64_t when = base + int64_t(ahead) * gran;

                if (best < 0 || when < best) best = when;
            }
            return best;
        }

        void AdvanceTo(int64_t target) {
            if (target <= currentTick) return;

            const int64_t from0 = currentTick;
            currentTick = target;

            for (int l = 1; l < kLevels; ++l) {
                const int shift = kSlotBits * l;
                const int64_t from = from0 >> shift;
                const int64_t to   = target >> shift;
                if (from == to) break;              

                const int64_t steps = to - from;
                if (steps >= int64_t(kSlots)) {
                    for (uint32_t s = 0; s < kSlots; ++s)
                        if (heads[l][s] != kNil) Cascade(l, s);
                } else {
                    for (int64_t k = 1; k <= steps; ++k) {
                        const uint32_t s = uint32_t((from + k) & kSlotMask);
                        if (heads[l][s] != kNil) Cascade(l, s);
                    }
                }
            }
        }

        void Run() {
            // The clock runs above the compute workers, as K does. It is parked nearly always and
            // brief when it runs, but at normal priority a saturated pool descheduled it for about
            // a whole 5 ms interval in 2 of 6 measured runs (p99 ~4.8 ms, ~48 grid points skipped);
            // at high, none in 6 (p99 <= 1.6 ms). The median is set by the OS wake and the 1 ms
            // tick, not by priority. Best effort on Linux, where a negative nice needs privileges.
#if JLIB_PLATFORM_WINDOWS
            ::SetThreadPriority(::GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
#elif JLIB_PLATFORM_LINUX
            (void)::syscall(SYS_setpriority, PRIO_PROCESS, (int)::syscall(SYS_gettid), -5);
#endif
            std::unique_lock<std::mutex> lk(m);
            for (;;) {
                if (stopping) return;

                const int64_t next = NextEventTick();
                if (next < 0) {
                    
                    currentTick = NowTick();
                    sleepUntil = INT64_MAX;
                    cv.wait(lk);
                    sleepUntil = kAwake;
                    continue;
                }

                const int64_t now = NowTick();
                if (next > now) {
                    // Sleep to tick `next`'s boundary. A whole number of ticks counted from `now`
                    // (a floored tick) lands up to one tick past it: measured ~0.3-0.5 ms later
                    // idle and about twice as late under load.
                    sleepUntil = next;
                    cv.wait_for(lk, std::chrono::nanoseconds((epochNs + next * tickNs) - MonotonicNs()));
                    sleepUntil = kAwake;
                    continue;
                }

                AdvanceTo(next);

                constexpr size_t kBatch = 64;
                CancelToken tokens[kBatch];
                TimerEject  ejects[kBatch];
                void*       ctxs[kBatch];
                size_t      n = 0;
                PeriodicRecord* fired[kBatch];
                size_t      nf = 0;

                const uint32_t slot = uint32_t(currentTick & kSlotMask);
                uint32_t idx = heads[0][slot];
                while (idx != kNil && n + nf < kBatch) {
                    const uint32_t nextIdx = entries[idx].next;
                    Entry& e = entries[idx];

                    if (e.deadlineTick <= currentTick && e.periodic) {
                        if (FirePeriodicLocked(idx)) fired[nf++] = e.periodic;
                    } else if (e.deadlineTick <= currentTick) {
                        tokens[n] = CancelToken(e.token);
                        ejects[n] = e.eject;
                        ctxs[n]   = e.ctx;
                        ++n;

                        Unlink(idx);
                        ++e.generation;              
                        ReleaseEntry(idx);
                        --armedCount;
                    }
                    idx = nextIdx;
                }

                if (n == 0 && nf == 0) {

                    ++currentTick;
                    continue;
                }

                lk.unlock();
                for (size_t i = 0; i < n; ++i) {

                    if (CancelVia(tokens[i]) && ejects[i]) ejects[i](ctxs[i], tokens[i]);
                }
                for (size_t i = 0; i < nf; ++i) LaunchPeriodic(fired[i]);
                lk.lock();
            }
        }

        // ---- periodic tasks ----

        int64_t TickFor(int64_t ns) const {
            const int64_t rel = ns - epochNs;
            return rel <= 0 ? 0 : (rel + tickNs - 1) / tickNs;
        }

        // Grid point `due` has been reached. The one place a next fire time is computed:
        // skipped = grid points already passed beyond `due`; next = the first one still ahead.
        // Starts an instance unless one is in flight (then this point is skipped too). The entry
        // stays armed at `next`. Returns true if the caller must launch an instance.
        bool FirePeriodicLocked(uint32_t idx) {
            Entry& e = entries[idx];
            PeriodicRecord* r = e.periodic;
            Unlink(idx);

            const int64_t now    = MonotonicNs();
            const int64_t behind = now - r->due;
            const int64_t passed = behind > 0 ? behind / r->interval : 0;   // O(1), any stall length
            const int64_t next   = r->due + (passed + 1) * r->interval;

            bool launch = false;
            uint64_t skippedNow = uint64_t(passed);
            if (r->inFlight) {
                skippedNow += 1;                       // this grid point is not started either
            } else {
                r->inFlight = true;
                r->runs.n.fetch_add(1, std::memory_order_acq_rel);
                launch = true;
            }
            if (launch) JLIB_STAT(PeriodicFires);
            JLIB_STAT_N(PeriodicSkipped, skippedNow);
            r->pendingSkipped += skippedNow;
            if (skippedNow) r->totalSkipped.fetch_add(skippedNow, std::memory_order_relaxed);

            r->due = next;
            e.deadlineTick = TickFor(next);            // > currentTick: next > now, interval >= 1 tick
            Place(idx, e.deadlineTick);
            return launch;
        }

        void CancelPeriodicLocked(PeriodicRecord* r) {
            r->cancelled = true;
            if (r->entry == kNil) return;
            Unlink(r->entry);
            ++entries[r->entry].generation;
            ReleaseEntry(r->entry);
            --armedCount;
            r->entry = kNil;
        }

        // The end of an instance (or of one that could not be launched). For a joined record the
        // wake is the last touch: the joiner may free the record as soon as it runs.
        void FinishPeriodic(PeriodicRecord* r, bool stop) {
            bool wake = false, freeNow = false;
            {
                std::lock_guard<std::mutex> lk(m);
                if (stop) CancelPeriodicLocked(r);
                r->inFlight = false;
                wake = r->runs.DoneBegin();
                freeNow = r->detached && r->cancelled;   // no joiner can exist for a detached record
            }
            if (wake) r->runs.WakeAll(true);
            if (freeNow) delete r;
        }

        static void RunPeriodic(void* p) {
            PeriodicRecord* r = static_cast<PeriodicRecord*>(p);
            Impl* im = TimerQueue::Instance().impl;
            uint64_t skipped = 0;
            bool cancelled = false;
            {
                std::lock_guard<std::mutex> lk(im->m);
                skipped = r->pendingSkipped;
                r->pendingSkipped = 0;
                cancelled = r->cancelled;
            }
            const bool keep = cancelled || r->fn(r->ctx, skipped);
            im->FinishPeriodic(r, !keep);
        }

        void LaunchPeriodic(PeriodicRecord* r) {
            if (!TaskScheduler::IsInitialized()) { FinishPeriodic(r, false); return; }
            TaskScheduler& s = TaskScheduler::Instance();
            Task* t = s.CreateTask(&RunPeriodic, r, r->lane);
            if (!t) { FinishPeriodic(r, false); return; }            // Latency instances go to K's lane intake when there is a K. Everything else (and
            // latency work with no K) to a compute worker's hi-pri inbox, round-robin with a wake,
            // as I/O completions do: a normal inbox waits behind the worker's own successors when
            // the pool is saturated.
            const bool toK = IsLowLatency(r->lane) && TaskScheduler::HiPriLaneActive()
                          && s.LaneIntakeEnabled() && TaskScheduler::PushLaneIntake(&t, 1);
            const bool ok = toK || s.PushTo(t, CorePref::Any, true);
            if (!ok) { s.FreeTask(t); FinishPeriodic(r, false); }
        }
    };

    constexpr uint32_t TimerQueue::Impl::kNil;

    TimerQueue::TimerQueue() : impl(new Impl()) {}

    TimerQueue::~TimerQueue() {
        Stop();
        delete impl;
    }

    TimerQueue& TimerQueue::Instance() {
        
        static TimerQueue* q = new TimerQueue();
        return *q;
    }

    TimerHandle TimerQueue::Arm(int64_t delayNs, CancelToken token, TimerEject eject, void* ctx) {
        
        if (!token.Valid()) return TimerHandle{};

        std::lock_guard<std::mutex> lk(impl->m);
        if (impl->stopping) return TimerHandle{};

        if (TaskScheduler::IsInitialized() && !TaskScheduler::TimersEnabled()) {
            std::fprintf(stderr,
                "[JLib::Scheduler] TimerQueue::Arm called but the timer layer is not enabled -- the "
                "pool was sized without a core for it. Set Config::timers = true at Init.\n");
            return TimerHandle{};
        }

        if (!impl->running) {
            impl->running = true;
            impl->worker = std::thread([this] { impl->Run(); });

        }

        // The deadline's own time rounded UP to a tick, as periodics do: never before now + delay.
        // Floored now + rounded-up delay could land up to a tick early (more when currentTick has
        // run a tick ahead of the clock on the empty-fire path).
        const int64_t nowTick = impl->NowTick();
        const int64_t deadlineTick = (delayNs > 0) ? impl->TickFor(MonotonicNs() + delayNs) : nowTick;

        impl->AdvanceTo(nowTick);

        const uint32_t i = impl->AcquireEntry();
        Impl::Entry& e = impl->entries[i];
        ++e.generation;                       
        e.deadlineTick = deadlineTick;
        e.token = token.Raw();
        e.eject = eject;
        e.ctx = ctx;
        impl->Place(i, deadlineTick);
        ++impl->armedCount;

        // Wake the sleeper only if this made its next event earlier. Not "next >= deadlineTick":
        // above level 0 the next event is the entry's cascade boundary, which is before its
        // deadline, so a 256+ tick timer never woke a thread in its untimed wait.
        if (impl->NextEventTick() < impl->sleepUntil) impl->cv.notify_one();

        return TimerHandle{ (uint64_t(e.generation) << 32) | uint64_t(i) };
    }

    bool TimerQueue::Disarm(TimerHandle h) noexcept {
        if (!h.Valid()) return false;
        const uint32_t i = uint32_t(h.raw & 0xFFFFFFFFu);
        const uint32_t g = uint32_t(h.raw >> 32);

        std::lock_guard<std::mutex> lk(impl->m);
        if (i >= impl->entries.size()) return false;

        Impl::Entry& e = impl->entries[i];
        
        if (e.generation != g || (g & 1u) == 0) return false;

        impl->Unlink(i);
        ++e.generation;                       
        impl->ReleaseEntry(i);
        --impl->armedCount;
        return true;
    }

    // ---- Periodic ----

    Periodic Periodic::Start(int64_t intervalNs, PeriodicFn fn, void* ctx, Lane lane, int64_t firstDelayNs) {
        assert(intervalNs > 0 && fn && "Periodic::Start: interval must be > 0 and fn non-null");
        if (intervalNs <= 0 || !fn) return Periodic();

        TimerQueue& q = TimerQueue::Instance();
        TimerQueue::Impl* im = q.impl;
        std::lock_guard<std::mutex> lk(im->m);
        if (im->stopping) return Periodic();
        if (!TaskScheduler::IsInitialized() || !TaskScheduler::TimersEnabled()) {
            std::fprintf(stderr,
                "[JLib::Scheduler] Periodic::Start needs the timer layer: set "
                "Config::timers = true at Init.\n");
            return Periodic();
        }
        if (!im->running) {
            im->running = true;
            im->worker = std::thread([im] { im->Run(); });
        }

        auto* r = new PeriodicRecord();
        r->interval = intervalNs < im->tickNs ? im->tickNs : intervalNs;
        r->fn   = fn;
        r->ctx  = ctx;
        r->lane = lane;
        r->due  = MonotonicNs() + (firstDelayNs < 0 ? r->interval : firstDelayNs);

        im->AdvanceTo(im->NowTick());
        const uint32_t i = im->AcquireEntry();
        TimerQueue::Impl::Entry& e = im->entries[i];
        ++e.generation;
        e.periodic     = r;
        e.deadlineTick = im->TickFor(r->due);
        im->Place(i, e.deadlineTick);
        ++im->armedCount;
        r->entry = i;

        if (im->NextEventTick() < im->sleepUntil) im->cv.notify_one();   // as in Arm
        return Periodic(r);
    }

    Periodic::~Periodic() {
        if (!rec_) return;
        Cancel();
        Join();
        delete rec_;
    }

    Periodic& Periodic::operator=(Periodic&& o) noexcept {
        if (this != &o) {
            if (rec_) { Cancel(); Join(); delete rec_; }
            rec_ = o.rec_;
            o.rec_ = nullptr;
        }
        return *this;
    }

    void Periodic::Cancel() noexcept {
        if (!rec_) return;
        TimerQueue::Impl* im = TimerQueue::Instance().impl;
        std::lock_guard<std::mutex> lk(im->m);
        im->CancelPeriodicLocked(rec_);
    }

    void Periodic::Join() {
        if (!rec_) return;
        assert(rec_->cancelled && "Periodic::Join before Cancel: a new instance may start right after");
        if (!TaskScheduler::IsInitialized()) return;   // no pool: nothing can be in flight
        TaskScheduler& s = TaskScheduler::Instance();
        const Task* cur = s.GetCurrentTask();
        if (cur && cur->fn == &TimerQueue::Impl::RunPeriodic && cur->data == rec_) {
            std::fprintf(stderr, "[JLib::Scheduler] FATAL: Periodic::Join called from inside its own "
                                 "callback -- it would wait for itself. Return false to stop instead.\n");
            std::fflush(stderr);
            std::abort();
        }
        s.WaitFor(rec_->runs);
    }

    void Periodic::Detach() noexcept {
        if (!rec_) return;
        bool freeNow = false;
        {
            TimerQueue::Impl* im = TimerQueue::Instance().impl;
            std::lock_guard<std::mutex> lk(im->m);
            rec_->detached = true;
            freeNow = rec_->cancelled && !rec_->inFlight;
        }
        if (freeNow) delete rec_;
        rec_ = nullptr;
    }

    uint64_t Periodic::Skipped() const noexcept {
        return rec_ ? rec_->totalSkipped.load(std::memory_order_relaxed) : 0;
    }

    std::size_t TimerQueue::PendingCount() const noexcept {
        std::lock_guard<std::mutex> lk(impl->m);
        return impl->armedCount;
    }

    void TimerQueue::Start() noexcept {
        std::lock_guard<std::mutex> lk(impl->m);
        impl->stopping = false;
    }

    void TimerQueue::Stop() noexcept {
        std::thread t;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            if (impl->stopping) return;
            impl->stopping = true;
            impl->running  = false;   // the next Arm after Start spawns a new thread
            impl->cv.notify_all();
            t.swap(impl->worker);
        }
        
        if (t.joinable()) t.join();
    }

} 
