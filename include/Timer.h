// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include "CancelToken.h"
#include "Task.h"   // Lane

#include <cstddef>
#include <cstdint>

namespace JLib {

    class Event;
    class SchedulerSemaphore;
    class SchedulerConditionVariable;

    int64_t MonotonicNs() noexcept;

    using TimerEject = void (*)(void* ctx, CancelToken token);
    class Periodic;

    struct TimerHandle {
        uint64_t raw = 0;
        bool Valid() const noexcept { return raw != 0; }
    };

    class TimerQueue {
    public:
        static TimerQueue& Instance();

        TimerHandle Arm(int64_t delayNs, CancelToken token,
                        TimerEject eject = nullptr, void* ctx = nullptr);

        bool Disarm(TimerHandle h) noexcept;

        std::size_t PendingCount() const noexcept;

        void Stop() noexcept;

        void Start() noexcept;

    private:
        friend class Periodic;
        TimerQueue();
        ~TimerQueue();
        TimerQueue(const TimerQueue&) = delete;
        TimerQueue& operator=(const TimerQueue&) = delete;

        struct Impl;
        Impl* impl;
    };

    class Deadline {
    public:
        Deadline(int64_t delayNs, CancelToken token,
                 TimerEject eject = nullptr, void* ctx = nullptr)
            : h_(TimerQueue::Instance().Arm(delayNs, token, eject, ctx)) {}

        ~Deadline() { TimerQueue::Instance().Disarm(h_); }

        Deadline(const Deadline&) = delete;
        Deadline& operator=(const Deadline&) = delete;

        bool Armed() const noexcept { return h_.Valid(); }

        bool Cancel() noexcept { return TimerQueue::Instance().Disarm(h_); }

    private:
        TimerHandle h_;
    };

    // A periodic task: fn runs on the pool every interval, on a fixed grid (first due = start +
    // firstDelay, then every interval after that; never re-derived from "now", so no drift).
    //  - At most one instance is live. A grid point that passes while it is still running, or
    //    while the machine stalled, is skipped, not started; fn receives the number of grid points
    //    skipped since its previous run.
    //  - fn returns false to stop itself.
    //  - Cancel() stops further fires. Join() returns once no instance is in flight; after
    //    Cancel + Join the caller may free whatever fn uses. Never Join from inside fn.
    //  - Destroying the handle cancels and joins. Detach() lets the task run until fn returns false.
    using PeriodicFn = bool (*)(void* ctx, uint64_t skipped);
    struct PeriodicRecord;

    class Periodic {
    public:
        Periodic() noexcept = default;
        ~Periodic();
        Periodic(Periodic&& o) noexcept : rec_(o.rec_) { o.rec_ = nullptr; }
        Periodic& operator=(Periodic&& o) noexcept;
        Periodic(const Periodic&) = delete;
        Periodic& operator=(const Periodic&) = delete;

        // Invalid (Valid() == false) if intervalNs <= 0, fn is null, or timers are not enabled.
        // Intervals shorter than the timer tick (1 ms) run once per tick. firstDelayNs < 0 means
        // one interval.
        static Periodic Start(int64_t intervalNs, PeriodicFn fn, void* ctx,
                              Lane lane = Lane::Normal, int64_t firstDelayNs = -1);

        bool Valid() const noexcept { return rec_ != nullptr; }
        void Cancel() noexcept;
        void Join();
        void Detach() noexcept;
        // Grid points skipped over the task's lifetime (diagnostic: a steady rise means fn is too
        // slow for its interval).
        uint64_t Skipped() const noexcept;

    private:
        explicit Periodic(PeriodicRecord* r) noexcept : rec_(r) {}
        PeriodicRecord* rec_ = nullptr;
    };

    void EjectEvent(void* ctx, CancelToken token);
    void EjectSemaphore(void* ctx, CancelToken token);      
    void EjectConditionVariable(void* ctx, CancelToken token);  

} 
