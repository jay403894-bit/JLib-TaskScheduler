// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include "Task.h"
#include "Fiber.h"
#include "concurrentqueue.h"

namespace JLib {

    namespace detail { void WakeCoroutineWaiter(Task* t); }

    // One waiter, one signal. The word holds only "no waiter" or "a waiter" -- there is no signaled
    // state -- which is safe because of two contracts every wait keeps:
    //   STORE BEFORE ARM: the waiter is stored BEFORE arm(e) runs (WaitOnEventDirectArmed,
    //     DirectEventAwaiter), and arm is the only way a signal source learns of e. So a source that
    //     has already completed (a finished GPU fence) signals after the store, never before it.
    //   ONE SIGNAL PER ARM: a source signals once. A pooled event is reused after its waiter wakes,
    //     so a second Signal would land on the NEXT waiter. Nothing but Signal unparks the waiter --
    //     never Thread::Resume a task parked here.
    // It has no deadline or cancel path of its own: only the armed source ends the wait.
    struct DirectEvent {
        std::atomic<Task*> waiter{ nullptr };

        void Signal() {
            if (Task* t = waiter.exchange(nullptr, std::memory_order_acq_rel)) {
                if (Fiber* f = t->record->fiber) f->Resume();
                else detail::WakeCoroutineWaiter(t);
            }
        }
    };

    // Grows by one block when empty; events are never freed while the pool lives, so a stale
    // pointer still names a DirectEvent (no use-after-free -- but see ONE SIGNAL PER ARM above).
    class EventPool {
        static constexpr size_t kBlock = 256;
        std::vector<std::unique_ptr<DirectEvent[]>> blocks;
        std::mutex growMtx;
        moodycamel::ConcurrentQueue<DirectEvent*> freeq;

        void AddBlock(size_t n) {
            std::unique_ptr<DirectEvent[]> b(new DirectEvent[n]);
            DirectEvent* first = b.get();
            blocks.push_back(std::move(b));
            for (size_t i = 0; i < n; ++i) freeq.enqueue(first + i);
        }
    public:
        explicit EventPool(size_t n) : freeq(n) { AddBlock(n); }

        DirectEvent* Acquire() {
            DirectEvent* e = nullptr;
            // moodycamel's queue is not linearizable: try_dequeue can fail while size_approx() still
            // reports items. Waiting for the estimate to reach 0 before growing would then spin under
            // the mutex, so after a few misses grow anyway -- a spare block is cheaper than the spin.
            for (int misses = 0; !freeq.try_dequeue(e); ++misses) {
                std::lock_guard<std::mutex> lock(growMtx);
                if (misses >= 3 || freeq.size_approx() == 0) { AddBlock(kBlock); misses = 0; }
            }
            e->waiter.store(nullptr, std::memory_order_relaxed);
            return e;
        }
        void Release(DirectEvent* e) { freeq.enqueue(e); }
    };
}
