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
    // pointer still names a DirectEvent.
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
            while (!freeq.try_dequeue(e)) {
                std::lock_guard<std::mutex> lock(growMtx);
                if (freeq.size_approx() == 0) AddBlock(kBlock);
            }
            e->waiter.store(nullptr, std::memory_order_relaxed);
            return e;
        }
        void Release(DirectEvent* e) { freeq.enqueue(e); }
    };
}
