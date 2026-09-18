// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "WaitPrimitive.h"
#include "Fiber.h"
#include "platform.h"
namespace JLib {

    // Broadcast event. Waiters are a Treiber stack linked through TaskRecord::waitNext: a wait is
    // one CAS on the head, SignalAll/CancelWaiters detach the whole list with one exchange and
    // wake it with no synchronisation. Nothing ever removes a single node.
    class Event : public WaitPrimitive {
    private:
        std::atomic<Task*> head{ nullptr };

        static constexpr std::size_t kBuf = 64;

        struct WakeBatch {
            Task* lo[kBuf]; std::size_t nlo = 0;
            Task* hi[kBuf]; std::size_t nhi = 0;

            void Add(Task* t) {
                Fiber* f = t->record->fiber;
                if (!f) { WakeCoroutine(t); return; }
                if (!f->ResumeQueueless()) return;
                if (IsLowLatency(t->lane)) {
                    hi[nhi++] = t;
                    if (nhi == kBuf) { RequeueResumedBatch(hi, nhi, Lane::LowLatency); nhi = 0; }
                } else {
                    lo[nlo++] = t;
                    if (nlo == kBuf) { RequeueResumedBatch(lo, nlo, Lane::Normal); nlo = 0; }
                }
            }
            ~WakeBatch() {
                RequeueResumedBatch(hi, nhi, Lane::LowLatency);
                RequeueResumedBatch(lo, nlo, Lane::Normal);
            }
        };

        static void WakeCoroutine(Task* t);

        // Reads the link before each wake: a woken task may run, finish or wait again at once.
        static void WakeList(Task* t, bool cancel) {
            WakeBatch batch;
            while (t) {
                Task* next = t->record->waitNext;
                t->record->waitNext = nullptr;
                if (cancel) t->cancelledDirect = 1;
                batch.Add(t);
                t = next;
            }
        }

    public:
        Event() = default;
        ~Event() { LeaveRegistry(); }

        Event(const Event&) = delete;
        Event& operator=(const Event&) = delete;

        // The task must already be marked suspending (fiber) or armed (coroutine).
        void AddWaiter(Task* t) {
            Task* h = head.load(std::memory_order_acquire);
            do {
                t->record->waitNext = h;
            } while (!head.compare_exchange_weak(h, t, std::memory_order_release,
                                                 std::memory_order_acquire));
        }

    protected:
        void DrainForShutdown() override { CancelWaiters(); }
    public:
        void CancelWaiters() {
            WakeList(head.exchange(nullptr, std::memory_order_acq_rel), true);
        }

        void SignalAll() {
            if (!head.load(std::memory_order_acquire)) return;
            WakeList(head.exchange(nullptr, std::memory_order_acq_rel), false);
        }
    };
};
