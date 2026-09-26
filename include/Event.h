// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "WaitPrimitive.h"
#include "CancelToken.h"
#include "Fiber.h"
#include "platform.h"
namespace JLib {

    // Broadcast event. Waiters are a doubly linked FIFO through TaskRecord::waitNext/waitPrev
    class Event : public WaitPrimitive {
    private:
        std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
        std::atomic<Task*> head_{ nullptr };   // atomic only for SignalAll's lock-free "empty?" check
        Task* tail_ = nullptr;                 // under lock_

        void Lock() noexcept   { while (lock_.test_and_set(std::memory_order_acquire)) platform::CpuRelax(); }
        void Unlock() noexcept { lock_.clear(std::memory_order_release); }

        static void WakeCoroutine(Task* t);
        // One claim, one placement. The caller already unlinked it under the lock.
        static void Wake(Task* t, bool cancel) {
            if (cancel) t->cancelledDirect = 1;
            if (t->type == TaskType::Fiber) WakeWaiter(t);
            else WakeCoroutine(t);
        }

        // A detached chain, linked by waitNext. Reads the link before each wake: a woken task may
        // run, finish or wait again at once.
        static void WakeChain(Task* t, bool cancel) {
            while (t) {
                Task* next = t->record->waitNext;
                t->record->waitNext = t->record->waitPrev = nullptr;
                Wake(t, cancel);
                t = next;
            }
        }

        Task* DetachAll() noexcept {
            Lock();
            Task* list = head_.load(std::memory_order_relaxed);
            head_.store(nullptr, std::memory_order_relaxed);
            tail_ = nullptr;
            Unlock();
            return list;
        }

    public:
        Event() = default;
        ~Event() { LeaveRegistry(); }

        Event(const Event&) = delete;
        Event& operator=(const Event&) = delete;

        // The task must already be marked suspending (fiber) or armed (coroutine). Returns false,
        [[nodiscard]] bool AddWaiter(Task* t) {
            TaskRecord* r = t->record;
            r->waitNext = nullptr;
            Lock();
            if (t->cancelledDirect || CancelToken(t->cancelToken).Cancelled()) { Unlock(); return false; }
            if (PoolStopping()) { t->cancelledDirect = 1; Unlock(); return false; }
            // Fiber::StoreTask forwards to home->StoreSuspended in Fiber.cpp, so this header does
            // not need Thread's complete type.
            if (t->type == TaskType::Fiber && r->fiber && !r->fiber->StoreTask(t)) {
                Unlock();
                return false;                 // no slot: the caller cancels its suspend
            }
            r->waitPrev = tail_;
            if (tail_) tail_->record->waitNext = t;
            else head_.store(t, std::memory_order_release);
            tail_ = t;
            Unlock();
            return true;
        }

    protected:
        void DrainForShutdown() override { CancelWaiters(); }
    public:
        // Every waiter, or only those inside `tok` (a Deadline's token -- see EjectEvent). The
        // others stay linked and parked, untouched.
        void CancelWaiters(CancelToken tok = CancelToken{}) {
            if (!tok.Valid()) { WakeChain(DetachAll(), true); return; }
            Task* victims = nullptr;
            Task* victimsTail = nullptr;
            Lock();
            for (Task* t = head_.load(std::memory_order_relaxed); t;) {
                TaskRecord* r = t->record;
                Task* next = r->waitNext;
                if (CancelToken(t->cancelToken).IsWithin(tok)) {
                    // unlink
                    if (r->waitPrev) r->waitPrev->record->waitNext = next;
                    else head_.store(next, std::memory_order_relaxed);
                    if (next) next->record->waitPrev = r->waitPrev;
                    else tail_ = r->waitPrev;
                    // onto the victims chain (waitNext), in list order
                    r->waitPrev = nullptr;
                    r->waitNext = nullptr;
                    if (victimsTail) victimsTail->record->waitNext = t; else victims = t;
                    victimsTail = t;
                }
                t = next;
            }
            Unlock();
            WakeChain(victims, true);
        }

        void SignalAll() {
            if (!head_.load(std::memory_order_acquire)) return;
            WakeChain(DetachAll(), false);
        }
    };
};
