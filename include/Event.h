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
    // under a short spinlock (held for a few pointer writes; every wake happens OUTSIDE it).
    //
    // THE RULE: whoever unlinks a task under the lock is the one who wakes it. SignalAll detaches
    // the whole list; CancelWaiters(token) unlinks only the tasks inside that token. A task can
    // therefore never be woken twice, never be woken by an event it has left, and never sit in a
    // list after it woke -- so its links are free for its next wait at once.
    //
    // History: with a bounded fiber pool this was an address table (a fixed slot per fiber). The
    // unbounded pool moved it to a Treiber stack -- one CAS per wait -- which could only detach the
    // WHOLE list, so a Deadline (the clock's EjectEvent) woke every waiter as Cancelled, not just
    // the ones under its token (tests/event_deadline_test.cpp). Events are not a hot path; removal
    // under a lock is the plain answer. The same FIFO would let SignalOne return (oldest first).
    //
    // CONTRACT -- LINKED MEANS PARKED: every wait (WaitOnEvent, WaitOnEventArmed, EventAwaiter)
    // checks cancellation BEFORE AddWaiter and parks after it; only this class unparks a linked
    // task. Never Thread::Resume a task parked here.
    class Event : public WaitPrimitive {
    private:
        std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
        std::atomic<Task*> head_{ nullptr };   // atomic only for SignalAll's lock-free "empty?" check
        Task* tail_ = nullptr;                 // under lock_

        void Lock() noexcept   { while (lock_.test_and_set(std::memory_order_acquire)) platform::CpuRelax(); }
        void Unlock() noexcept { lock_.clear(std::memory_order_release); }

        static void WakeCoroutine(Task* t);

        // Fiber::Resume() claims AND places -- or declines and leaves it to the park step -- so a
        // task is placed exactly once. The caller already unlinked it under the lock.
        static void Wake(Task* t, bool cancel) {
            if (cancel) t->cancelledDirect = 1;
            if (Fiber* f = t->record->fiber) f->Resume();
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

        // The task must already be marked suspending (fiber) or armed (coroutine).
        void AddWaiter(Task* t) {
            TaskRecord* r = t->record;
            r->waitNext = nullptr;
            Lock();
            r->waitPrev = tail_;
            if (tail_) tail_->record->waitNext = t;
            else head_.store(t, std::memory_order_release);
            tail_ = t;
            Unlock();
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
