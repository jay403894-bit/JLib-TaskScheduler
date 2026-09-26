// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "CancelToken.h"
#include "Task.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

namespace JLib {
    struct DirectEvent;   
                          
    // A thread's wait word (main in the pool). Same permit handshake as the worker park:
    // the waiter CASes RUNNING->WAITING, rechecks, then blocks while the word is WAITING.
    // A kick swaps in KICKED and wakes the address only if the old value was WAITING.
    enum : int { kWaitRunning = 0, kWaitWaiting = 1, kWaitKicked = 2 };
    void KickWaitWord(std::atomic<int>* word) noexcept;
    void BlockOnWaitWord(std::atomic<int>* word) noexcept;   // returns once the word leaves WAITING

    struct WaitGroup {

        static constexpr int WAITER_BIT = 0x40000000;
        static constexpr int COUNT_MASK = WAITER_BIT - 1;
        std::atomic<int> n{ 0 };
        std::mutex mtx;

        // Tasks suspended on this group -- a fiber in WaitFor or a coroutine in co_await -- linked
        // through TaskRecord::waitNext. Lock-free push; a wake takes the whole list.
        std::atomic<Task*> taskWaiters{ nullptr };

        // Registers a task that is about to give up its thread. If the count is already zero the
        // wake runs at once, and the resume path copes with a task that has not finished switching out.
        void AddWaiter(Task* t);

        // A thread (not a fiber) blocked on this group: its wait word is kicked when n reaches 0.
        std::atomic<std::atomic<int>*> threadWaiter{ nullptr };

        // Worker threads blocked with no fiber (a coroutine that reached a blocking wait).
        // Guarded by mtx; WakeAll kicks every word.
        std::vector<std::atomic<int>*> blockedThreads;

        // Blocks the calling thread until the count reaches zero.
        void BlockThread();

        void WakeAllDirect();

        // Completion. Every "one task done" goes through Done() (or DoneBegin + WakeAll(true)).
        // `waking` is non-zero from before the decrement until the wake has taken everything it needs
        // under mtx, so a waiter that returns without having suspended (Settle) never leaves while a
        // waker may still touch this group. Model: tests/verify/waitgroup_model.c. The waiter bit can
        // NOT stand in for `waking` (other paths clear it); packing `waking` into `n` is correct but
        // measured slower (fib ~13%, pingpong) -- both recorded there.
        std::atomic<int> waking{ 0 };
        void Done() { if (DoneBegin()) WakeAll(true); }
        // Decrements; true if the caller must now call WakeAll(true).
        bool DoneBegin() {
            waking.fetch_add(1, std::memory_order_acq_rel);
            const int old = n.fetch_sub(1, std::memory_order_acq_rel);
            if ((old & COUNT_MASK) == 1 && (old & WAITER_BIT)) return true;
            waking.fetch_sub(1, std::memory_order_acq_rel);
            return false;
        }
        // For a waiter that saw the count reach zero without being woken: wait out any wake in progress.
        void Settle();

        // releaseMark: called from Done/DoneBegin (clears the `waking` mark under the lock).
        void WakeAll(bool releaseMark = false);

    };
}
