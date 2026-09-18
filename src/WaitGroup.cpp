// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/WaitGroup.h"
#include "../include/Fiber.h"
#include "../include/TaskScheduler.h"
#include "../include/RetryStats.h"

using namespace JLib;

namespace {
    // A fiber resumes through its status handshake (it may still be switching out); a coroutine's
    // frame is already suspended once its awaiter registered, so it is simply woken.
    void WakeWaiter(Task* t) {
        if (Fiber* f = t->record->fiber) f->Resume();
        else TaskScheduler::Instance().WakeTask(t);
    }

    // Wakes a detached list. Reads the link before each wake: a woken task may finish and free
    // its record on another thread at once.
    void WakeList(Task* head) {
        while (head) {
            Task* next = head->record->waitNext;
            head->record->waitNext = nullptr;
            WakeWaiter(head);
            head = next;
        }
    }
}

void JLib::WaitGroup::AddWaiter(Task* t)
{
    JLib::RetryProbe wgProbe(JLib::RetrySite::WaitGroupPush);
    Task* head = taskWaiters.load(std::memory_order_relaxed);
    do {
        t->record->waitNext = head;
    } while (!taskWaiters.compare_exchange_weak(
                head, t, std::memory_order_acq_rel, std::memory_order_relaxed)
             && (wgProbe.Miss(), true));

    // Bit before count, same order as every other waiter: a decrement to zero after this sees the
    // bit and wakes the list; if the count was already zero, wake it here.
    const int old = n.fetch_or(WAITER_BIT, std::memory_order_acq_rel);
    if ((old & COUNT_MASK) == 0) WakeAllDirect();
}

void JLib::WaitGroup::WakeAllDirect()
{
    Task* head = taskWaiters.exchange(nullptr, std::memory_order_acq_rel);
    if (!head) return;

    n.fetch_and(~WAITER_BIT, std::memory_order_release);
    WakeList(head);
}

void JLib::WaitGroup::Settle()
{
    while (waking.load(std::memory_order_acquire) != 0) platform::CpuRelax();
    std::lock_guard<std::mutex> lock(mtx);   // and past any wake's locked section
}

void JLib::WaitGroup::WakeAll(bool releaseMark)
{

    std::vector<std::atomic<int>*> threads;
    Task* taskHead = nullptr;
    std::atomic<int>* mainWord = nullptr;
    // Everything is read under the lock. After it, only local copies are touched: a woken waiter
    // may return and destroy this group (often a stack object) before this function ends.
    {
        std::lock_guard<std::mutex> lock(mtx);
        threads.swap(blockedThreads);
        mainWord = threadWaiter.load(std::memory_order_acquire);

        taskHead = taskWaiters.exchange(nullptr, std::memory_order_acq_rel);
        n.fetch_and(~WAITER_BIT, std::memory_order_release);
        if (releaseMark) waking.fetch_sub(1, std::memory_order_acq_rel);
    }

    for (auto* w : threads)
        KickWaitWord(w);
    if (mainWord) KickWaitWord(mainWord);
    WakeList(taskHead);
}

// The failure case only: a thread that cannot suspend (a coroutine that reached a blocking wait).
// The worker is lost until the group finishes; nothing is handed off.
void JLib::WaitGroup::BlockThread()
{
    std::atomic<int> word{ kWaitRunning };
    for (;;) {
        word.store(kWaitWaiting, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(mtx);
            // Set the bit before reading the count: a decrement to zero after this sees the bit
            // and calls WakeAll, which waits for this lock and then kicks the word.
            const int old = n.fetch_or(WAITER_BIT, std::memory_order_acq_rel);
            if ((old & COUNT_MASK) == 0) break;
            blockedThreads.push_back(&word);
        }
        BlockOnWaitWord(&word);
        if ((n.load(std::memory_order_acquire) & COUNT_MASK) == 0) break;
    }
    Settle();
}
