// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <cstdint>

#include "CancelToken.h"
#include "Task.h"          // TaskHandle: the node holds one by value

namespace JLib {

    struct Fiber;
    enum class WaitResult : std::uint8_t;

    // The waiter queue for the blocking primitives (mutex, semaphore, condition variable).
    //
    // THE NODE LIVES ON THE WAITER'S OWN STACK -- the suspending fiber's frame, or the awaiter
    // object inside a coroutine frame. Both outlive the wait by construction, so joining a queue
    // costs nothing. The std::queue<Waiter> this replaces allocated on the way in, and it did it
    // while holding the primitive's spinlock: a malloc inside a critical section that every other
    // waiter spins on.
    //
    // THE CALLER HOLDS THE PRIMITIVE'S LOCK. Every method here assumes it. That is not a
    // compromise: the primitive's lock already has to be held across the queue operation. A waiter
    // decides to queue because `locked` was true or `permits` was zero, and if it publishes itself
    // after dropping the lock, a signal landing in that window sees an empty queue, banks a permit,
    // and the waiter sleeps forever next to it. Enqueue and the state test are one decision, so
    // they are under one lock, and a lock-free queue beside them would buy nothing.
    //
    // ORDER IS FIFO, kept by a tail pointer. LIFO is legal for a semaphore but not for a mutex:
    // under steady contention the node at the bottom is never reached.
    // ONE FIELD FOR BOTH KINDS. The waiter parked before it linked, so its park handle is on the
    // task (TaskRecord::handle) and Thread::Kick resolves it through the lot -- no waker branches
    // on fiber versus coroutine, and none of them holds a Fiber*.
    struct WaitNode {
        WaitNode*   next   = nullptr;
        Task*       task   = nullptr;
        WaitResult* result = nullptr;                   // non-null only for a cancellable wait
        std::uint32_t token = CancelToken::kNone;
    };

    class WaitList {
    public:
        bool Empty() const noexcept { return head_ == nullptr; }

        void PushBack(WaitNode* n) noexcept {
            n->next = nullptr;
            if (tail_) tail_->next = n;
            else       head_ = n;
            tail_ = n;
        }

        // The oldest waiter, or null.
        WaitNode* PopFront() noexcept {
            WaitNode* n = head_;
            if (n) {
                head_ = n->next;
                if (!head_) tail_ = nullptr;
                n->next = nullptr;
            }
            return n;
        }

        // Every waiter, oldest first, detached in one go. The caller walks the chain through `next`.
        WaitNode* TakeAll() noexcept {
            WaitNode* n = head_;
            head_ = tail_ = nullptr;
            return n;
        }

        // Unlinks one node if it is still queued: the cancel path, where a waiter that returns on
        // its own must not stay linked, because its frame is about to go away.
        bool Remove(WaitNode* target) noexcept {
            WaitNode** link = &head_;
            WaitNode*  prev = nullptr;
            while (WaitNode* n = *link) {
                if (n == target) {
                    *link = n->next;
                    if (tail_ == n) tail_ = prev;
                    n->next = nullptr;
                    return true;
                }
                prev = n;
                link = &n->next;
            }
            return false;
        }

        // Detaches every node the predicate accepts, oldest first, leaving the rest queued in order.
        template <class Pred>
        WaitNode* TakeIf(Pred pred) noexcept {
            WaitNode*  takenHead = nullptr; WaitNode** takenTail = &takenHead;
            WaitNode*  keepHead  = nullptr; WaitNode** keepTail  = &keepHead;
            WaitNode*  keepLast  = nullptr;
            for (WaitNode* n = head_; n; ) {
                WaitNode* next = n->next;
                n->next = nullptr;
                if (pred(n)) { *takenTail = n; takenTail = &n->next; }
                else         { *keepTail  = n; keepTail  = &n->next; keepLast = n; }
                n = next;
            }
            head_ = keepHead;
            tail_ = keepLast;
            return takenHead;
        }

    private:
        WaitNode* head_ = nullptr;
        WaitNode* tail_ = nullptr;
    };

}
