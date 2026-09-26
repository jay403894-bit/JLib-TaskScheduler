// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <utility>
#include "Epochs.h"
#include "Thread.h"   // EpochGuard

namespace JLib {

    // Lock-free priority queue: Linden & Jonsson, "A Skiplist-Based Concurrent Priority Queue with
    // Minimal Memory Contention" (OPODIS 2013; Uppsala TR 2018-003). Model:
    // tests/verify/skiplist_pq_model.c.
    //
    // The delete flag of a node is the low bit of its PREDECESSOR's next[0], so deleted nodes always
    // form a prefix. DeleteMin claims with one fetch_or and walks past the claimed prefix instead of
    // unlinking; once the prefix passes boundOffset, one thread cuts it with a single CAS on
    // head.next[0] and retires the batch.
    //
    // Keys must be unique (the paper's proof depends on it), so a key is (priority << kSeqBits) | seq
    // from one counter: equal priorities come out first-in, first-out.
    //
    // Callers must be threads with an epoch slot (main or pool workers): retired nodes go to the
    // calling thread's epoch bag.
    template <typename T>
    class SkipListPQ {
    public:
        static constexpr int      kMaxLevel   = 16;
        static constexpr int      kSeqBits    = 40;
        static constexpr uint32_t kMaxPriority = (1u << (64 - kSeqBits)) - 1;

        explicit SkipListPQ(std::size_t boundOffset = 32) : boundOffset_(boundOffset) {
            tail_ = new Node(UINT64_MAX, T{}, kMaxLevel);
            head_ = new Node(0, T{}, kMaxLevel);
            head_->inserting.store(0, std::memory_order_relaxed);
            tail_->inserting.store(0, std::memory_order_relaxed);
            for (int i = 0; i < kMaxLevel; ++i)
                head_->next[i].store(pack(tail_, 0), std::memory_order_relaxed);
        }

        // Not concurrent with any operation. Nodes already cut off belong to the epoch bags; the
        // head-pointed node onward is still ours.
        ~SkipListPQ() {
            Node* cur = ptr_of(head_->next[0].load(std::memory_order_acquire));
            while (cur != tail_) {
                Node* nxt = ptr_of(cur->next[0].load(std::memory_order_relaxed));
                delete cur;
                cur = nxt;
            }
            delete head_;
            delete tail_;
        }

        SkipListPQ(const SkipListPQ&) = delete;
        SkipListPQ& operator=(const SkipListPQ&) = delete;

        // Algorithm 4.
        void insert(uint32_t priority, T value) {
            assert(priority <= kMaxPriority);
            const uint64_t seq = seq_.fetch_add(1, std::memory_order_relaxed);
            assert(seq < (uint64_t(1) << kSeqBits) - 1 && "sequence space exhausted");
            const uint64_t key = (uint64_t(priority) << kSeqBits) | seq;

            const int height = randomHeight();
            Node* n = new Node(key, std::move(value), height);   // inserting = 1
            Node* preds[kMaxLevel];
            Node* succs[kMaxLevel];

            EpochGuard guard;
            Node* del;
            for (;;) {
                del = locatePreds(key, preds, succs);
                n->next[0].store(pack(succs[0], 0), std::memory_order_relaxed);
                uintptr_t exp = pack(succs[0], 0);
                // Fails if succs[0] was deleted meanwhile: the flag lives in this word.
                if (preds[0]->next[0].compare_exchange_strong(exp, pack(n, 0),
                        std::memory_order_acq_rel, std::memory_order_relaxed))
                    break;
            }

            int i = 1;
            while (i < height) {
                n->next[i].store(pack(succs[i], 0), std::memory_order_relaxed);
                // n already deleted, or the search result is skewed by the deleted prefix: done.
                if (deletedFlag(n) || deletedFlag(succs[i]) || succs[i] == del) break;
                uintptr_t exp = pack(succs[i], 0);
                if (preds[i]->next[i].compare_exchange_strong(exp, pack(n, 0),
                        std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    ++i;
                } else {
                    del = locatePreds(key, preds, succs);
                    if (succs[0] != n) break;   // n has been deleted
                }
            }
            // A batch cut may now pass n.
            n->inserting.store(0, std::memory_order_release);
        }

        // Algorithm 2. Moves the minimum into `out`; false if empty.
        bool deleteMin(T& out) {
            EpochGuard guard;
            Node* x = head_;
            Node* newhead = nullptr;
            std::size_t offset = 0;
            const uintptr_t obshead = head_->next[0].load(std::memory_order_acquire);
            uintptr_t nxt;
            do {
                nxt = x->next[0].load(std::memory_order_acquire);
                if (ptr_of(nxt) == tail_) return false;
                // The head may not be cut past a node still being inserted.
#ifndef JLIB_PQ_CTL_NO_INSERTING
                if (!newhead && x->inserting.load(std::memory_order_acquire)) newhead = x;
#endif
                if (!mark_of(nxt))
                    nxt = x->next[0].fetch_or(1, std::memory_order_acq_rel);
                ++offset;
                x = ptr_of(nxt);
            } while (mark_of(nxt));

            out = std::move(x->value);   // x is ours: we set its delete flag

            if (offset < boundOffset_) return true;
            if (!newhead) newhead = x;
            // Compare against the RAW value read above, as the reference code does. The paper's
            // <obshead, 1> lets the CAS succeed after an insert, a claim and another thread's cut
            // have brought an unmarked head back to that value, moving the head backward onto a
            // retired node (the model's PAPER_OBSHEAD control).
#ifdef JLIB_PQ_CTL_PAPER_OBSHEAD
            uintptr_t exp = pack(ptr_of(obshead), 1);
#else
            uintptr_t exp = obshead;
#endif
            if (head_->next[0].compare_exchange_strong(exp, pack(newhead, 1),
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                restructure();
                EpochManager& em = EpochManager::Instance();
                const std::size_t epoch = em.CurrentEpoch();
                Node* cur = ptr_of(obshead);
                while (cur != newhead) {
                    Node* n2 = ptr_of(cur->next[0].load(std::memory_order_acquire));
                    em.RetirePtr(cur, epoch, &SkipListPQ::NodeDeleter);
                    cur = n2;
                }
            }
            return true;
        }

    private:
        struct Node {
            uint64_t               key;
            int                    height;
            std::atomic<uint32_t>  inserting;
            T                      value;
            std::atomic<uintptr_t> next[kMaxLevel];   // next[0] low bit: the successor is deleted

            Node(uint64_t k, T&& v, int h) : key(k), height(h), inserting(1), value(std::move(v)) {
                for (int i = 0; i < kMaxLevel; ++i)
                    next[i].store(0, std::memory_order_relaxed);
            }
        };
        static_assert(alignof(Node) >= 2, "tagged pointers need a spare low bit");

        static uintptr_t pack(Node* p, int mark) { return reinterpret_cast<uintptr_t>(p) | uintptr_t(mark); }
        static Node* ptr_of(uintptr_t v)  { return reinterpret_cast<Node*>(v & ~uintptr_t(1)); }
        static bool  mark_of(uintptr_t v) { return (v & 1) != 0; }
        static bool  deletedFlag(Node* n) { return mark_of(n->next[0].load(std::memory_order_acquire)); }
        static void  NodeDeleter(void* p) { delete static_cast<Node*>(p); }

        // Geometric height in [1, kMaxLevel]: one plus the trailing zeros of a random word.
        static int randomHeight() {
            static thread_local uint64_t s =
                0x9E3779B97F4A7C15ull ^ reinterpret_cast<uintptr_t>(&s);
            s ^= s << 13; s ^= s >> 7; s ^= s << 17;
            uint64_t x = s | (uint64_t(1) << (kMaxLevel - 1));
            int h = 1;
            while (!(x & 1)) { x >>= 1; ++h; }
            return h;
        }

        // Algorithm 5. Returns the last deleted node passed at level 0, if any.
        Node* locatePreds(uint64_t k, Node** preds, Node** succs) {
            Node* pred = head_;
            Node* del = nullptr;
            for (int i = kMaxLevel - 1; i >= 0; --i) {
                // <cur, d> <- <pred.next[i], pred.d>: at level 0 one load gives both.
                uintptr_t raw = pred->next[i].load(std::memory_order_acquire);
                Node* cur = ptr_of(raw);
                bool d = (i == 0) ? mark_of(raw) : deletedFlag(pred);
                while (cur->key < k || deletedFlag(cur) || (d && i == 0)) {
                    if (d && i == 0) del = cur;
                    pred = cur;
                    raw  = pred->next[i].load(std::memory_order_acquire);
                    cur  = ptr_of(raw);
                    d    = (i == 0) ? mark_of(raw) : deletedFlag(pred);
                }
                preds[i] = pred;
                succs[i] = cur;
            }
            return del;
        }

        // Algorithm 3: move the head's upper-level pointers past the deleted prefix.
        void restructure() {
            int i = kMaxLevel - 1;
            Node* pred = head_;
            while (i > 0) {
                uintptr_t hraw = head_->next[i].load(std::memory_order_acquire);
                Node* h   = ptr_of(hraw);
                Node* cur = ptr_of(pred->next[i].load(std::memory_order_acquire));
                if (!deletedFlag(h)) { --i; continue; }
                while (deletedFlag(cur)) {
                    pred = cur;
                    cur  = ptr_of(pred->next[i].load(std::memory_order_acquire));
                }
                if (head_->next[i].compare_exchange_strong(hraw, pack(cur, 0),
                        std::memory_order_acq_rel, std::memory_order_relaxed))
                    --i;
            }
        }

        Node*             head_;
        Node*             tail_;
        const std::size_t boundOffset_;
        alignas(64) std::atomic<uint64_t> seq_{ 1 };   // 1: no key equals the head's 0
    };

}
