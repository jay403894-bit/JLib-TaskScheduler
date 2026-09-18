#pragma once
// Lock-free skiplist set/map (Fraser / Herlihy-Shavit style) with epoch reclamation.
//
// Changes from the previous version:
//   - popMin removed: the priority queue is its own structure now.
//   - Value stored inline: get() copies it under the epoch guard; no T* ownership to race on.
//   - Insert/remove race closed: a node has two owners (inserter, winning remover) and is retired
//     only when both are done, so a remover can no longer retire a node the inserter is still
//     linking. The inserter updates its own upper links by CAS, so it never erases a remover's mark.
//   - seq_cst fences on both sides of the "linked at level L" / "marked at level 0" handoff
//     (store-buffering shape), so either the remover's find sees the link or the inserter sees
//     the mark and cleans up itself.
//   - Node allocated once per add, not once per retry; xorshift + countr_zero for levels.
//   - UINT64_MAX is reserved for the tail sentinel.

#include <atomic>
#include <bit>
#include <cstdint>
#include <cstddef>
#include <utility>
#include "Epochs.h"

namespace JLib {

    template <typename T>
    class SkipListTagged {
    public:
        static constexpr int      kMaxLevel = 16;
        static constexpr uint64_t kMaxKey   = UINT64_MAX - 1;   // UINT64_MAX is the tail

    private:
        struct NodeBase {
            const uint64_t         key;
            const int              topLevel;
            std::atomic<uintptr_t> next[kMaxLevel];

            NodeBase(uint64_t k, int top) : key(k), topLevel(top) {
                for (auto& n : next) n.store(0, std::memory_order_relaxed);
            }
        };

        struct Node : NodeBase {
            std::atomic<int> owners{ 2 };   // inserter + winning remover; last one out retires
            T value;                        // immutable after construction

            template <typename... A>
            Node(uint64_t k, int top, A&&... a) : NodeBase(k, top), value(std::forward<A>(a)...) {}
        };

        static_assert(alignof(NodeBase) >= 2, "tagged pointers need a spare low bit");

        static uintptr_t pack(NodeBase* p, bool mark) noexcept {
            return reinterpret_cast<uintptr_t>(p) | (mark ? uintptr_t(1) : uintptr_t(0));
        }
        static NodeBase* ptr_of(uintptr_t v) noexcept  { return reinterpret_cast<NodeBase*>(v & ~uintptr_t(1)); }
        static bool      mark_of(uintptr_t v) noexcept { return (v & uintptr_t(1)) != 0; }

        NodeBase* head_;
        NodeBase* tail_;

        static uint64_t mix(uint64_t z) noexcept {   // splitmix64 finalizer
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
            z ^= z >> 31;
            return z ? z : 1;                        // xorshift state must be nonzero
        }

        // Geometric(1/2) level in [0, kMaxLevel-1]. Seeded from the TLS slot's own address, so
        // every thread gets a different stream with no shared counter.
        static int randomLevel() noexcept {
            static thread_local uint64_t s = mix(reinterpret_cast<uintptr_t>(&s) ^ 0x9E3779B97F4A7C15ull);
            s ^= s << 13; s ^= s >> 7; s ^= s << 17;
            return std::countr_zero(s | (uint64_t(1) << (kMaxLevel - 1)));
        }

        static void release(Node* n) noexcept {
            if (n->owners.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                EpochManager::Instance().RetirePtr(
                    n, EpochManager::Instance().CurrentEpoch(),
                    [](void* p) { delete static_cast<Node*>(p); });
            }
        }

    public:
        SkipListTagged() {
            tail_ = new NodeBase(UINT64_MAX, kMaxLevel - 1);
            head_ = new NodeBase(0,          kMaxLevel - 1);
            for (int i = 0; i < kMaxLevel; ++i)
                head_->next[i].store(pack(tail_, false), std::memory_order_relaxed);
        }

        // Requires quiescence: no concurrent operations. Removed nodes were unlinked and handed to
        // the epoch manager, so only live nodes are still reachable here.
        ~SkipListTagged() {
            NodeBase* cur = ptr_of(head_->next[0].load(std::memory_order_relaxed));
            while (cur != tail_) {
                NodeBase* nxt = ptr_of(cur->next[0].load(std::memory_order_relaxed));
                delete static_cast<Node*>(cur);
                cur = nxt;
            }
            delete head_;
            delete tail_;
        }

        SkipListTagged(const SkipListTagged&) = delete;
        SkipListTagged& operator=(const SkipListTagged&) = delete;

    private:
        // Fills preds/succs at every level and physically unlinks every marked node it passes.
        // After it returns, no node marked at level L lies on the level-L path to `key`.
        bool find(uint64_t key, NodeBase** preds, NodeBase** succs) {
        retry:
            NodeBase* pred = head_;
            for (int level = kMaxLevel - 1; level >= 0; --level) {
                NodeBase* curr = ptr_of(pred->next[level].load(std::memory_order_acquire));
                for (;;) {
                    uintptr_t succRaw = curr->next[level].load(std::memory_order_acquire);
                    while (mark_of(succRaw)) {
                        uintptr_t expected = pack(curr, false);
                        if (!pred->next[level].compare_exchange_strong(
                                expected, pack(ptr_of(succRaw), false),
                                std::memory_order_seq_cst, std::memory_order_relaxed))
                            goto retry;   // pred changed or was marked itself
                        curr    = ptr_of(succRaw);
                        succRaw = curr->next[level].load(std::memory_order_acquire);
                    }
                    if (curr->key < key) { pred = curr; curr = ptr_of(succRaw); }
                    else break;
                }
                preds[level] = pred;
                succs[level] = curr;
            }
            return succs[0]->key == key;
        }

        // Read-only search: skips marked nodes without unlinking them.
        NodeBase* locate(uint64_t key) const {
            NodeBase* pred = head_;
            NodeBase* curr = nullptr;
            for (int level = kMaxLevel - 1; level >= 0; --level) {
                curr = ptr_of(pred->next[level].load(std::memory_order_acquire));
                for (;;) {
                    uintptr_t succRaw = curr->next[level].load(std::memory_order_acquire);
                    while (mark_of(succRaw)) {   // curr is deleted at this level: step past it
                        curr    = ptr_of(succRaw);
                        succRaw = curr->next[level].load(std::memory_order_acquire);
                    }
                    if (curr->key < key) { pred = curr; curr = ptr_of(succRaw); }
                    else break;
                }
            }
            return curr;
        }

    public:
        template <typename... A>
        bool add(uint64_t key, A&&... args) {
            if (key > kMaxKey) return false;
            EpochGuard guard;
            NodeBase* preds[kMaxLevel];
            NodeBase* succs[kMaxLevel];
            Node* node = nullptr;

            for (;;) {
                if (find(key, preds, succs)) { delete node; return false; }   // node never published
                if (!node) node = new Node(key, randomLevel(), std::forward<A>(args)...);
                for (int level = 0; level <= node->topLevel; ++level)
                    node->next[level].store(pack(succs[level], false), std::memory_order_relaxed);

                uintptr_t expected = pack(succs[0], false);
                if (preds[0]->next[0].compare_exchange_strong(
                        expected, pack(node, false),
                        std::memory_order_seq_cst, std::memory_order_relaxed))
                    break;   // linearization point: the key is in the set
            }

            for (int level = 1; level <= node->topLevel; ++level) {
                for (;;) {
                    // Aim our own link at the current successor. CAS, not store: if a remover has
                    // marked this level, the CAS fails and we stop linking rather than erase the mark.
                    uintptr_t own = node->next[level].load(std::memory_order_acquire);
                    if (mark_of(own)) goto linked;
                    if (ptr_of(own) != succs[level] &&
                        !node->next[level].compare_exchange_strong(
                            own, pack(succs[level], false),
                            std::memory_order_acq_rel, std::memory_order_acquire))
                        goto linked;   // only a remover writes here before we link: it was marked

                    uintptr_t exp = pack(succs[level], false);
                    if (preds[level]->next[level].compare_exchange_strong(
                            exp, pack(node, false),
                            std::memory_order_seq_cst, std::memory_order_relaxed))
                        break;
                    find(key, preds, succs);   // refresh the path and retry this level
                }
            }

        linked:
            // Pairs with the fence in remove(): either the remover's find sees every level we
            // linked, or we see its level-0 mark here and unlink them ourselves.
            std::atomic_thread_fence(std::memory_order_seq_cst);
            if (mark_of(node->next[0].load(std::memory_order_relaxed)))
                find(key, preds, succs);
            release(node);   // the inserter is done with it
            return true;
        }

        bool remove(uint64_t key) {
            if (key > kMaxKey) return false;
            EpochGuard guard;
            NodeBase* preds[kMaxLevel];
            NodeBase* succs[kMaxLevel];

            if (!find(key, preds, succs)) return false;
            Node* victim = static_cast<Node*>(succs[0]);   // key <= kMaxKey: never a sentinel

            // Upper levels first, top down, so level 0 (the linearization point) is marked last.
            for (int level = victim->topLevel; level >= 1; --level) {
                uintptr_t raw = victim->next[level].load(std::memory_order_acquire);
                while (!mark_of(raw) &&
                       !victim->next[level].compare_exchange_weak(
                           raw, raw | uintptr_t(1),
                           std::memory_order_acq_rel, std::memory_order_acquire)) {}
            }

            uintptr_t raw = victim->next[0].load(std::memory_order_acquire);
            for (;;) {
                if (mark_of(raw)) return false;   // another remover won
                if (victim->next[0].compare_exchange_weak(
                        raw, raw | uintptr_t(1),
                        std::memory_order_seq_cst, std::memory_order_acquire))
                    break;
            }

            std::atomic_thread_fence(std::memory_order_seq_cst);   // pairs with add()'s fence
            find(key, preds, succs);   // unlink at every level where it is still reachable
            release(victim);           // the remover is done with it
            return true;
        }

        bool contains(uint64_t key) {
            if (key > kMaxKey) return false;
            EpochGuard guard;
            NodeBase* curr = locate(key);
            return curr->key == key && !mark_of(curr->next[0].load(std::memory_order_acquire));
        }

        bool get(uint64_t key, T& out) {
            if (key > kMaxKey) return false;
            EpochGuard guard;
            NodeBase* curr = locate(key);
            if (curr->key != key) return false;
            if (mark_of(curr->next[0].load(std::memory_order_acquire))) return false;
            out = static_cast<Node*>(curr)->value;   // node pinned by the epoch; value immutable
            return true;
        }
    };

}
