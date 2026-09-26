// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstddef>
#include <type_traits>
#include "platform.h"

namespace JLib {

    // Vyukov-style intrusive MPSC. Many producers push, ONE consumer pops -- and the single
    // consumer is a requirement of the algorithm, not a convention: `tail_` is plain consumer-owned
    // state, so a second popper corrupts it. Where that matters, say so at the owning site.
    //
    // The node is a MEMBER of T, not a base, so T keeps its own layout and can sit in a slab slot
    // or inside another object. No allocation here: the stub is embedded and the queue is usable
    // immediately after construction.
    struct MPSCNode {
        std::atomic<MPSCNode*> next{ nullptr };
    };

    template <typename T>
    struct MPSCTraits {
        static MPSCNode* Hook(T* p) { return &p->mpscHook; }
        static T* Owner(MPSCNode* n) {
            return reinterpret_cast<T*>(reinterpret_cast<char*>(n) - offsetof(T, mpscHook));
        }
    };

    template<typename T, typename Traits = MPSCTraits<T>>
    class MPSCQueue {
        // MPSCTraits::Owner walks back from the hook with offsetof, which is only defined for a
        // standard-layout type. Without this it compiles and works on MSVC/GCC/Clang today and is
        // UB the moment someone gives T a virtual, a base with members, or mixed access.
        static_assert(std::is_standard_layout<T>::value,
                      "MPSCQueue<T>: T must be standard-layout -- MPSCTraits::Owner uses offsetof");

        alignas(JLib::platform::kCacheLine) std::atomic<MPSCNode*> head_;
        // tail_ is consumer-only. The stub shares this line ON PURPOSE: a producer writes
        // stub_.next only in the consumer's own append(&stub_), so the line is not contended.
        alignas(JLib::platform::kCacheLine) MPSCNode* tail_;
        MPSCNode stub_;

        void append(MPSCNode* n) {
            n->next.store(nullptr, std::memory_order_relaxed);
            MPSCNode* prev = head_.exchange(n, std::memory_order_acq_rel);
            prev->next.store(n, std::memory_order_release);
        }

    public:

        MPSCQueue() : head_(&stub_), tail_(&stub_) {}
        MPSCQueue(const MPSCQueue&) = delete;
        MPSCQueue& operator=(const MPSCQueue&) = delete;

        void push(T* node) { append(Traits::Hook(node)); }

        // CONSUMER ONLY. False can mean "empty" or "a producer is mid-push": the exchange in
        // append publishes the new head before linking it, so there is a window where the queue is
        // not empty but the link is not visible yet. A consumer that must not miss an item retries.
        bool pop(T*& out) {
            MPSCNode* tail = tail_;
            MPSCNode* next = tail->next.load(std::memory_order_acquire);

            if (tail == &stub_) {
                if (!next) return false;
                tail_ = next;
                tail = next;
                next = next->next.load(std::memory_order_acquire);
            }

            if (next) {
                tail_ = next;
                out = Traits::Owner(tail);
                return true;
            }

            if (tail != head_.load(std::memory_order_acquire)) return false;

            append(&stub_);
            next = tail->next.load(std::memory_order_acquire);
            if (next) {
                tail_ = next;
                out = Traits::Owner(tail);
                return true;
            }
            return false;
        }

        // CONSUMER ONLY: it reads tail_.
        bool empty() const {
            return tail_ == &stub_ && tail_->next.load(std::memory_order_acquire) == nullptr;
        }
    };
}
