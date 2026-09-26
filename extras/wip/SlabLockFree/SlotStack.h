#pragma once
#include <atomic>
#include <cstddef>
struct SlotStack {
    std::atomic<void*> head{ nullptr };

    static void*& next(void* p) { return *reinterpret_cast<void**>(p); }

    void push(void* slot) {
        void* h = head.load(std::memory_order_relaxed);
        do { next(slot) = h; } while (!head.compare_exchange_weak(h, slot,
            std::memory_order_release, std::memory_order_relaxed));
    }

    void* pop() {
        void* h = head.load(std::memory_order_relaxed);
        while (h) {
            void* n = next(h);
            if (head.compare_exchange_weak(h, n,
                std::memory_order_acquire, std::memory_order_relaxed))
                return h;
        }
        return nullptr;
    }

    // batch already linked: batchHead .. batchTail, next(tail) unset
    void pushBatch(void* batchHead, void* batchTail) {
        void* h = head.load(std::memory_order_relaxed);
        do { next(batchTail) = h; } while (!head.compare_exchange_weak(h, batchHead,
            std::memory_order_release, std::memory_order_relaxed));
    }

    // Returns how many were taken.
    std::size_t popBatch(void*& outHead, void*& outTail, std::size_t maxN) {
        outHead = outTail = nullptr;
        std::size_t n = 0;
        for (; n < maxN; ++n) {
            void* s = pop();
            if (!s) break;
            next(s) = outHead;
            if (!outHead) outTail = s;
            outHead = s;
        }
        return n;
    }
};