// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include "LockFreeList.h"

namespace JLib {

    template <typename T>
    class LockFreeHashMap {
        TaskAllocator& allocator;
        
        std::unique_ptr<std::atomic<LockFreeList<T>*>[]> buckets;
        size_t bucketCount = 0;
        size_t mask = 0;

        static uint64_t Mix(uint64_t k) {
            k ^= k >> 30; k *= 0xbf58476d1ce4e5b9ULL;
            k ^= k >> 27; k *= 0x94d049bb133111ebULL;
            k ^= k >> 31;
            return k;
        }

        LockFreeList<T>* BucketFor(uint64_t key, bool create) {
            std::atomic<LockFreeList<T>*>& slot = buckets[Mix(key) & mask];
            LockFreeList<T>* b = slot.load(std::memory_order_acquire);
            if (b || !create) return b;

            auto* fresh = new (std::nothrow) LockFreeList<T>(allocator);
            if (!fresh) return nullptr;
            if (!fresh->ok()) { delete fresh; return nullptr; }

            if (slot.compare_exchange_strong(b, fresh,
                    std::memory_order_release, std::memory_order_acquire)) {
                return fresh;
            }
            delete fresh;   
            return b;
        }

    public:
        
        static size_t SuggestBuckets(size_t maxEntries) {
            size_t want = maxEntries / 8;
            if (want < 16)  want = 16;     
            if (want > 512) want = 512;    
            size_t n = 1;
            while (n < want) n <<= 1;
            return n;
        }

        explicit LockFreeHashMap(TaskAllocator& alloc, size_t buckets_ = 16) : allocator(alloc) {
            size_t n = 1;
            while (n < buckets_) n <<= 1;
            bucketCount = n;
            mask = n - 1;
            buckets.reset(new std::atomic<LockFreeList<T>*>[n]);
            for (size_t i = 0; i < n; ++i)
                buckets[i].store(nullptr, std::memory_order_relaxed);
        }

        ~LockFreeHashMap() {
            
            for (size_t i = 0; i < bucketCount; ++i)
                delete buckets[i].load(std::memory_order_acquire);
        }

        LockFreeHashMap(const LockFreeHashMap&) = delete;
        LockFreeHashMap& operator=(const LockFreeHashMap&) = delete;

        bool add(uint64_t key, T item) {
            LockFreeList<T>* b = BucketFor(key, true);
            return b && b->add(key, item);
        }

        bool remove(uint64_t key) {
            LockFreeList<T>* b = BucketFor(key, false);
            return b && b->remove(key);
        }

        bool contains(uint64_t key) {
            LockFreeList<T>* b = BucketFor(key, false);
            return b && b->contains(key);
        }

        size_t buckets_count() const { return bucketCount; }

        size_t bucket_index(uint64_t key) const { return Mix(key) & mask; }

        template <typename F>
        void for_each(F func) {
            for (size_t i = 0; i < bucketCount; ++i) {
                if (LockFreeList<T>* b = buckets[i].load(std::memory_order_acquire))
                    b->for_each(func);
            }
        }
    };
}
