// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstddef>
#include <optional>
#include <iostream>
#include <stdexcept>
#include <algorithm>

#include <vector>          
#include "CspTask.h"
#include "platform.h"   

namespace JLib {
namespace csp {

    class alignas(platform::kCacheLine) TaskDeque {
    private:
        
        struct Ring {
            size_t                  mask;
            size_t                  capacity;
            std::atomic<uintptr_t>* slots;
        };

    public:
        
        explicit TaskDeque(size_t capacity = 32768, size_t maxCapacity = kDefaultMaxCapacity)
            : maxCapacity_(maxCapacity < capacity ? capacity : maxCapacity),
              ceilingIsHard_(maxCapacity != kDefaultMaxCapacity) {
            if ((capacity & (capacity - 1)) != 0)
                throw std::runtime_error("Capacity must be a power of 2");
            ring_.store(MakeRing(capacity), std::memory_order_relaxed);
            top_.store(0, std::memory_order_relaxed);
            bottom_.store(0, std::memory_order_relaxed);
        }

        ~TaskDeque() {
            for (Ring* r : retired_) { delete[] r->slots; delete r; }
            Ring* r = ring_.load(std::memory_order_relaxed);
            delete[] r->slots;
            delete r;
        }

        static constexpr size_t kDefaultMaxCapacity = 1u << 16;   

        static constexpr size_t kHardMaxCapacity = 1u << 22;

        static inline std::atomic<size_t> g_growCount{ 0 };
        static size_t GrowCount() { return g_growCount.load(std::memory_order_relaxed); }

        void SetOwnerTag(size_t index, const char* lane) noexcept {
            ownerIndex_ = index;
            ownerLane_  = lane;
        }

        static Ring* MakeRing(size_t capacity) {
            Ring* r = new Ring{ capacity - 1, capacity, new std::atomic<uintptr_t>[capacity] };
            for (size_t i = 0; i < capacity; ++i)
                r->slots[i].store(0, std::memory_order_relaxed);
            return r;
        }

        void grow(Ring* old, size_t t, size_t b) {
            const size_t newCap = old->capacity * 2;
            if (newCap > maxCapacity_) {
                
                if (ceilingIsHard_)
                    FatalGrow(old->capacity, "ceiling reached", ownerIndex_, ownerLane_, t, b);

                if (newCap > kHardMaxCapacity)
                    FatalGrow(old->capacity, "hard ceiling reached", ownerIndex_, ownerLane_, t, b);

                if (!warnedPastCeiling_) {
                    warnedPastCeiling_ = true;
                    std::fprintf(stderr,
                        "[JLib::Scheduler] NOTE: deque (queue %zu, %s lane) grew PAST its %zu-slot\n"
                        "  sizing ceiling to %zu, holding %zu tasks. Growth continues to %zu slots.\n"
                        "  One lane this deep means placement is concentrating rather than spreading\n"
                        "  -- and the usual cause is bulk submitted through Push() in a loop, which\n"
                        "  means \"start this now\" and steers. Bulk goes through PushBatch.\n",
                        ownerIndex_, ownerLane_, maxCapacity_, newCap, b - t, kHardMaxCapacity);
                    std::fflush(nullptr);
                }
            }

            Ring* r = nullptr;
            try {
                r = MakeRing(newCap);
            }
            catch (const std::bad_alloc&) {
                FatalGrow(old->capacity, "out of memory", ownerIndex_, ownerLane_, t, b);
            }

            for (size_t i = t; i != b; ++i)
                r->slots[i & r->mask].store(old->slots[i & old->mask].load(std::memory_order_relaxed),
                                            std::memory_order_relaxed);

            ring_.store(r, std::memory_order_release);

            retired_.push_back(old);
            g_growCount.fetch_add(1, std::memory_order_relaxed);
        }

        [[noreturn]] static void FatalPushRefused() {
            std::fprintf(stderr,
                "[JLib::Scheduler] FATAL: push_bottom refused a non-null item.\n"
                "  The deque grows rather than filling, so the only documented refusal is a null\n"
                "  task -- which this call site has already excluded. Reaching here means that\n"
                "  invariant broke. Stopping rather than dropping the task: a dropped task never\n"
                "  signals its WaitGroup, and the failure would surface as an unexplained hang.\n");
            std::fflush(stderr);
            std::abort();
        }

        [[noreturn]] static void FatalGrow(size_t capacity, const char* why,
                                           size_t owner, const char* lane,
                                           size_t top, size_t bottom) {
            std::fprintf(stderr,
                "[JLib::Scheduler] FATAL: a work-stealing deque could not grow past %zu slots -- %s.\n"
                "  owner: queue %zu, %s lane   top=%zu bottom=%zu  (bottom-top=%zu)\n"
                "  A lane holds this many tasks only if something is spawning without bound; the\n"
                "  ceiling is this deque's maxCapacity (default kDefaultMaxCapacity in TaskDeque.h).\n"
                "  This is fatal rather than dropped because a dropped task never signals its\n"
                "  WaitGroup, and the failure would surface as a hang somewhere else entirely.\n",
                capacity, why, owner, lane, top, bottom, bottom - top);
            
            std::fflush(nullptr);
            std::abort();
        }
        
        static uintptr_t tag(Task* item)  { return reinterpret_cast<uintptr_t>(item); }
        static Task*     untag(uintptr_t v) { return reinterpret_cast<Task*>(v); }

        bool push_bottom(Task* item) {
            if (!item) {
                std::cerr << "[TaskDeque::push_bottom] ERROR: pushing null item!\n";
                return false;
            }
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);

            Ring* r = ring_.load(std::memory_order_relaxed);
            if (b - t >= r->capacity) {
                
                grow(r, t, b);   
                r = ring_.load(std::memory_order_relaxed);
            }
            r->slots[b & r->mask].store(tag(item), std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_release);
            bottom_.store(b + 1, std::memory_order_release);
            return true;
        }

        bool push_bottom_batch(Task** items, size_t count) {
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);

            Ring* r = ring_.load(std::memory_order_relaxed);
            while ((b + count) - t > r->capacity) {
                
                grow(r, t, b);
                r = ring_.load(std::memory_order_relaxed);
            }

            for (size_t i = 0; i < count; ++i) {
                r->slots[(b + i) & r->mask].store(tag(items[i]), std::memory_order_relaxed);
            }

            std::atomic_thread_fence(std::memory_order_release);
            bottom_.store(b + count, std::memory_order_release);
            return true;
        }

        std::optional<Task*> pop_bottom() {
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);

            if (t >= b) {
                return std::nullopt;  
            }

            b -= 1;
            bottom_.store(b, std::memory_order_release);

            std::atomic_thread_fence(std::memory_order_seq_cst);

            t = top_.load(std::memory_order_acquire);

            if (t <= b) {
                Ring* r = ring_.load(std::memory_order_relaxed);
                Task* item = untag(r->slots[b & r->mask].load(std::memory_order_relaxed));
                if (t == b) {
                    
                    if (!top_.compare_exchange_strong(
                        t, t + 1,
                        std::memory_order_seq_cst,
                        std::memory_order_relaxed))
                    {
                        
                        bottom_.store(b + 1, std::memory_order_relaxed);
                        return std::nullopt;
                    }
                    
                    bottom_.store(b + 1, std::memory_order_relaxed);
                }
                return item;
            }
            else {
                
                bottom_.store(t, std::memory_order_relaxed);
                return std::nullopt;
            }
        }

        std::optional<Task*> steal() {
            size_t t = top_.load(std::memory_order_acquire);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            size_t b = bottom_.load(std::memory_order_acquire);

            if (t < b) {
                
                Ring* r = ring_.load(std::memory_order_acquire);
                Task* item = untag(r->slots[t & r->mask].load(std::memory_order_relaxed));
                if (top_.compare_exchange_strong(
                    t, t + 1,
                    std::memory_order_seq_cst,   
                    std::memory_order_relaxed))
                {
                    return item;
                }
            }
            return std::nullopt;
        }
        
        size_t size() const {
            size_t t = top_.load(std::memory_order_acquire);
            size_t b = bottom_.load(std::memory_order_acquire);
            return (b > t) ? (b - t) : 0;
        }

        size_t capacity() const {
            return ring_.load(std::memory_order_acquire)->capacity;
        }
        bool empty() const {
            size_t t = top_.load(std::memory_order_acquire);
            size_t b = bottom_.load(std::memory_order_acquire);
            return t >= b;
        }
    private:
        
        std::atomic<Ring*> ring_;

        std::vector<Ring*> retired_;

        const size_t maxCapacity_;

        const bool  ceilingIsHard_;

        bool        warnedPastCeiling_ = false;

        size_t      ownerIndex_ = SIZE_MAX;
        const char* ownerLane_  = "untagged";

        alignas(platform::kCacheLine) std::atomic<size_t> top_;
        alignas(platform::kCacheLine) std::atomic<size_t> bottom_;
    };

}
}   
