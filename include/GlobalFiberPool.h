// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include "Fiber.h"
#include "FiberStackArena.h"
#include "platform.h"   
#include "Context.h"
#include "concurrentqueue.h"

namespace JLib {
    class Thread;
    class GlobalFiberPool {
        
        static constexpr size_t kStandardUsable  = 60  * 1024;
        static constexpr size_t kDeepUsable      = 508 * 1024;

        static size_t UsableFor(StackClass c) {
            return c == StackClass::Deep ? kDeepUsable : kStandardUsable;
        }

        static size_t RegionFor(StackClass c) {
            return UsableFor(c) + JLib::platform::PageSize();
        }

        static constexpr size_t kClassCount = 2;   // StackClass::Standard, StackClass::Deep

        // Fibers come in blocks: one Fiber array and one stack arena each. A block is never moved
        // or freed while the pool lives, so a Fiber* stays valid; the pool grows by one block when
        // a class runs dry, until the stack memory limit.
        struct Block {
            FiberStackArena* arena  = nullptr;
            Fiber*           fibers = nullptr;   // raw storage, placement-constructed
            size_t           count  = 0;         // fibers constructed
        };

        mutable std::mutex poolMutex;              // guards growth only

        moodycamel::ConcurrentQueue<Fiber*> availableFibers[kClassCount];
        std::atomic<size_t> size{ 0 };
        std::atomic<size_t> classCount[kClassCount] = {};
        std::atomic<size_t> committedBytes{ 0 };
        size_t memoryLimit = 0;                    // 0 = no limit
        size_t blockSize[kClassCount] = {};
        size_t nextIndex = 0;                      // poolIndex of the next fiber made (under the mutex)
        bool   warnedLimit[kClassCount] = {};
        bool   notedGrowth[kClassCount] = {};
        std::vector<Block> blocks[kClassCount];

        GlobalFiberPool(size_t standardCount, size_t deepCount, size_t memoryLimit);

        bool AddBlock(StackClass c, size_t count);   // caller holds poolMutex
        bool Grow(StackClass c);

    public:
        ~GlobalFiberPool();

        // memoryLimit: total stack bytes all classes may reach by growing (0 = no limit). The
        // initial counts are always made, even past it.
        static GlobalFiberPool* Create(size_t standardCount, size_t deepCount = 0,
                                       size_t memoryLimit = 0);

        size_t CountOf(StackClass c) const { return classCount[(size_t)c].load(std::memory_order_relaxed); }
        size_t CommittedBytes() const { return committedBytes.load(std::memory_order_relaxed); }
        size_t MemoryLimit() const { return memoryLimit; }
        static size_t StackBytesFor(StackClass c) { return RegionFor(c); }

        std::vector<Fiber*> StealBatch(size_t count, StackClass c = StackClass::Standard);

        size_t StealInto(Fiber** dest, size_t maxCount, StackClass c = StackClass::Standard);

        void ReturnBatch(Fiber** fibers, size_t count);
        static void FiberEntryWrapper();

        size_t AvailableCount() const;

        size_t TotalCount() const { return size.load(std::memory_order_relaxed); }
    };
}
