// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "SlabPool.h"
#include <cstddef>

namespace JLib {

    class TaskAllocator {
    public:
        
        static constexpr std::size_t SLOT       = 256;
        static constexpr std::size_t MID_SLOT   = 128;

        static constexpr std::size_t SLOT80     = 80;
        static constexpr std::size_t SMALL_SLOT = 64;
        // Two cache lines' worth of 256: coroutine frames and lambda tasks with some state land
        // just over 256 B, and without this class every one of them went to the heap.
        static constexpr std::size_t LARGE_SLOT = 512;
        static constexpr std::size_t BATCH      = SlabPool<SLOT>::BATCH;

    private:

        SlabPool<LARGE_SLOT> pool512;
        SlabPool<SLOT>       pool256;
        SlabPool<MID_SLOT>   pool128;
        SlabPool<SLOT80>     pool80;
        SlabPool<SMALL_SLOT> pool64;

    public:

        explicit TaskAllocator(std::size_t bigSlots, std::size_t midSlots,
                               std::size_t slots80, std::size_t smallSlots,
                               std::size_t largeSlots, bool lazy = false)
            : pool512(largeSlots, lazy)
            , pool256(bigSlots, lazy)
            , pool128(midSlots, lazy)
            , pool80(slots80, lazy)
            , pool64(smallSlots, lazy) {
        }

        explicit TaskAllocator(std::size_t slots, bool lazy = false)
            : TaskAllocator(slots, slots / 8, slots / 8, slots / 8, slots / 8, lazy) {
        }
        TaskAllocator& operator=(const TaskAllocator&) = delete;

        void* Alloc()                     { return pool256.Alloc(); }

        void  Free(void* slot)            { if (!FreeSized(slot)) ::operator delete(slot); }
        bool  SlotInSlab(const void* p) const { return pool256.SlotInSlab(p); }
        long long LiveCount() const       { return pool256.LiveCount(); }

        std::size_t Capacity() const      { return pool256.Capacity() + pool128.Capacity()
                                                 + pool80.Capacity()  + pool64.Capacity()
                                                 + pool512.Capacity(); }
        std::size_t BigCapacity() const   { return pool256.Capacity(); }
        void Prefault(std::size_t slots)  { pool256.Prefault(slots); }

        void* AllocSized(std::size_t n) {
            if (n <= SMALL_SLOT) if (void* p = pool64.Alloc())  return p;
            if (n <= SLOT80)     if (void* p = pool80.Alloc())  return p;
            if (n <= MID_SLOT)   if (void* p = pool128.Alloc())   return p;
            if (n <= SLOT)       if (void* p = pool256.Alloc())   return p;
            if (n <= LARGE_SLOT) if (void* p = pool512.Alloc())   return p;
            return nullptr;
        }

        bool FreeSized(void* p) {
            if (pool64.SlotInSlab(p)) { pool64.Free(p); return true; }
            if (pool80.SlotInSlab(p)) { pool80.Free(p); return true; }
            if (pool128.SlotInSlab(p))   { pool128.Free(p);   return true; }
            if (pool256.SlotInSlab(p))   { pool256.Free(p);   return true; }
            if (pool512.SlotInSlab(p))   { pool512.Free(p);   return true; }
            return false;
        }

        bool OwnsSized(const void* p) const {
            return pool64.SlotInSlab(p) || pool80.SlotInSlab(p)
                || pool128.SlotInSlab(p) || pool256.SlotInSlab(p) || pool512.SlotInSlab(p);
        }

        long long   SmallLiveCount() const { return pool64.LiveCount(); }
        std::size_t SmallCapacity()  const { return pool64.Capacity(); }
        long long   MidLiveCount()   const { return pool128.LiveCount(); }
        long long   Slot80LiveCount() const { return pool80.LiveCount(); }
        std::size_t Slot80Capacity()  const { return pool80.Capacity(); }

        std::size_t MidCapacity()    const { return pool128.Capacity(); }

        struct ClassUsage {
            std::size_t slotBytes;
            std::size_t capacity;      
            std::size_t resident;      
            long long   peakLive;      
            long long   live;          
            std::size_t extents;       
        };
        struct Usage { ClassUsage c64, c80, c128, c256, c512; };

        Usage UsageProfile() const {
            return Usage{
                { 64,  pool64.Capacity(),  pool64.HighWaterSlots(),  SlabPool<SMALL_SLOT>::PeakLive(), pool64.LiveCount(),  pool64.ExtentCount()  },
                { 80,  pool80.Capacity(),  pool80.HighWaterSlots(),  SlabPool<SLOT80>::PeakLive(),     pool80.LiveCount(),  pool80.ExtentCount()  },
                { 128, pool128.Capacity(), pool128.HighWaterSlots(), SlabPool<MID_SLOT>::PeakLive(),   pool128.LiveCount(), pool128.ExtentCount() },
                { 256, pool256.Capacity(), pool256.HighWaterSlots(), SlabPool<SLOT>::PeakLive(),       pool256.LiveCount(), pool256.ExtentCount() },
                { 512, pool512.Capacity(), pool512.HighWaterSlots(), SlabPool<LARGE_SLOT>::PeakLive(), pool512.LiveCount(), pool512.ExtentCount() },
            };
        }

        std::size_t HighWaterBytes() const {
            return pool64.HighWaterBytes() + pool80.HighWaterBytes()
                 + pool128.HighWaterBytes() + pool256.HighWaterBytes() + pool512.HighWaterBytes();
        }
    };
}
