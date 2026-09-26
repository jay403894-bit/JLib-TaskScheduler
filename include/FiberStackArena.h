// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"   
#include <atomic>
#include <stdexcept>

class FiberStackArena {
    size_t pageSize;
    void* base;
    size_t totalSize;
    std::atomic<size_t> offset;

public:
    FiberStackArena(size_t capacity)
        : pageSize(JLib::platform::PageSize()) {
        
        base = JLib::platform::ReserveNoAccess(capacity);
        if (!base) throw std::runtime_error("FiberStackArena: reservation failed");
        totalSize = capacity;
        offset = 0;
    }
    ~FiberStackArena() {
        if (base) {
            JLib::platform::ReleaseReservation(base, totalSize);
        }
    }
    void* AllocateStack(size_t rawSize) {
        
        const size_t size = (rawSize + pageSize - 1) & ~(pageSize - 1);
        if (size <= pageSize) return nullptr;  

        const size_t current = offset.fetch_add(size, std::memory_order_relaxed);
        
        if (current > totalSize || size > totalSize - current) return nullptr;

        char* region = (char*)base + current;
        if (!JLib::platform::CommitReadWrite(region + pageSize, size - pageSize))
            return nullptr;

        return region;
    }
    // The reservation itself, so the pool can map a stack pointer back to its fiber.
    void*  Base()     const { return base; }
    size_t Capacity() const { return totalSize; }
};
