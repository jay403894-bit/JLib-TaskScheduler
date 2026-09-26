// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <vector>
#include "Fiber.h"
#include "GlobalFiberPool.h"

namespace JLib {
    
    template<size_t MaxCapacity = 256>
    struct alignas(platform::kCacheLine) ThreadLocalCache {
        Fiber* localFibers[MaxCapacity];
        size_t activeCapacity = 0;
        size_t count = 0;
        GlobalFiberPool* globalPool = nullptr;
        
        StackClass cls = StackClass::Standard;

        void Initialize(GlobalFiberPool* pool, size_t runtimeCapacity,
                        StackClass c = StackClass::Standard) {
            cls = c;
            activeCapacity = (runtimeCapacity <= MaxCapacity) ? runtimeCapacity : MaxCapacity;
            if (activeCapacity < 2) activeCapacity = 2; 
            globalPool = pool;
        }

        void Push(Fiber* f) {
            if (count >= activeCapacity) {
                
                size_t half = activeCapacity / 2;
                globalPool->ReturnBatch(&localFibers[count - half], half);
                count -= half;
            }
            localFibers[count++] = f; 
        }

        Fiber* Pop() {
            if (count == 0 && globalPool) {
                
                size_t got = globalPool->StealInto(localFibers, activeCapacity, cls);
                count = got;
            }
            if (count == 0) return nullptr;
            return localFibers[--count];
        }
    };
};
