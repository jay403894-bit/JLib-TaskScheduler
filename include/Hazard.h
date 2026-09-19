// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace JLib {

    class HazardDomain {
    public:
        
#ifndef JLIBSCHED_HAZARD_CELLS
    #define JLIBSCHED_HAZARD_CELLS 4
#endif
        static constexpr std::size_t kCellsPerReader = JLIBSCHED_HAZARD_CELLS;
        static_assert(kCellsPerReader >= 1, "a reader needs at least one hazard cell");

        [[noreturn]] static void FatalCellOverflow(std::size_t k, const char* what);

        static constexpr std::size_t kExternalReaders = 64;

        static HazardDomain& Instance();

        static constexpr std::size_t kNoReader = ~std::size_t(0);
        std::size_t CurrentReader();

        std::atomic<void*>* Cells(std::size_t reader);

        void Retire(void* p, void (*deleter)(void*));

        // Frees this thread's unprotected retires; with orphans set, also sweeps the orphan store.
        void Scan(bool orphans = true);

        std::size_t ReaderCount() { EnsureTable(); return readerCount; }
        std::size_t PendingRetired() const;

        std::size_t OrphanedRetired() const;

        // Moves this thread's pending retires to the shared orphan store (swept by any Scan).
        void HandOffPending();

        // Gives back this thread's external reader row, if it has one (ThreadScope's end). The
        // row's cells are cleared first; a later guard on this thread claims a row again.
        void ReleaseCurrentReader();
        std::size_t OrphanedTotal() const;

        void Init();

    private:
        void EnsureTable();

    public:
        [[noreturn]] static void FatalSuspendWithGuard(std::size_t depth);

        [[noreturn]] static void FatalNestedWorkerGuard();

        JLIB_NOINLINE static std::size_t& SuspendUnsafeDepth() noexcept;   // TLS; see JLIB_NOINLINE

    private:

        std::atomic<std::atomic<void*>*> cells{ nullptr };
        std::atomic<std::uint64_t>*      externalOwners = nullptr;   
        std::size_t workerCount = 0;
        std::size_t readerCount = 0;
        
    };

    class HazardGuard {
    public:
        HazardGuard();
        ~HazardGuard();

        HazardGuard(const HazardGuard&) = delete;
        HazardGuard& operator=(const HazardGuard&) = delete;

        template <typename T>
        T* Protect(std::size_t k, const std::atomic<T*>& src) {
            
            if (!cells || k >= HazardDomain::kCellsPerReader)
                HazardDomain::FatalCellOverflow(k, cells ? "cell index out of range"
                                                         : "no reader slot (external slots exhausted?)");
            for (;;) {
                T* p = src.load(std::memory_order_acquire);
                Set(k, static_cast<void*>(p));
                
                std::atomic_thread_fence(std::memory_order_seq_cst);
                if (src.load(std::memory_order_acquire) == p) return p;
            }
        }

        void Set(std::size_t k, void* p);
        void Clear(std::size_t k);
        void Swap(std::size_t a, std::size_t b);

        std::size_t Reader() const { return reader; }

    private:
        std::atomic<void*>* cells  = nullptr;

        bool countsForSuspend = false;
        
        std::size_t         reader = HazardDomain::kNoReader;
    };

    template <typename T>
    inline void HazardRetire(T* p) {
        if (!p) return;
        HazardDomain::Instance().Retire(
            static_cast<void*>(p),
            [](void* q) { delete static_cast<T*>(q); });
    }

    inline void HazardRetire(void* p, void (*deleter)(void*)) {
        if (p) HazardDomain::Instance().Retire(p, deleter);
    }

} 
