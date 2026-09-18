// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/CancelToken.h"

namespace JLib {
    namespace detail {

        CancelSlot* CancelSlotTable() {
            
            static CancelSlot* table = [] {
                static CancelSlot t[kCancelSlots];
                for (uint32_t i = 0; i < kCancelSlots; ++i) t[i].nextFree = i + 2;  
                t[kCancelSlots - 1].nextFree = 0;                                   
                return t;
            }();
            return table;
        }

        static std::atomic<uint64_t>& FreeHead() {
            static std::atomic<uint64_t> head{ 1 };   
            return head;
        }

        static constexpr uint64_t kIndexMask = 0xFFFFFFFFull;

        static uint32_t AcquireSlot() {
            CancelSlot* table = CancelSlotTable();
            uint64_t head = FreeHead().load(std::memory_order_acquire);
            for (;;) {
                const uint32_t idx = uint32_t(head & kIndexMask);
                if (idx == 0) return 0xFFFFFFFFu;              
                const uint64_t next = table[idx - 1].nextFree;
                const uint64_t bumped = (((head >> 32) + 1) << 32) | next;
                if (FreeHead().compare_exchange_weak(head, bumped,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                    return idx - 1;
            }
        }

        static void ReleaseSlot(uint32_t index) {
            CancelSlot* table = CancelSlotTable();
            uint64_t head = FreeHead().load(std::memory_order_acquire);
            for (;;) {
                
                table[index].nextFree = uint32_t(head & kIndexMask);
                const uint64_t bumped = (((head >> 32) + 1) << 32) | uint64_t(index + 1);
                if (FreeHead().compare_exchange_weak(head, bumped,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                    return;
            }
        }

        CancelSlot* ResolveCancelSlot(uint32_t raw) {
            const uint32_t index = raw & 0xFFFFu;
            const uint32_t gen = raw >> 16;
            if (index >= kCancelSlots) return nullptr;

            CancelSlot* s = &CancelSlotTable()[index];
            
            if (CancelSlot::GenOf(s->state.load(std::memory_order_acquire)) != gen) return nullptr;
            return s;
        }

    } 

    CancelScope::CancelScope() noexcept {
        const uint32_t i = detail::AcquireSlot();
        if (i == 0xFFFFFFFFu) {
            
            raw_ = CancelToken::kNone;
            return;
        }
        detail::CancelSlot* s = &detail::CancelSlotTable()[i];
        
        s->parent.store(CancelToken::kNone, std::memory_order_release);
        const uint32_t gen = detail::CancelSlot::GenOf(s->state.load(std::memory_order_acquire));
        raw_ = (gen << 16) | i;
    }

    CancelScope::CancelScope(CancelToken parent) noexcept {
        const uint32_t i = detail::AcquireSlot();
        if (i == 0xFFFFFFFFu) { raw_ = CancelToken::kNone; return; }

        detail::CancelSlot* s = &detail::CancelSlotTable()[i];
        
        s->parent.store(detail::ResolveCancelSlot(parent.Raw()) ? parent.Raw() : CancelToken::kNone,
                        std::memory_order_release);
        const uint32_t gen = detail::CancelSlot::GenOf(s->state.load(std::memory_order_acquire));
        raw_ = (gen << 16) | i;
    }

    CancelScope::~CancelScope() {
        if (raw_ == CancelToken::kNone) return;
        const uint32_t i = raw_ & 0xFFFFu;
        detail::CancelSlot* s = &detail::CancelSlotTable()[i];
        
        uint64_t st = s->state.load(std::memory_order_acquire);
        while (!s->state.compare_exchange_weak(
                   st, uint64_t(uint32_t(st >> 32) + 1) << 32,
                   std::memory_order_acq_rel, std::memory_order_acquire)) {
            
        }

        s->parent.store(CancelToken::kNone, std::memory_order_release);
        detail::ReleaseSlot(i);
    }

    void CancelScope::Cancel() noexcept {
        if (raw_ == CancelToken::kNone) return;
        
        detail::CancelSlotTable()[raw_ & 0xFFFFu].state
            .fetch_or(detail::CancelSlot::kCancelledBit, std::memory_order_acq_rel);
    }

    bool CancelScope::Cancelled() const noexcept {
        return CancelToken(raw_).Cancelled();
    }

    bool CancelVia(CancelToken token) noexcept {
        const uint32_t raw = token.Raw();
        if (raw == CancelToken::kNone) return false;
        const uint32_t index = raw & 0xFFFFu;
        if (index >= detail::kCancelSlots) return false;

        detail::CancelSlot* s = &detail::CancelSlotTable()[index];
        uint64_t st = s->state.load(std::memory_order_acquire);
        for (;;) {
            
            if (detail::CancelSlot::GenOf(st) != (raw >> 16)) return false;
            if (detail::CancelSlot::CancelledIn(st)) return true;   
            if (s->state.compare_exchange_weak(st, st | detail::CancelSlot::kCancelledBit,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire))
                return true;
            
        }
    }

} 
