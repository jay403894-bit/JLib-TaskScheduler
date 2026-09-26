// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include <atomic>
#include <cstdint>

namespace JLib {

    class CancelToken;

    namespace detail {
        
        struct CancelSlot {
            
            std::atomic<uint64_t> state{ 0 };

            static constexpr uint64_t kCancelledBit = 1;
            static uint32_t GenOf(uint64_t s) noexcept { return uint32_t(s >> 32) & 0xFFFFu; }
            static bool     CancelledIn(uint64_t s) noexcept { return (s & kCancelledBit) != 0; }

            std::atomic<uint32_t> parent{ 0xFFFFFFFFu };
            
            uint32_t nextFree{ 0 };
        };

        static_assert(sizeof(CancelSlot) == 16, "CancelSlot pads the table; keep it 16 bytes");

        inline constexpr uint32_t kCancelSlots = 65535;
        CancelSlot* CancelSlotTable();

        CancelSlot* ResolveCancelSlot(uint32_t raw);

        inline constexpr int kMaxScopeDepth = 8;
    }

    class CancelToken {
    public:
        static constexpr uint32_t kNone = 0xFFFFFFFFu;

        CancelToken() noexcept = default;
        explicit CancelToken(uint32_t raw) noexcept : raw_(raw) {}

        bool Valid() const noexcept { return raw_ != kNone; }
        uint32_t Raw() const noexcept { return raw_; }

        bool Cancelled() const noexcept {
            uint32_t raw = raw_;
            for (int depth = 0; depth < detail::kMaxScopeDepth; ++depth) {
                if (raw == kNone) return false;
                const uint32_t index = raw & 0xFFFFu;
                if (index >= detail::kCancelSlots) return false;

                detail::CancelSlot* s = &detail::CancelSlotTable()[index];
                
                const uint64_t st = s->state.load(std::memory_order_acquire);

                if (detail::CancelSlot::GenOf(st) != (raw >> 16)) return false;
                if (detail::CancelSlot::CancelledIn(st)) return true;
                raw = s->parent.load(std::memory_order_acquire);
            }
            return false;
        }

        bool IsWithin(CancelToken ancestor) const noexcept {
            if (!ancestor.Valid()) return false;
            uint32_t raw = raw_;
            for (int depth = 0; depth < detail::kMaxScopeDepth; ++depth) {
                if (raw == kNone) return false;
                if (raw == ancestor.raw_) return true;

                const uint32_t index = raw & 0xFFFFu;
                if (index >= detail::kCancelSlots) return false;
                detail::CancelSlot* s = &detail::CancelSlotTable()[index];
                const uint64_t st = s->state.load(std::memory_order_acquire);
                
                if (detail::CancelSlot::GenOf(st) != (raw >> 16)) return false;
                raw = s->parent.load(std::memory_order_acquire);
            }
            return false;
        }

    private:
        uint32_t raw_ = kNone;
    };

    class CancelScope {
    public:
        CancelScope() noexcept;

        explicit CancelScope(CancelToken parent) noexcept;

        ~CancelScope();

        CancelScope(const CancelScope&) = delete;
        CancelScope& operator=(const CancelScope&) = delete;

        void Cancel() noexcept;
        bool Cancelled() const noexcept;

        CancelToken Token() const noexcept { return CancelToken(raw_); }
        bool Valid() const noexcept { return raw_ != CancelToken::kNone; }

    private:
        uint32_t raw_ = CancelToken::kNone;
    };

    bool CancelVia(CancelToken token) noexcept;

} 
