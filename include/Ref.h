// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include "Thread.h"   // EpochGuard, EpochManager

#include <atomic>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace JLib {

    // A reference-counted object. The count lives on the object, so any thread may take or drop a
    // reference and migration does not matter. Derive from it: struct Node : JLib::RefCounted {...};
    class RefCounted {
    protected:
        RefCounted() noexcept = default;
        ~RefCounted() = default;
        RefCounted(const RefCounted&) = delete;
        RefCounted& operator=(const RefCounted&) = delete;

    private:
        template <class> friend class Ref;

        // Succeeds only while the object is still live (count above zero).
        bool TryIncrement() noexcept {
            std::uint32_t c = refs_.load(std::memory_order_acquire);
            do {
                if (c == 0) return false;
            } while (!refs_.compare_exchange_weak(c, c + 1, std::memory_order_acq_rel,
                                                  std::memory_order_acquire));
            return true;
        }
        void Increment() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }
        bool DecrementIsLast() noexcept { return refs_.fetch_sub(1, std::memory_order_acq_rel) == 1; }

        std::atomic<std::uint32_t> refs_{ 1 };
    };

    // A user-owned reference. Its lifetime is whatever scope holds it: a local, a field, another
    // thread, a suspended task. Two mechanisms, two jobs:
    //   - the epoch guard makes TAKING a reference safe (the object cannot be freed between the
    //     load and the increment), so Acquire requires a live EpochGuard and does the load itself;
    //   - the count keeps the object alive while the Ref is held, across suspensions and threads.
    // The last release RETIRES the object into the current thread's epoch bag; it is freed only
    // after an epoch advance proves no thread is mid-acquire on it.
    template <class T>
    class Ref {
    public:
        Ref() noexcept = default;
        ~Ref() { Reset(); }

        Ref(const Ref& o) noexcept : p_(o.p_) { if (p_) p_->Increment(); }   // o already holds one
        Ref(Ref&& o) noexcept : p_(std::exchange(o.p_, nullptr)) {}
        Ref& operator=(const Ref& o) noexcept { Ref tmp(o); Swap(tmp); return *this; }
        Ref& operator=(Ref&& o) noexcept { Ref tmp(std::move(o)); Swap(tmp); return *this; }

        // A new object with one reference, owned by the returned Ref. Not shared yet, so no guard.
        template <class... A>
        static Ref Make(A&&... args) {
            return Ref(new T(std::forward<A>(args)...));
        }

        // Takes a reference to the object currently published in `src`. The guard proves the load
        // is protected. Empty if src is null or the object's last reference is already gone.
        static Ref Acquire(const EpochGuard& /*proof*/, const std::atomic<T*>& src) noexcept {
            for (;;) {
                T* p = src.load(std::memory_order_acquire);
                if (!p) return Ref();
                if (p->TryIncrement()) return Ref(p);
                // p is on its way out; src has been or is about to be repointed. Look again.
                if (src.load(std::memory_order_acquire) == p) return Ref();
            }
        }

        // Gives this Ref's reference to the caller, typically to store in a shared structure
        // (which then owns that reference). The Ref becomes empty.
        [[nodiscard]] T* ReleaseToShared() noexcept { return std::exchange(p_, nullptr); }

        // Takes back a reference previously given up with ReleaseToShared, e.g. after the caller
        // unlinked the pointer from its structure with a successful CAS. Does not increment.
        static Ref AdoptFromShared(T* p) noexcept { return Ref(p); }

        void Reset() noexcept {
            if (T* p = std::exchange(p_, nullptr))
                if (p->DecrementIsLast()) Retire(p);
        }

        T* get() const noexcept { return p_; }
        T& operator*() const noexcept { return *p_; }
        T* operator->() const noexcept { return p_; }
        explicit operator bool() const noexcept { return p_ != nullptr; }
        void Swap(Ref& o) noexcept { std::swap(p_, o.p_); }

    private:
        explicit Ref(T* p) noexcept : p_(p) {
            static_assert(std::is_base_of<RefCounted, T>::value, "Ref<T> requires T : JLib::RefCounted");
        }

        static void Delete(void* q) { delete static_cast<T*>(q); }
        static void Retire(T* p) {
            EpochManager& em = EpochManager::Instance();
            em.RetirePtr(p, em.CurrentEpoch(), &Ref::Delete);
        }

        T* p_ = nullptr;
    };

}
