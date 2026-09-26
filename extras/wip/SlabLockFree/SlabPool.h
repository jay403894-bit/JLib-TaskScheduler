// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"
#include "Stats.h"
#include "SlotStack.h"
#include <memory>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <cstdio>
#include <cstdlib>

#if defined(_DEBUG) || defined(JLIB_DEVELOPMENT)
    #define JLIBSCHED_ALLOC_CANARY 1
#endif

#ifdef JLIBSCHED_ALLOC_CANARY
  #if JLIB_PLATFORM_WINDOWS
    #include <Windows.h>
    #define JLIBSCHED_CANARY_REPORT(msg) do { OutputDebugStringA(msg); std::fprintf(stderr, "%s", msg); __debugbreak(); } while (0)
  #else
    #include <csignal>
    #define JLIBSCHED_CANARY_REPORT(msg) do { std::fprintf(stderr, "%s", msg); std::fflush(stderr); std::raise(SIGTRAP); } while (0)
  #endif
#endif

namespace JLib {
    namespace detail {
        
        struct alignas(platform::kCacheLine) LiveCounter {
            std::atomic<long long> v{ 0 };
            std::atomic<long long> peak{ 0 };
        };

        inline std::atomic<bool>& SlabGrowthEnabled() {
            static std::atomic<bool> on{ true };
            return on;
        }
    }

    template <std::size_t SLOTSZ>
    class SlabPool {
    public:
        static constexpr std::size_t SLOT  = SLOTSZ;
        static constexpr std::size_t BATCH = 32;    

    private:
        struct alignas(16) Block { std::byte b[SLOTSZ]; };

        static void*& next(void* slot) { return *reinterpret_cast<void**>(slot); }

        std::unique_ptr<Block[]> mem;
        std::size_t memSlots = 0;

        struct Extent {
            std::unique_ptr<Block[]> mem;
            std::size_t              slots = 0;
            std::atomic<std::size_t> bumpNext{ 0 };
            std::atomic<Extent*>     next{ nullptr };
        };

        std::atomic<std::size_t> bumpNext{ 0 };
        std::atomic<bool>        grewOnce{ false };
        static std::size_t Claim(std::atomic<std::size_t>& cur, std::size_t cap, std::size_t want) {
            for (;;) {
                std::size_t i = cur.load(std::memory_order_relaxed);
                if (i >= cap) return cap;                 // none left
                std::size_t n = want;
                if (n > cap - i) n = cap - i;
                if (cur.compare_exchange_weak(i, i + n,
                    std::memory_order_acq_rel, std::memory_order_relaxed))
                    return i;                             // slots [i, i+n)
            }
        }
        static constexpr std::size_t kGrowSlots = 4096;

        std::atomic<Extent*> extents{ nullptr };
        SlotStack shared;
        std::mutex mtx;

        using LiveCounter = detail::LiveCounter;
        static constexpr std::size_t kLiveSlots = 128;
        inline static LiveCounter s_live[kLiveSlots];
        inline static std::atomic<std::size_t> s_liveNext{ 0 };

        struct LiveRef { LiveCounter* c; bool exclusive; };
        JLIB_NOINLINE static const LiveRef& liveSlot() {   // TLS: see JLIB_NOINLINE
            static thread_local LiveRef r = [] {
                const std::size_t n = s_liveNext.fetch_add(1, std::memory_order_relaxed);
                
                return LiveRef{ &s_live[n % kLiveSlots], n < kLiveSlots };
            }();
            return r;
        }
        static void liveAdd(long long d) {
            const LiveRef& r = liveSlot();
            long long now;
            if (r.exclusive) {
                
                now = r.c->v.load(std::memory_order_relaxed) + d;
                r.c->v.store(now, std::memory_order_relaxed);
            }
            else {
                now = r.c->v.fetch_add(d, std::memory_order_relaxed) + d;
            }
            
            if (d > 0 && now > r.c->peak.load(std::memory_order_relaxed))
                r.c->peak.store(now, std::memory_order_relaxed);
        }


        // Which pool of this size class is alive, so an exiting thread can tell whether it has
        // anyone to give its cache back to. The mutex makes "check it is alive, then give back"
        // one step against the pool's destructor; it is taken at thread exit and at pool
        // construction/destruction, never on Alloc/Free.
        struct Registry { std::mutex m; SlabPool* current = nullptr; };
        static Registry& Live() { static Registry r; return r; }

        // Bumped once per pool. A thread cache stamped with an older number holds slots of a pool
        // that no longer exists.
        static std::atomic<std::uint32_t>& Generation() {
            static std::atomic<std::uint32_t> g{ 0 };
            return g;
        }
        std::uint32_t gen_ = 0;

        // The per-thread magazine. Only its own thread ever touches it.
        struct Cache {
            void*         head  = nullptr;
            std::size_t   count = 0;
            std::uint32_t gen   = 0;

            // Thread exit: hand the slots back to the pool they came from, on this thread, if that
            // pool is still alive. If it is gone, its memory went with it -- drop the pointers.
            ~Cache() {
                if (!head) return;
                Registry& reg = Live();
                std::lock_guard<std::mutex> lk(reg.m);
#if !defined(JLIB_SLAB_CTL_NO_GIVEBACK)   // negative control: slab_thread_exit_test must fail
                if (reg.current && reg.current->gen_ == gen) reg.current->giveBack(head, count);
#endif
                head  = nullptr;
                count = 0;
            }
        };
        JLIB_NOINLINE static Cache& local() {   // TLS: see JLIB_NOINLINE
            static thread_local Cache c;
            return c;
        }

        // This thread's cache, emptied first if it belongs to an earlier pool. Never walks the
        // stale list: those slots were in a slab that has been freed.
        Cache& cache() {
            Cache& c = local();
            if (c.gen != gen_) {
                c.head  = nullptr;
                c.count = 0;
                c.gen   = gen_;
            }
            return c;
        }

        static std::atomic<int>& LiveInstances() {
            static std::atomic<int> n{ 0 };
            return n;
        }

#ifdef JLIBSCHED_ALLOC_CANARY
        
        static constexpr std::uint64_t kFreeCanary = 0xFEEDFACECAFEBEEFULL;
        static void StampCanary(void* slot) {
            *reinterpret_cast<std::uint64_t*>(reinterpret_cast<std::byte*>(slot) + 8) = kFreeCanary;
        }
        static void CheckCanary(void* slot) {
            const std::uint64_t v = *reinterpret_cast<std::uint64_t*>(reinterpret_cast<std::byte*>(slot) + 8);
            if (v != kFreeCanary) {
                JLIBSCHED_CANARY_REPORT("SlabPool: corrupted freed slot detected "
                    "(use-after-free or double-free) -- breaking at the Alloc() that noticed.\n");
            }
        }
        static_assert(SLOTSZ >= 16, "the canary needs bytes [8,16) of a free slot");
#endif
        static_assert(SLOTSZ >= sizeof(void*), "a free slot must hold the intrusive next pointer");

    public:
        
        explicit SlabPool(std::size_t slots, bool lazy = false)
            : mem(slots ? new Block[slots] : nullptr), memSlots(slots) {
            if (LiveInstances().fetch_add(1, std::memory_order_relaxed) != 0) {
                std::fprintf(stderr,
                    "[JLib::Scheduler] FATAL: a second SlabPool<%zu> was constructed.\n"
                    "  A pool's per-thread free-list cache is shared by all instances of that size\n"
                    "  class (it is a static thread_local in a static member function), so two slabs\n"
                    "  feed one free list and each hands out the other's slots. This corrupts the\n"
                    "  heap immediately and the crash appears somewhere unrelated.\n"
                    "  Use one pool per size class, or make local() per-instance first.\n",
                    SLOTSZ);
                std::fflush(stderr);
                std::abort();
            }
            // The only pool of this size class, so nobody else is counting: start the live
            // counters from zero instead of from whatever the previous pool left.
            for (std::size_t i = 0; i < kLiveSlots; ++i) {
                s_live[i].v.store(0, std::memory_order_relaxed);
                s_live[i].peak.store(0, std::memory_order_relaxed);
            }
            gen_ = Generation().fetch_add(1, std::memory_order_relaxed) + 1;
            {
                std::lock_guard<std::mutex> lk(Live().m);
                Live().current = this;
            }
            if (!lazy) Prefault(slots);
        }

        ~SlabPool() {
            {
                // After this, an exiting thread drops its cache instead of giving it back here.
                std::lock_guard<std::mutex> lk(Live().m);
                if (Live().current == this) Live().current = nullptr;
            }
            for (Extent* e = extents.exchange(nullptr, std::memory_order_acquire); e; ) {
                Extent* nx = e->next.load(std::memory_order_relaxed);
                delete e;
                e = nx;
            }
            LiveInstances().fetch_sub(1, std::memory_order_relaxed);
        }

        SlabPool(const SlabPool&) = delete;
        SlabPool& operator=(const SlabPool&) = delete;

        bool SlotInSlab(const void* p) const {
            const std::byte* q = reinterpret_cast<const std::byte*>(p);
            if (mem) {
                const std::byte* base = reinterpret_cast<const std::byte*>(mem.get());
                if (q >= base && q < base + (std::size_t)memSlots * SLOT)
                    return ((std::size_t)(q - base) % SLOT) == 0;  
            }
            
            for (Extent* e = extents.load(std::memory_order_acquire); e;
                 e = e->next.load(std::memory_order_acquire)) {
                const std::byte* base = reinterpret_cast<const std::byte*>(e->mem.get());
                if (q >= base && q < base + (std::size_t)e->slots * SLOT)
                    return ((std::size_t)(q - base) % SLOT) == 0;
            }
            return false;
        }

        void* Alloc() {
            Cache& c = cache();
            JLIB_STAT(SlabAllocs);
            if (!c.head) refill(c);
            if (!c.head) return nullptr;       
            void* slot = c.head;
#ifdef JLIBSCHED_ALLOC_CANARY
            CheckCanary(slot);
#endif
            c.head = next(slot);
            c.count--;
            
            liveAdd(+1);
            return slot;
        }

        void Free(void* slot) {                
            JLIB_STAT(SlabFrees);
            Cache& c = cache();
#ifdef JLIBSCHED_ALLOC_CANARY
            
            if (!SlotInSlab(slot)) {
                char msg[192];
                std::snprintf(msg, sizeof msg,
                    "SlabPool: Free(%p) is not a slot in this slab -- refusing to link it into the "
                    "free list. Freeing a non-slab pointer, or freeing twice through a mangled one.\n",
                    slot);
                JLIBSCHED_CANARY_REPORT(msg);
                return;                        
            }
#endif
            next(slot) = c.head;
#ifdef JLIBSCHED_ALLOC_CANARY
            StampCanary(slot);
#endif
            c.head = slot;
            c.count++;
            liveAdd(-1);
            if (c.count > 2 * BATCH) flush(c);
        }

        long long LiveCount() const {
            
            long long n = 0;
            for (std::size_t i = 0; i < kLiveSlots; ++i)
                n += s_live[i].v.load(std::memory_order_relaxed);
            return n;
        }
        
        std::size_t HighWaterSlots() const {
            std::lock_guard<std::mutex> lk(const_cast<std::mutex&>(mtx));
            std::size_t n = bumpNext;
            for (Extent* e = extents.load(std::memory_order_acquire); e;
                 e = e->next.load(std::memory_order_acquire)) n += e->bumpNext;
            return n;
        }
        std::size_t HighWaterBytes() const { return HighWaterSlots() * SLOT; }

        static long long PeakLive() {
            long long p = 0;
            for (std::size_t i = 0; i < kLiveSlots; ++i)
                p += s_live[i].peak.load(std::memory_order_relaxed);
            return p;
        }

        std::size_t ExtentCount() const {
            std::size_t n = 0;
            for (Extent* e = extents.load(std::memory_order_acquire); e;
                 e = e->next.load(std::memory_order_acquire)) ++n;
            return n;
        }

        std::size_t Capacity() const {
            std::size_t n = memSlots;
            for (Extent* e = extents.load(std::memory_order_acquire); e;
                 e = e->next.load(std::memory_order_acquire)) n += e->slots;
            return n;
        }

        void Prefault(std::size_t slots) {
            if (!mem || !slots) return;
            // Claimed like any refill: refill takes from bumpNext without the mutex, so a range
            // counted here under a lock could be handed out twice.
            const std::size_t i = Claim(bumpNext, memSlots, slots);
            if (i >= memSlots) return;
            if (slots > memSlots - i) slots = memSlots - i;
            void* h = nullptr;
            void* t = nullptr;
            for (std::size_t k = 0; k < slots; ++k) {
                void* slot = &mem[i + k];
                next(slot) = h;
                if (!h) t = slot;
                h = slot;
#ifdef JLIBSCHED_ALLOC_CANARY
                StampCanary(slot);
#endif
            }
            shared.pushBatch(h, t);
        }

    private:
        void refill(Cache& c) {
            if (!mem && !extents.load(std::memory_order_acquire)) return;

            void* batchHead = nullptr;
            void* batchTail = nullptr;
            std::size_t moved = 0;

            moved = shared.popBatch(batchHead, batchTail, BATCH);

            auto append = [&](void* slot) {
                next(slot) = batchHead;
                if (!batchHead) batchTail = slot;
                batchHead = slot;
#ifdef JLIBSCHED_ALLOC_CANARY
                StampCanary(slot);
#endif
                ++moved;
                };

            auto takeFrom = [&](Block* base, std::atomic<std::size_t>& cur, std::size_t cap) {
                const std::size_t want = BATCH - moved;
                if (!want || !base) return;
                const std::size_t i = Claim(cur, cap, want);
                if (i >= cap) return;
                std::size_t n = want;
                if (n > cap - i) n = cap - i;
                for (std::size_t k = 0; k < n; ++k)
                    append(&base[i + k]);
                };

            if (moved < BATCH)
                takeFrom(mem.get(), bumpNext, memSlots);

            if (moved < BATCH) {
                for (Extent* e = extents.load(std::memory_order_acquire); e && moved < BATCH;
                    e = e->next.load(std::memory_order_acquire))
                    takeFrom(e->mem.get(), e->bumpNext, e->slots);
            }

            if (moved == 0 && detail::SlabGrowthEnabled().load(std::memory_order_relaxed)) {
                Extent* fresh = new (std::nothrow) Extent();
                if (fresh) {
                    fresh->mem.reset(new (std::nothrow) Block[kGrowSlots]);
                    if (!fresh->mem) { delete fresh; fresh = nullptr; }
                    else fresh->slots = kGrowSlots;
                }
                if (fresh) {
                    Extent* head = extents.load(std::memory_order_relaxed);
                    do { fresh->next.store(head, std::memory_order_relaxed); } while (!extents.compare_exchange_weak(head, fresh,
                        std::memory_order_release, std::memory_order_relaxed));
                    takeFrom(fresh->mem.get(), fresh->bumpNext, fresh->slots);
                }
            }

            if (!batchHead) return;
            next(batchTail) = c.head;
            c.head = batchHead;
            c.count += moved;
        }
        
        void flush(Cache& c) {
            if (c.count <= BATCH) return;
            std::size_t toMove = c.count - BATCH;
            void* batchHead = c.head;
            void* batchTail = batchHead;
            for (std::size_t i = 1; i < toMove; ++i)
                batchTail = next(batchTail);
            c.head = next(batchTail);
            c.count = BATCH;
            shared.pushBatch(batchHead, batchTail);
        }

        void giveBack(void* head, std::size_t count) {
            if (!head || count == 0) return;
            void* tail = head;
            for (std::size_t i = 1; i < count; ++i) tail = next(tail);
            shared.pushBatch(head, tail);
        }


    };
}
