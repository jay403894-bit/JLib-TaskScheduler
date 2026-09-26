// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"
#include "Stats.h"
#include <memory>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <mimalloc.h>   // storage only: mi_malloc/mi_free directly, nothing is overridden

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

        // MIMALLOC, NOT THE CRT HEAP. Block is trivially constructible and destructible, so a raw
        // block of the right size and alignment IS the array -- no construction to do. This is the
        // last structure in the pool that went to the process allocator, and it is the one that
        // allocates on a HOT path: a class that runs out grows inline, on whichever thread happened
        // to exhaust it, by kGrowSlots * SLOTSZ (256 KB to 2 MB).
        struct MiBlockFree { void operator()(Block* p) const noexcept { mi_free(p); } };
        using BlockPtr = std::unique_ptr<Block[], MiBlockFree>;

        static Block* AllocBlocks(std::size_t n) noexcept {
            if (!n) return nullptr;
            return static_cast<Block*>(mi_malloc_aligned(n * sizeof(Block), alignof(Block)));
        }

        static void*& next(void* slot) { return *reinterpret_cast<void**>(slot); }

        BlockPtr    mem;
        std::size_t memSlots = 0;

        struct Extent {
            BlockPtr                 mem;
            std::size_t              slots    = 0;
            std::size_t              bumpNext = 0;
            std::atomic<Extent*>     next{ nullptr };
        };
        std::atomic<Extent*> extents{ nullptr };
        
        static constexpr std::size_t kGrowSlots = 4096;
        bool grewOnce = false;      
        
        std::size_t bumpNext = 0;
        void* sharedHead = nullptr;
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
            : mem(AllocBlocks(slots)), memSlots(slots) {
            if (slots && !mem) throw std::bad_alloc();   // AllocBlocks is noexcept: null is failure
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
            // THE PAGES ARE NOT FREED. Something allocated here can outlive the pool -- a coroutine
            // frame destroyed at exit or after a restart -- and destroying it touches its memory
            // before any free runs. Kept mapped, that is a read of live memory and a late free is a
            // no-op (see FrameFree); unmapped, it is a use-after-free. Cost: one slab per destroyed
            // pool, which only happens on restart; the OS takes it back at exit.
#if defined(JLIB_SLAB_CTL_UNMAP_ON_DESTROY)   // negative control: late_free_test must fail under ASan
            for (Extent* e = extents.exchange(nullptr, std::memory_order_acquire); e; ) {
                Extent* nx = e->next.load(std::memory_order_relaxed);
                delete e;
                e = nx;
            }
#else
            (void)mem.release();
            extents.store(nullptr, std::memory_order_relaxed);
#endif
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
            if (!mem) return;
            std::lock_guard<std::mutex> lk(mtx);
            if (slots > memSlots - bumpNext) slots = memSlots - bumpNext;
            for (std::size_t k = 0; k < slots; ++k) {
                void* slot = &mem[bumpNext + k];
                next(slot) = sharedHead;
                sharedHead = slot;
#ifdef JLIBSCHED_ALLOC_CANARY
                StampCanary(slot);
#endif
            }
            bumpNext += slots;
        }

    private:
        
        void refill(Cache& c) {
            JLIB_STAT(SlabRefills);
            if (!mem) return;
            void* batchHead = nullptr;
            void* batchTail = nullptr;
            std::size_t moved = 0;
            {
                std::lock_guard<std::mutex> lk(mtx);

                if (sharedHead) {
                    batchHead = sharedHead;
                    void* curr = batchHead;
                    while (curr && moved < BATCH) {
                        batchTail = curr;
                        curr = next(curr);
#ifdef JLIBSCHED_ALLOC_CANARY
                        
                        if (curr && !SlotInSlab(curr)) {
                            char msg[256];
                            std::snprintf(msg, sizeof msg,
                                "SlabPool: free-list link corrupted -- slot %p points to %p, which is "
                                "not a slot in the slab [%p, %p). Something wrote the first 8 bytes of "
                                "a FREED slot (the canary at [8,16) cannot see that).\n",
                                batchTail, curr, (void*)mem.get(),
                                (void*)(reinterpret_cast<std::byte*>(mem.get()) + (std::size_t)memSlots * SLOT));
                            JLIBSCHED_CANARY_REPORT(msg);
                            curr = nullptr;          
                        }
#endif
                        ++moved;
                    }
                    sharedHead = curr;              
                }

                if (moved < BATCH && bumpNext < memSlots) {
                    std::size_t take = BATCH - moved;
                    if (take > memSlots - bumpNext) take = memSlots - bumpNext;
                    for (std::size_t k = 0; k < take; ++k) {
                        void* slot = &mem[bumpNext + k];
                        
                        next(slot) = batchHead;
                        if (!batchHead) batchTail = slot;
                        batchHead = slot;
#ifdef JLIBSCHED_ALLOC_CANARY
                        
                        StampCanary(slot);
#endif
                    }
                    bumpNext += take;
                    moved += take;
                }

                if (moved == 0 && detail::SlabGrowthEnabled().load(std::memory_order_relaxed)) {
                    Extent* e = extents.load(std::memory_order_acquire);
                    while (e && e->bumpNext >= e->slots) e = e->next.load(std::memory_order_acquire);

                    if (!e) {
                        
                        if (!grewOnce) {
                            grewOnce = true;
                            std::fprintf(stderr,
                                "[JLib::Scheduler] slab class %zu exhausted (%zu slots) -- growing by "
                                "%zu. This is safe but costs an allocation; raise this class in "
                                "Config::slab at Init to avoid it.\n",
                                (std::size_t)SLOT, memSlots, kGrowSlots);
                        }
                        JLIB_STAT(SlabGrowths);
                        auto* fresh = new (std::nothrow) Extent();
                        if (fresh) {
                            fresh->mem.reset(AllocBlocks(kGrowSlots));
                            if (!fresh->mem) { delete fresh; fresh = nullptr; }
                        }
                        
                        if (!fresh) goto refill_done;
                        fresh->slots = kGrowSlots;

                        fresh->next.store(extents.load(std::memory_order_relaxed),
                                          std::memory_order_relaxed);
                        extents.store(fresh, std::memory_order_release);
                        e = fresh;
                    }

                    std::size_t take = BATCH;
                    if (take > e->slots - e->bumpNext) take = e->slots - e->bumpNext;
                    for (std::size_t k = 0; k < take; ++k) {
                        void* slot = &e->mem[e->bumpNext + k];
                        next(slot) = batchHead;
                        if (!batchHead) batchTail = slot;
                        batchHead = slot;
#ifdef JLIBSCHED_ALLOC_CANARY
                        StampCanary(slot);
#endif
                    }
                    e->bumpNext += take;
                    moved += take;
                }
            }
        refill_done:
            if (!batchHead) return;                 
            
            next(batchTail) = c.head;
            c.head = batchHead;
            c.count += moved;
        }

        void flush(Cache& c) {
            JLIB_STAT(SlabFlushes);
            if (c.count <= BATCH) return;
            std::size_t toMove = c.count - BATCH;
            void* batchHead = c.head;               
            void* batchTail = batchHead;
            for (std::size_t i = 1; i < toMove; ++i)
                batchTail = next(batchTail);
            c.head = next(batchTail);               
            c.count = BATCH;
            {
                std::lock_guard<std::mutex> lk(mtx);
                next(batchTail) = sharedHead;
                sharedHead = batchHead;
            }
        }

        // A whole thread cache coming home at thread exit.
        void giveBack(void* head, std::size_t count) {
            if (!head || count == 0) return;
            void* tail = head;
            for (std::size_t i = 1; i < count; ++i) tail = next(tail);
            std::lock_guard<std::mutex> lk(mtx);
            next(tail) = sharedHead;
            sharedHead = head;
        }
    };
}
