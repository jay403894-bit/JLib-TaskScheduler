// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Hazard.h"
#include "../include/Stats.h"
#include "../include/TaskScheduler.h"
#include "../include/Thread.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>

namespace JLib {

    namespace {
        
        struct RetireBatch {
            std::vector<std::pair<void*, void (*)(void*)>> items;
            ~RetireBatch();
        };
        thread_local RetireBatch t_retire;

        thread_local bool t_retireDead = false;

        struct OrphanStore {
            std::mutex mtx;
            std::vector<std::pair<void*, void (*)(void*)>> items;
        };
        OrphanStore* const g_orphans = new OrphanStore();

        std::atomic<std::size_t> g_orphanCount{ 0 };

        std::atomic<std::size_t> g_orphanTotal{ 0 };

        void AdoptOrphans(std::vector<std::pair<void*, void (*)(void*)>>& from) {
            if (from.empty()) return;
            std::lock_guard<std::mutex> lk(g_orphans->mtx);
            for (auto& it : from) g_orphans->items.push_back(it);
            g_orphanTotal.fetch_add(from.size(), std::memory_order_relaxed);
            g_orphanCount.store(g_orphans->items.size(), std::memory_order_release);
            from.clear();
        }

        RetireBatch::~RetireBatch() {
            t_retireDead = true;
            AdoptOrphans(items);
        }

        // NOINLINE, and with a clobber, deliberately. glibc declares pthread_self (which is what
        // std::this_thread::get_id() is) __attribute_const__: the compiler may assume the result
        // depends on nothing in memory and CSE it ACROSS our context switch -- so a fiber that
        // migrates would keep the identity of the thread it started on and reach into that
        // thread's hazard cells. A "memory" clobber alone does not stop it (const means "reads no
        // memory"); what stops it is that this call cannot be inlined or proved pure.
        JLIB_NOINLINE std::uint64_t ThisThreadId() {
#if defined(__GNUC__) && !defined(_MSC_VER)
            __asm__ __volatile__("" ::: "memory");
#endif
            return static_cast<std::uint64_t>(
                       std::hash<std::thread::id>{}(std::this_thread::get_id())) | 1ull;
        }

        std::mutex g_tableInit;

    }

    HazardDomain& HazardDomain::Instance() {
        
        static HazardDomain* d = new HazardDomain();
        return *d;
    }

    void HazardDomain::EnsureTable() {
        if (cells.load(std::memory_order_acquire)) return;

        if (!TaskScheduler::IsInitialized()) return;

        std::lock_guard<std::mutex> lk(g_tableInit);
        if (cells.load(std::memory_order_relaxed)) return;

        // Rows belong to threads: one per worker, plus external rows for other threads. A guard
        // never spans a suspension, so no row has to follow a task.
        const std::size_t w = TaskScheduler::IsInitialized()
                            ? TaskScheduler::Instance().GetWorkerCount()
                            : 0;

        workerCount = w;
        readerCount = w + kExternalReaders;

        auto* fresh = new std::atomic<void*>[readerCount * kCellsPerReader];
        for (std::size_t i = 0; i < readerCount * kCellsPerReader; ++i)
            fresh[i].store(nullptr, std::memory_order_relaxed);

        auto* owners = new std::atomic<std::uint64_t>[kExternalReaders];
        for (std::size_t i = 0; i < kExternalReaders; ++i)
            owners[i].store(0, std::memory_order_relaxed);

        externalOwners = owners;
        cells.store(fresh, std::memory_order_release);
    }

    void HazardDomain::Init() { EnsureTable(); }

    std::size_t HazardDomain::CurrentReader() {
        EnsureTable();

        if (!cells.load(std::memory_order_acquire)) return kNoReader;

        Thread* w = TaskScheduler::IsInitialized()
                  ? TaskScheduler::SelfWorker(TaskScheduler::GetWorkers()) : nullptr;
        if (w && w->IsPoolWorker()) {   // main's helper uses an external row, like main
            const std::size_t qi = static_cast<std::size_t>(w->qIndex);
            assert(qi < workerCount && "worker index outside the hazard table");
            return qi;
        }

        const std::uint64_t me = ThisThreadId();
        for (std::size_t i = 0; i < kExternalReaders; ++i) {
            std::uint64_t cur = externalOwners[i].load(std::memory_order_acquire);
            if (cur == me) return workerCount + i;
        }
        for (std::size_t i = 0; i < kExternalReaders; ++i) {
            std::uint64_t expected = 0;
            if (externalOwners[i].compare_exchange_strong(expected, me,
                                                          std::memory_order_acq_rel,
                                                          std::memory_order_acquire))
                return workerCount + i;
        }
        
        FatalCellOverflow(0, "ran out of external reader slots -- raise kExternalReaders");
    }

    void HazardDomain::FatalCellOverflow(std::size_t k, const char* what) {
        
        std::fprintf(stderr,
            "[JLib::Scheduler] FATAL: hazard cell %zu -- %s.\n"
            "  kCellsPerReader is %zu. Raise it with -DJLIBSCHED_HAZARD_CELLS=n, or restructure\n"
            "  the traversal to hold fewer pointers live at once. This is fatal rather than\n"
            "  ignored because continuing would return a pointer that was never published --\n"
            "  protected in the caller's belief and not in fact.\n",
            k, what, kCellsPerReader);
        std::fflush(stderr);
        std::abort();
    }

    std::size_t& HazardDomain::SuspendUnsafeDepth() noexcept {
        static thread_local std::size_t d = 0;
        return d;
    }

    void HazardDomain::FatalNestedWorkerGuard() {
        std::fprintf(stderr,
            "[JLib::Scheduler] FATAL: a second HazardGuard on a worker row while one is live.\n"
            "  Every non-fiber guard on a worker shares one set of cells, so a second guard object\n"
            "  is not more capacity -- it is a collision, and the inner destructor erases the\n"
            "  outer's announcement while the outer is still using it.\n"
            "\n"
            "  USE ONE GUARD WITH SEVERAL CELLS. That is already the mechanism for a nested or\n"
            "  hand-over-hand lookup, and it is the scheme as published:\n"
            "\n"
            "      HazardGuard g;\n"
            "      Node* a = g.Protect(0, head);       // hold one\n"
            "      Node* b = g.Protect(1, a->next);    // and another, same guard\n"
            "\n"
            "  kCellsPerReader is the budget (-DJLIBSCHED_HAZARD_CELLS=n to raise it).\n"
            "  Two independent holders on one OS thread would need per-guard cell allocation,\n"
            "  which this deliberately does not have.\n");
        std::fflush(stderr);
        std::abort();
    }

    void HazardDomain::FatalSuspendWithGuard(std::size_t depth) {
        std::fprintf(stderr,
            "[JLib::Scheduler] FATAL: a task suspended while holding %zu hazard guard(s).\n"
            "  The guard resolved to the THREAD's cells, which are correct only while the task\n"
            "  stays on that thread. Resuming elsewhere leaves the announcement behind and the\n"
            "  protection silently stops -- a use-after-free with a confident comment above it.\n"
            "  A guard protects the LOOKUP; a Ref<T> (Ref.h) protects the wait. Take the Ref\n"
            "  inside an EpochGuard, drop the guard, then suspend:\n"
            "\n"
            "      JLib::Ref<Node> ref;\n"
            "      {\n"
            "          JLib::EpochGuard g;                        // no suspend inside\n"
            "          ref = JLib::Ref<Node>::Acquire(g, head);\n"
            "      }                                              // guard released HERE\n"
            "      if (ref) co_await ref->AsyncWork();            // free to migrate now\n",
            depth);
        std::abort();
    }

    std::atomic<void*>* HazardDomain::Cells(std::size_t reader) {
        EnsureTable();
        if (reader == kNoReader || reader >= readerCount) return nullptr;
        return cells.load(std::memory_order_acquire) + reader * kCellsPerReader;
    }

    void HazardDomain::Retire(void* p, void (*deleter)(void*)) {
        if (!p || !deleter) return;
        JLIB_STAT(Retired);
        
        const bool unslotted = !detail::RunsGates();
        assert(!unslotted && "hazard retire from a thread with no epoch slot -- give it a thread id");
        if (t_retireDead || unslotted) {
            std::vector<std::pair<void*, void (*)(void*)>> one{ { p, deleter } };
            AdoptOrphans(one);
            return;
        }
        EnsureTable();
        t_retire.items.emplace_back(p, deleter);

        const std::size_t threshold = 2 * readerCount * kCellsPerReader;
        // The retire path clears only this thread's list; orphans are swept by idle workers.
        if (t_retire.items.size() >= (threshold ? threshold : 64)) Scan(false);
    }

    void HazardDomain::Scan(bool orphans) {
        EnsureTable();

        if (t_retire.items.empty()
            && (!orphans || g_orphanCount.load(std::memory_order_acquire) == 0)) return;

        std::atomic<void*>* base = cells.load(std::memory_order_acquire);
        if (!base) return;

        std::atomic_thread_fence(std::memory_order_seq_cst);

        std::vector<void*> named;
        named.reserve(readerCount * kCellsPerReader / 4 + 8);
        for (std::size_t i = 0; i < readerCount * kCellsPerReader; ++i)
            if (void* v = base[i].load(std::memory_order_acquire)) named.push_back(v);

        std::sort(named.begin(), named.end());

        std::vector<std::pair<void*, void (*)(void*)>> keep;
        keep.reserve(named.size());
        for (auto& it : t_retire.items) {
            if (std::binary_search(named.begin(), named.end(), it.first)) keep.push_back(it);
            else it.second(it.first);
        }
        t_retire.items.swap(keep);

        if (orphans && g_orphanCount.load(std::memory_order_acquire) != 0) {
            std::lock_guard<std::mutex> lk(g_orphans->mtx);
            std::vector<std::pair<void*, void (*)(void*)>> stillNamed;
            stillNamed.reserve(g_orphans->items.size());
            for (auto& it : g_orphans->items) {
                if (std::binary_search(named.begin(), named.end(), it.first)) stillNamed.push_back(it);
                else it.second(it.first);
            }
            g_orphans->items.swap(stillNamed);
            g_orphanCount.store(g_orphans->items.size(), std::memory_order_release);
        }
    }

    std::size_t HazardDomain::PendingRetired() const { return t_retire.items.size(); }

    void HazardDomain::HandOffPending() {
        if (!t_retireDead) AdoptOrphans(t_retire.items);
    }

    void HazardDomain::ReleaseCurrentReader() {
        std::atomic<void*>* base = cells.load(std::memory_order_acquire);
        if (!base || !externalOwners) return;
        const std::uint64_t me = ThisThreadId();
        for (std::size_t i = 0; i < kExternalReaders; ++i) {
            if (externalOwners[i].load(std::memory_order_acquire) != me) continue;
            std::atomic<void*>* row = base + (workerCount + i) * kCellsPerReader;
            for (std::size_t k = 0; k < kCellsPerReader; ++k) row[k].store(nullptr, std::memory_order_release);
            externalOwners[i].store(0, std::memory_order_release);
            return;
        }
    }

    std::size_t HazardDomain::OrphanedRetired() const {
        return g_orphanCount.load(std::memory_order_acquire);
    }

    std::size_t HazardDomain::OrphanedTotal() const {
        return g_orphanTotal.load(std::memory_order_relaxed);
    }

    HazardGuard::HazardGuard() {
        HazardDomain& d = HazardDomain::Instance();

        reader = d.CurrentReader();
        cells  = d.Cells(reader);

        countsForSuspend = (reader != HazardDomain::kNoReader);

        if (countsForSuspend && HazardDomain::SuspendUnsafeDepth() != 0)
            HazardDomain::FatalNestedWorkerGuard();

        if (countsForSuspend) ++HazardDomain::SuspendUnsafeDepth();
    }

    HazardGuard::~HazardGuard() {
        
        if (countsForSuspend) {
            countsForSuspend = false;
            --HazardDomain::SuspendUnsafeDepth();
        }

        if (!cells) return;
        for (std::size_t k = 0; k < HazardDomain::kCellsPerReader; ++k)
            cells[k].store(nullptr, std::memory_order_release);

    }

    void HazardGuard::Set(std::size_t k, void* p) {
        
        assert(k < HazardDomain::kCellsPerReader && "hazard cell index out of range");
        if (!cells || k >= HazardDomain::kCellsPerReader) return;
        cells[k].store(p, std::memory_order_release);
    }

    void HazardGuard::Clear(std::size_t k) { Set(k, nullptr); }

    void HazardGuard::Swap(std::size_t a, std::size_t b) {
        assert(a < HazardDomain::kCellsPerReader && b < HazardDomain::kCellsPerReader);
        if (!cells) return;
        void* va = cells[a].load(std::memory_order_relaxed);
        void* vb = cells[b].load(std::memory_order_relaxed);
        cells[a].store(vb, std::memory_order_release);
        cells[b].store(va, std::memory_order_release);
    }

} 
