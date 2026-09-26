// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Memory.h"
#include "../include/Thread.h"
#include "../include/TaskScheduler.h"
#include <mimalloc.h>
#include <cassert>
#include <atomic>

namespace {
	// Threads outside the pool allocate here; it also marks the pool's lifetime (null outside
	// Init..Join, when no per-worker heap may be created).
	mi_heap_t*    g_shared = nullptr;
	mi_arena_id_t g_arena  = nullptr;   // reserved once per process, reused by every pool

	mi_heap_t* NewHeap() noexcept {
		mi_heap_t* h = g_arena ? mi_heap_new_in_arena(g_arena) : nullptr;
		return h ? h : mi_heap_new();
	}

}

// On the owning thread, from AdoptCurrentThread / AdoptAsHelper: a theap only allocates for its
// owner. After this, Thread::Alloc is inline mimalloc over these two pointers (Thread.h).
void JLib::detail::EnsureThreadHeap(Thread* t) noexcept {
	if (!t || !g_shared || t->theap) return;
	if (!t->heap) t->heap = NewHeap();
	if (t->heap) t->theap = mi_heap_theap(t->heap);
}

void JLib::detail::MemoryInit(std::size_t reserveBytes, bool commit, bool eagerPurge) noexcept {
	if (g_shared) return;

	// Off by default: this is mimalloc's own policy and changing it is a decision about the host
	// application, not about the scheduler. Default is purge_delay 10 ms with arena_purge_mult 10,
	// so freed pages are held ~100 ms before going back.
	//
	// Measured 2026-09-26 with retention_test (laptop, 15 workers): idle RSS 38.1 -> 30.8 MB, i.e.
	// eager purging returns at idle exactly what a scheduled mi_collect used to return, without the
	// collect pass -- "collect returned 0.0%" afterwards means there was nothing left to return.
	// COMMIT is unchanged at 115.1 MB over baseline either way: that figure is address space, not
	// resident memory, and purging does not touch it. No measurable cost on the bench, but the bench
	// allocates in bursts and idles; a frame-cadence workload is where an immediate decommit would
	// actually be paid for, so profile it in the game before turning this on.
	if (eagerPurge) {
		mi_option_set(mi_option_purge_delay, 0);        // 0 = purge now, -1 = never purge
		mi_option_set(mi_option_arena_purge_mult, 1);   // arenas on the same clock, not 10x slower
		mi_option_set(mi_option_purge_decommits, 1);    // hand pages back, do not merely reset them
	}

	if (reserveBytes > 0 && !g_arena)
		mi_reserve_os_memory_ex(reserveBytes, commit, /*allow_large*/ false, /*exclusive*/ true, &g_arena);
	g_shared = NewHeap();
}

void JLib::detail::MemoryReleaseThread(Thread* t) noexcept {
	if (!t) return;
	t->DestroyLocals();        // Local<T> objects first: their destructors may still free into this heap
	if (!t->heap) return;
	mi_heap_delete(t->heap);   // frees its theaps; live blocks move to the main heap and stay valid
	t->heap  = nullptr;
	t->theap = nullptr;
}

void JLib::detail::MemoryShutdown() noexcept {
	if (!g_shared) return;
	mi_heap_delete(g_shared);
	g_shared = nullptr;
}

const void* JLib::detail::HeapOf(const void* p) noexcept { return p ? mi_heap_of(p) : nullptr; }

JLib::detail::Usage JLib::detail::MemoryUsageNow() noexcept {
	Usage u{};
	std::size_t elapsed = 0, user = 0, sys = 0, faults = 0;
	mi_process_info(&elapsed, &user, &sys,
	                &u.currentRss, &u.peakRss, &u.currentCommit, &u.peakCommit, &faults);
	return u;
}

// Only the calling thread's heap -- see the header. Scheduling this onto each worker is what a
// collector would have to do; calling it from one thread drains only that one.
void JLib::detail::MemoryCollect(bool force) noexcept { mi_collect(force); }

std::size_t JLib::Thread::NextLocalTypeId() noexcept {
	static std::atomic<std::size_t> next{ 0 };
	return next.fetch_add(1, std::memory_order_relaxed);
}

void JLib::Thread::DestroyLocals() noexcept {
	for (std::size_t i = 0; i < kMaxLocalTypes; ++i) {
		if (localObjs_[i] && localDtors_[i]) localDtors_[i](localObjs_[i]);
		localObjs_[i] = nullptr;
		localDtors_[i] = nullptr;
	}
}
