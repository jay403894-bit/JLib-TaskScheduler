// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <cstddef>

// mimalloc (third_party/mimalloc) through explicit per-thread heaps. There is no wrapper: allocate
// with Thread::Alloc / AllocAligned (inline over that thread's theap, in Thread.h) and free with
// mi_free, which needs no heap and works from any thread. Nothing here discovers which thread you
// are -- the caller holds it, normally as task->record->home.
namespace JLib {
	class Thread;

	namespace detail {
		// Creates this Thread's heap and its thread-local part. MUST run on that thread, and does,
		// from AdoptCurrentThread / AdoptAsHelper.
		void EnsureThreadHeap(Thread* t) noexcept;

		// At Init, before any pool thread exists. reserveBytes > 0 reserves an EXCLUSIVE arena for
		// the pool's heap up front (plain malloc never uses it); commit also commits it.
		void MemoryInit(std::size_t reserveBytes, bool commit, bool eagerPurge) noexcept;
		// At Join, for each pool Thread after its OS thread has exited and before it is deleted.
		// Live blocks survive (they move to mimalloc's main heap), so a structure freed after the
		// pool -- a TaskDAG edge block -- is still valid.
		void MemoryReleaseThread(Thread* t) noexcept;
		// At Join, after every pool Thread is gone: the shared heap, same rule.
		void MemoryShutdown() noexcept;
		// Diagnostic: the heap that owns block p (compare with a Thread's `heap`). Tests only.
		const void* HeapOf(const void* p) noexcept;

		// Allocator-level retention, straight from mimalloc. Tests only.
		struct Usage { std::size_t currentRss, peakRss, currentCommit, peakCommit; };
		Usage MemoryUsageNow() noexcept;

		// Return what this thread's heap can give back. Tests only.
		void MemoryCollect(bool force) noexcept;
	}
}
