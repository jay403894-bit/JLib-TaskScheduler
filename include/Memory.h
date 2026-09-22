// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <cstddef>

// The scheduler's general allocator: mimalloc (third_party/mimalloc), through explicit heaps, never
// through malloc/new -- overriding those is the application's decision, not the library's.
//
// Each pool Thread owns a heap and allocates through that heap's thread-local part (a mimalloc
// "theap": its own pages, no lookup, no lock). Both are reached through Thread::GetCurrent() on
// EVERY call and never cached -- a fiber holding one across a suspension could resume on another
// thread, and a theap may only allocate on the thread that owns it. Threads outside the pool use
// one shared heap. Frees are safe from any thread: a block allocated on one worker and freed on
// another goes back to its owner without a lock.
namespace JLib {
	class Thread;

	void* Alloc(std::size_t bytes) noexcept;                       // null on failure
	void* AllocAligned(std::size_t bytes, std::size_t alignment) noexcept;
	void  Free(void* p) noexcept;                                  // any thread; null is a no-op

	namespace detail {
		// At Init, before any pool thread exists. reserveBytes > 0 reserves an EXCLUSIVE arena for
		// the pool's heap up front (plain malloc never uses it); commit also commits it.
		void MemoryInit(std::size_t reserveBytes, bool commit) noexcept;
		// At Join, for each pool Thread after its OS thread has exited and before it is deleted.
		// Live blocks survive (they move to mimalloc's main heap), so a structure freed after the
		// pool -- a TaskDAG edge block -- is still valid.
		void MemoryReleaseThread(Thread* t) noexcept;
		// At Join, after every pool Thread is gone: the shared heap, same rule.
		void MemoryShutdown() noexcept;
		// Diagnostic: the heap that owns block p (compare with a Thread's `heap`). Tests only.
		const void* HeapOf(const void* p) noexcept;
	}
}
