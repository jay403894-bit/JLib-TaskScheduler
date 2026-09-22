// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Memory.h"
#include "../include/Thread.h"
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

	// A Thread's own heap, created the first time that thread allocates, and its thread-local
	// part. Must be called ON that thread: a theap only allocates for its owner.
	inline mi_theap_t* TheapOf(JLib::Thread* self) noexcept {
		if (!self || !g_shared) return nullptr;
		if (!self->theap) {
			if (!self->heap) self->heap = NewHeap();
			if (self->heap) self->theap = mi_heap_theap(self->heap);   // on the owning thread
		}
		return self->theap;
	}

	// The plain entry points: the calling thread through GetCurrent(), read on EVERY call (see
	// Memory.h). Task code that holds its task should prefer task->record->home->Alloc().
	inline mi_theap_t* CurrentTheap() noexcept { return TheapOf(JLib::Thread::GetCurrent()); }
}

void* JLib::Thread::Alloc(std::size_t bytes) noexcept {
	assert(this == GetCurrent() && "Thread::Alloc on a Thread that is not running this code -- "
	                               "a stale home kept across a suspension?");
	if (mi_theap_t* th = TheapOf(this)) return mi_theap_malloc(th, bytes);
	return g_shared ? mi_heap_malloc(g_shared, bytes) : mi_malloc(bytes);
}

void* JLib::Thread::AllocAligned(std::size_t bytes, std::size_t alignment) noexcept {
	assert(this == GetCurrent() && "Thread::AllocAligned on a Thread that is not running this code -- "
	                               "a stale home kept across a suspension?");
	if (mi_theap_t* th = TheapOf(this)) return mi_theap_malloc_aligned(th, bytes, alignment);
	return g_shared ? mi_heap_malloc_aligned(g_shared, bytes, alignment) : mi_malloc_aligned(bytes, alignment);
}

void* JLib::Alloc(std::size_t bytes) noexcept {
	if (mi_theap_t* th = CurrentTheap()) return mi_theap_malloc(th, bytes);
	return g_shared ? mi_heap_malloc(g_shared, bytes)   // a thread outside the pool
	                : mi_malloc(bytes);                 // no pool at all
}

void* JLib::AllocAligned(std::size_t bytes, std::size_t alignment) noexcept {
	if (mi_theap_t* th = CurrentTheap()) return mi_theap_malloc_aligned(th, bytes, alignment);
	return g_shared ? mi_heap_malloc_aligned(g_shared, bytes, alignment)
	                : mi_malloc_aligned(bytes, alignment);
}

void JLib::Free(void* p) noexcept { mi_free(p); }

void JLib::detail::MemoryInit(std::size_t reserveBytes, bool commit) noexcept {
	if (g_shared) return;
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
