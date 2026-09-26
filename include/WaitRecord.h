// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstdint>
#include "MPSCQueue.h"
#include "Task.h"

namespace JLib {
	struct Task;

	// What the observer watches for one task. One per fiber, embedded, never allocated: a fiber is
	// parked at most once at a time. The MPSC hook makes registration legal from any thread.
	struct WaitRecord {
		MPSCNode                     mpscHook;               // MPSCTraits offsets to it

		// The handle, not the task: Kick validates the generation and never dereferences.
		TaskHandle                   handle{};

		// THE FLAG MUST OUTLIVE THE WATCH: the observer polls this address from another thread.
		// Unregister before destroying it, or hold it with a Ref<T>. A flag on the waiting task's
		// own stack is safe -- the fiber is parked, so its frame cannot unwind.
		const std::atomic<uint64_t>* flag       = nullptr;   // null = deadline-only
		uint64_t                     mask       = 0;
		uint64_t                     deadlineMs = 0;         // absolute, 0 = none

		// Which registration this is. Bumped by Register and by Cancel, so a cancel is one relaxed
		// store and a re-registration of the same fiber retires the previous watch. The observer
		// stores the seq it saw and drops entries whose record has moved on.
		std::atomic<uint64_t>        seq{ 0 };

		// A wake that arrived before the park published its handle. The waker leaves it here and
		// re-reads the handle; the park stores the handle and then consumes this. Seq_cst on those
		// four operations means at least one side sees the other. Delivering twice is harmless: the
		// PARKED CAS admits one winner. A bit left by a wake for a task that never parked is
		// consumed by its next park, which is a spurious wake -- inside the contract.
		std::atomic<uint32_t>        pendingWake{ 0 };
	};

	// The observer is the only consumer: MPSCQueue's tail_ is consumer-owned state.
	using ObserverQueue = MPSCQueue<WaitRecord>;
}
