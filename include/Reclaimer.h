// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

namespace JLib {

	// The pool's memory reclamation. Slotted threads (main, pool workers) keep their own retire
	// bags and clear them at gates; leftovers go to the orphan store, which pool workers sweep.
	class Reclaimer {
	public:
		// True if this thread holds retired memory.
		static bool GatePending() noexcept;
		// True if the shared orphan store holds anything.
		static bool OrphansPending() noexcept;
		// Advance the epoch and reclaim this thread's bags.
		//   idle:    whatever is still protected moves to the orphan store.
		//   orphans: also sweep the orphan store (pool workers at their idle gate only).
		static void Gate(bool idle, bool orphans);
		// Reclaim this thread's bags now (no orphans).
		static void Flush();
		// Teardown, after the workers have exited: sweep until everything is freed.
		// False if something is still protected (a guard held at exit).
		static bool Drain(unsigned maxPasses = 4096);
	};
}
