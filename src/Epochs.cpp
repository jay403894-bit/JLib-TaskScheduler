// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Epochs.h"
#include <atomic>
#include <vector>

namespace JLib {
	thread_local size_t thread_id = 0;

	size_t CurrentThreadId() noexcept { return thread_id; }

	namespace detail {
		namespace {
			thread_local EpochRetireBatch t_epochBag;
			thread_local bool             t_epochBagDead = false;
			thread_local size_t           t_bagLimit     = kPublishAt;
			thread_local bool             t_runsGates    = false;
		}

		EpochRetireBatch& EpochBag() noexcept { return t_epochBag; }
		bool              EpochBagDead() noexcept { return t_epochBagDead; }
		size_t&           BagLimit() noexcept { return t_bagLimit; }
		bool&             RunsGates() noexcept { return t_runsGates; }

		EpochRetireBatch::~EpochRetireBatch() {
			t_epochBagDead = true;
			OrphanEpochEntries(items);
		}
	}
}
