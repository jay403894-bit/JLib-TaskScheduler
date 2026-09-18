// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "Reclaimer.h"
#include "../include/Stats.h"
#include "Epochs.h"
#include "Hazard.h"

namespace JLib {

	bool Reclaimer::GatePending() noexcept {
		if (!detail::EpochBagDead() && !detail::EpochBag().items.empty()) return true;
		return HazardDomain::Instance().PendingRetired() != 0;
	}

	bool Reclaimer::OrphansPending() noexcept {
		return detail::g_epochOrphanCount.load(std::memory_order_acquire) != 0
		    || HazardDomain::Instance().OrphanedRetired() != 0;
	}

	void Reclaimer::Gate(bool idle, bool orphans) {
		static thread_local bool inGate = false;
		if (inGate || detail::EpochBagDead()) return;   // a deleter retired more; the outer pass owns it
		inGate = true;
		JLIB_STAT(ReclaimPasses);
		JLIB_STAT_ONLY(const std::uint64_t t0 = JLIB_STAT_TICKS();)

		EpochManager& em = EpochManager::Instance();
		em.AdvanceEpoch();
		em.TryReclaim(orphans);
		HazardDomain& hz = HazardDomain::Instance();
		hz.Scan(orphans);
		em.NoteSwept();

		auto& bag = detail::EpochBag();
		if (idle) {
			detail::OrphanEpochEntries(bag.items);
			hz.HandOffPending();
		}
		detail::BagLimit() = bag.items.size() + detail::kPublishAt;
		JLIB_STAT_HIST(ReclaimPass, JLIB_STAT_TICKS() - t0);
		inGate = false;
	}

	void Reclaimer::Flush() { Gate(false, false); }

	bool Reclaimer::Drain(unsigned maxPasses) {
		for (unsigned i = 0; i < maxPasses; ++i) {
			Gate(false, true);
			if (!GatePending() && !OrphansPending()) return true;
		}
		return false;
	}
}
