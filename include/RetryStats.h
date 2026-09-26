// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace JLib {

	enum class RetrySite : unsigned {
		Bands = 0,        
		WaitGroupPush,    
		Count
	};

	inline const char* const kRetrySiteNames[] = { "bands", "wg-push" };
	static_assert(sizeof(kRetrySiteNames) / sizeof(kRetrySiteNames[0]) == (size_t)RetrySite::Count,
		"kRetrySiteNames must have one entry per RetrySite -- an unnamed site prints as garbage, "
		"which is worse than not measuring it.");

#ifdef JLIBSCHED_RETRY_STATS

	inline constexpr size_t kRetrySlots = 65;
	inline constexpr size_t kRetrySpillSlot = 64;

	struct alignas(platform::kCacheLine) RetryCell {
		std::atomic<unsigned long long> calls{ 0 };
		std::atomic<unsigned long long> retries{ 0 };
		std::atomic<unsigned>           maxRetries{ 0 };
		std::atomic<unsigned long long> ge8{ 0 }, ge64{ 0 }, ge512{ 0 }, ge4096{ 0 };
	};

	inline RetryCell g_retry[(size_t)RetrySite::Count][kRetrySlots];

	size_t RetrySlotForCurrentThread() noexcept;

	inline void RecordRetries(RetrySite s, unsigned n) noexcept {
		RetryCell& c = g_retry[(size_t)s][RetrySlotForCurrentThread()];
		c.calls.fetch_add(1, std::memory_order_relaxed);
		if (!n) return;
		c.retries.fetch_add(n, std::memory_order_relaxed);
		
		if (n > c.maxRetries.load(std::memory_order_relaxed))
			c.maxRetries.store(n, std::memory_order_relaxed);
		if (n >= 8)    c.ge8.fetch_add(1, std::memory_order_relaxed);
		if (n >= 64)   c.ge64.fetch_add(1, std::memory_order_relaxed);
		if (n >= 512)  c.ge512.fetch_add(1, std::memory_order_relaxed);
		if (n >= 4096) c.ge4096.fetch_add(1, std::memory_order_relaxed);
	}

	struct RetryProbe {
		unsigned  n = 0;
		RetrySite site;
		explicit RetryProbe(RetrySite s) noexcept : site(s) {}
		RetryProbe(const RetryProbe&) = delete;
		RetryProbe& operator=(const RetryProbe&) = delete;
		inline void Miss() noexcept { ++n; }
		~RetryProbe() { RecordRetries(site, n); }
	};

	inline constexpr bool kRetryStatsEnabled = true;
	void RetryStatsReset() noexcept;
	
	void RetryStatsReport();

#else

	struct RetryProbe {
		explicit RetryProbe(RetrySite) noexcept {}
		RetryProbe(const RetryProbe&) = delete;
		RetryProbe& operator=(const RetryProbe&) = delete;
		inline void Miss() noexcept {}
	};
	inline constexpr bool kRetryStatsEnabled = false;
	inline void RetryStatsReset() noexcept {}
	inline void RetryStatsReport() {}

#endif

}
