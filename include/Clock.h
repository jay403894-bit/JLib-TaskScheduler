// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <cstdint>

// Two reads of one timeline, platform switch inside. No clock thread.
// Coarse is a lagged sample of exact: same epoch, same units, coarse <= exact.
// Wall-clock time is not here -- it jumps on NTP and DST, so it is never used for scheduling.
namespace JLib {

    // QueryPerformanceCounter / CLOCK_MONOTONIC. ~11 ns. For intervals and deadlines.
    int64_t MonotonicNs() noexcept;

    // QueryInterruptTime / CLOCK_MONOTONIC_COARSE. ~1 ns to read, ~1 ms granularity.
    // Not for firing a deadline.
    int64_t CoarseMs() noexcept;
}
