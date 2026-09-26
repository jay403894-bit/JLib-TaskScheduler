// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Clock.h"

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
// QueryInterruptTime lives in the mincore umbrella (Win8+), not kernel32. The directive rides in
// this .obj so consumers of the static lib link it without their own CMake knowing.
#  pragma comment(lib, "mincore.lib")
#else
#  include <time.h>
#endif

namespace JLib {

#if defined(_WIN32)

// The frequency is fixed for the life of the process, so it is read once. On every machine this
// targets QPC is a user-mode read of a shared page or a hardware counter -- no syscall.
static int64_t QpcFreq() noexcept {
	static const int64_t f = [] {
		LARGE_INTEGER v;
		QueryPerformanceFrequency(&v);
		return (int64_t)v.QuadPart;
	}();
	return f;
}

int64_t MonotonicNs() noexcept {
	LARGE_INTEGER v;
	QueryPerformanceCounter(&v);
	const int64_t f = QpcFreq();
	// Split to keep the nanosecond scaling exact without overflowing at process uptimes measured
	// in days: whole seconds first, then the remainder.
	const int64_t sec = v.QuadPart / f;
	const int64_t rem = v.QuadPart % f;
	return sec * 1000000000LL + (rem * 1000000000LL) / f;
}

int64_t CoarseMs() noexcept {
	// 100 ns units, read straight out of KUSER_SHARED_DATA -- no syscall, ~1 ns, 1 ms granularity
	// even without timeBeginPeriod. This is the kernel's published coarse clock.
	ULONGLONG t = 0;
	QueryInterruptTime(&t);
	return (int64_t)(t / 10000ULL);
}

#else

int64_t MonotonicNs() noexcept {
	timespec ts{};
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

int64_t CoarseMs() noexcept {
#if defined(CLOCK_MONOTONIC_COARSE)
	timespec ts{};
	clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);   // vDSO page read, no syscall
	return (int64_t)ts.tv_sec * 1000LL + (int64_t)ts.tv_nsec / 1000000LL;
#else
	return MonotonicNs() / 1000000LL;
#endif
}

#endif

}   // namespace JLib
