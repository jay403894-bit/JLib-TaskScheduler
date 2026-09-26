// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../../include/Fiber.h"
#include "../../include/Thread.h"
#include "../../include/TaskScheduler.h"
#include <intrin.h>   

using namespace JLib;

extern "C" void FiberTrampoline();

extern "C" unsigned char JLibCtxHasAvx = 0;

namespace {

bool DetectAvx() {
	int r[4]{};
	__cpuid(r, 0);
	if (r[0] < 1) return false;

	__cpuid(r, 1);
	const bool osxsave = (r[2] & (1 << 27)) != 0;
	const bool avx     = (r[2] & (1 << 28)) != 0;
	if (!osxsave || !avx) return false;

	const unsigned long long xcr0 = _xgetbv(0);
	return (xcr0 & 0x6) == 0x6;
}

struct AvxGateInit { AvxGateInit() { JLibCtxHasAvx = DetectAvx() ? 1u : 0u; } };
const AvxGateInit g_avxGateInit;

} 


