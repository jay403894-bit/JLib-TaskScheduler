// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../../../include/Fiber.h"
#include "../../../include/Thread.h"
#include "../../../include/TaskScheduler.h"

#if !defined(__aarch64__) && !defined(_M_ARM64)
#error "aarch64/FiberInit.cpp built for a non-AArch64 target: the build picked the wrong arch directory."
#endif

using namespace JLib;

extern "C" void FiberTrampoline();

