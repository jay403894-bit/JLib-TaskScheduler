// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../../../include/Fiber.h"
#include "../../../include/Thread.h"
#include "../../../include/TaskScheduler.h"

#if !defined(__x86_64__) && !defined(_M_X64)
#error "x86_64/FiberInit.cpp built for a non-x86-64 target: the build picked the wrong arch directory."
#endif

using namespace JLib;

extern "C" void FiberTrampoline();


