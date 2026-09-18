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

void Fiber::Init(void(*entryPoint)())
{
	
	uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
	uintptr_t* sp = (uintptr_t*)top;

	*(--sp) = (uintptr_t)&FiberTrampoline;
	*(--sp) = 0;                     

	*(--sp) = 0;                     
	*(--sp) = 0;                     
	*(--sp) = 0;                     
	*(--sp) = 0;                     
	*(--sp) = 0;                     
	*(--sp) = 0;                     
	*(--sp) = 0;                     
	*(--sp) = 0;                     
	*(--sp) = 0;                     
	*(--sp) = (uintptr_t)entryPoint; 

	*(--sp) = 0;

	*(--sp) = 0;

	for (int i = 0; i < 8; ++i)
		*(--sp) = 0;

	ctx.rsp = (void*)sp;
}
