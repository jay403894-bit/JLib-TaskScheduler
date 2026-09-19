// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Fiber.h"
#include "../include/Thread.h"
#include "../include/TaskScheduler.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
using namespace JLib;

void JLib::NativeSuspendViolation(const char* where) noexcept {
	std::fprintf(stderr,
		"[JLib::Scheduler] FATAL: a native task tried to suspend (%s).\n"
		"  Native tasks (every lambda task, and CreateNativeTask) run on the worker's stack and\n"
		"  must not wait or yield. Block the OS thread inside TaskScheduler::BlockInPlace, or use\n"
		"  CreateTask(void(*)(void*), void* ctx, ...) for work that suspends.\n", where);
	std::fflush(stderr);
	std::abort();
}

void JLib::CoroutineBlockingWait(const Task* t, const char* where) noexcept {
#if !defined(NDEBUG)
	std::fprintf(stderr,
		"[JLib::Scheduler] a coroutine task (%p) reached a blocking wait (%s).\n"
		"  Coroutines must co_await a scheduler awaitable; a blocking wait stalls the worker.\n",
		static_cast<void*>(const_cast<Task*>(t)), where);
	std::fflush(stderr);
	assert(false && "coroutine reached a blocking wait -- see stderr");
#else
	(void)t; (void)where;
#endif
}

void JLib::CheckSuspendableCurrent(const char* where) noexcept {
	if (Thread* t = Thread::GetCurrent()) CheckSuspendable(t->currentRunningTask, where);
}

void Fiber::Resume() {
	if (ResumeQueueless())
		TaskScheduler::Instance().ResumeFiber(this->owningTask);
}

void JLib::RequeueResumedBatch(Task** tasks, size_t n, Lane lane) {
	if (n == 0) return;

	(void)lane;
	auto& sched = TaskScheduler::Instance();
	for (size_t i = 0; i < n; ++i) sched.ResumeFiber(tasks[i]);
}

#if defined(JLIB_TSAN)
namespace JLib { namespace detail {
	void TsanSwitchToScheduler() noexcept { Thread::TsanSwitchToScheduler(); }
} }
#endif
