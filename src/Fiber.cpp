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
	if (!TaskScheduler::IsInitialized()) return;
	if (Thread* t = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers()))
		CheckSuspendable(t->currentRunningTask, where);
}


// The park and the unpark, both addressed through record->home -- the thread that was running the
// task when it suspended, which is the thread whose table holds the slot. Never detail::TlsThread():
// a resumer runs on some other thread entirely.
bool Fiber::StoreTask(Task* task, uint64_t timeoutMs) {
	Thread* home = task->record->home;
	return home && home->StoreSuspended(task, timeoutMs);
}

// True only for the caller that actually took the task out of its slot; everyone else lost the
// race and must not requeue it.
bool Fiber::ReleaseTask(Task* task) {
	Thread* home = task->record->home;
	return home && home->ResumeTask(task);
}

// See the note in Fiber.h. Set before a hand-off switch, drained by whatever lands on this thread
// afterwards -- which is the first moment the handed-off fiber's context is saved and it is safe
// for another worker to pick it up.
static thread_local Task* t_handoff = nullptr;

void JLib::detail::SetPendingHandoff(Task* t) noexcept { t_handoff = t; }

void JLib::detail::PlacePendingHandoff() noexcept {
	Task* const t = t_handoff;
	if (!t) return;
	t_handoff = nullptr;
	// Push, not Requeue: the fiber that handed off is unpinned by construction (a pinned one would
	// have had to come back here anyway) and Push leaves it on this worker's own deque, hot and
	// stealable -- the same placement an unpinned wake takes.
	if (TaskScheduler::IsInitialized()) TaskScheduler::Instance().Push(t);
}

