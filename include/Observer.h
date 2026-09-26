// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstdint>
#include "MPSCQueue.h"

namespace JLib {
	struct Task;

	// One thread outside the pool, for waits nothing inside can see: a deadline has no waker, so a
	// worker that parks with one would sleep through it.
	//
	// Not a worker: no qIndex, no deque, no inbox, nothing can be pinned to it. It never runs user
	// code. It does not deliver either -- it calls Thread::Kick, and the owner's PollTasks delivers
	// through the same PARKED claim every other waker uses, so an observer beating a worker to a
	// deadline is a lost CAS rather than a second delivery.
	//
	// A watched flag must outlive the watch (WaitRecord::flag).
	class Observer {
	public:
		// Flag bits, so a woken task can tell why it came back.
		static constexpr uint32_t kDeadlineFired = 1u << 1;
		static constexpr uint32_t kFlagFired     = 1u << 2;

		// Called from a park commit, after the fiber is off its stack. The record belongs to the
		// observer from here. False if the observer is not running: no backstop for this wait.
		static bool Register(Task* task, uint64_t deadlineMs,
		                     const std::atomic<uint64_t>* flag = nullptr, uint64_t mask = 0) noexcept;

		// Any thread, one relaxed bump, no queue push. Required for a flag-only wait: it has no
		// deadline to expire on, so nothing else retires it.
		static void Cancel(Task* task) noexcept;

		static void Start();
		static void Stop();
		static bool Running() noexcept;

		static uint64_t Fired() noexcept;
		static size_t   Active() noexcept;
		// Flag-only watches retired because their park had already ended. Stuck at zero while such
		// waits run means they are leaking.
		static uint64_t GenDrops() noexcept;
	};

	// Optional companion, off unless Config::watchdog is set. A separate thread, not extra work on
	// the observer: a hook that blocked there would stop every deadline in the process.
	//
	// It depends on nothing in the pool -- plain thread, plain queue, no scheduler lock, no task
	// allocation, no fiber -- so it can run while the pool is stalled. It accumulates nothing: a
	// hook walks the live structures when it runs.
	class Watchdog {
	public:
		// A function pointer, not std::function: MPSCQueue<T> requires a standard-layout T.
		using Hook = void (*)(void*) noexcept;

		// The caller owns the entry, it must outlive the run, and it may be queued only once --
		// the hook is the node. Same rule as WaitRecord.
		struct HookEntry {
			MPSCNode mpscHook;
			Hook     fn  = nullptr;
			void*    ctx = nullptr;
		};

		// Any thread. False if the watchdog is not running or the entry has no function.
		// A hook reads pool state; calling back into the scheduler during a stall can hang on the
		// thing that has stalled.
		static bool Push(HookEntry* e) noexcept;

		static void Start();
		static void Stop();
		static bool Running() noexcept;

		static uint64_t Ran() noexcept;
	};
}
