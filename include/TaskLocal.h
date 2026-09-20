// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Task-local storage. The slots live on the running task's TaskRecord, so they follow the task
// across migration and work for any body type (fiber, coroutine, native). Nothing here is a
// registry: there is no table of live tasks and no instance to reach through -- a slot index and
// the record are all it takes.
//
// This replaces FiberRegistry, which existed for two things: these slots, and a cleanup chain
// that hopped a dead task's record from worker to worker. The hop went in 2026-09-19 (nothing in
// the library ever created a holder-specific debt), and with it the per-holder table, the
// dispatcher indirection and the singleton.
#pragma once
#include "Fiber.h"
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace JLib {

	inline constexpr uint16_t kNoTaskLocalSlot = 0xFFFF;

	// Slots are allocated once per handle, process-wide, and survive pool restarts: a TaskLocal<T>
	// is usually a static in the caller's code, outliving any one Init.
	uint16_t AllocTaskLocalSlot() noexcept;
	void*    TaskLocalGet(uint16_t slot) noexcept;
	void     TaskLocalSet(uint16_t slot, void* p) noexcept;

	using TaskLocalDeleter = void (*)(void*);
	// Called on the value in `slot` when a task dies, if one is set.
	void SetTaskLocalDeleter(size_t slot, TaskLocalDeleter fn) noexcept;

	// Debts left by a dead record (ReleaseOnFiberDeath) are released by a pool task.
	void QueueDebtRelease();

	template <typename T>
	struct TaskLocal {
		uint16_t slot = kNoTaskLocalSlot;

		T*   get() const noexcept        { return static_cast<T*>(TaskLocalGet(slot)); }
		void set(T* p) const noexcept    { TaskLocalSet(slot, p); }
		T*   operator->() const noexcept { return get(); }
		explicit operator bool() const noexcept { return get() != nullptr; }
	};
	template <typename T> using FiberLocal = TaskLocal<T>;   // the old name

	template <typename T>
	inline TaskLocal<T> MakeTaskLocal() noexcept {
		return TaskLocal<T>{ AllocTaskLocalSlot() };
	}
	template <typename T>
	inline TaskLocal<T> MakeFiberLocal() noexcept { return MakeTaskLocal<T>(); }

}
