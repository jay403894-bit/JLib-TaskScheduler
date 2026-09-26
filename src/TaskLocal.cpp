// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Task-local slots and the death-debt release. Was FiberRegistry; see TaskLocal.h for what left.

#include "../include/TaskLocal.h"
#include "../include/TaskScheduler.h"
#include "../include/Thread.h"

namespace JLib {

	// One slot index per handle, handed out once. Process-wide on purpose: a TaskLocal<T> is
	// usually a static that outlives any single pool.
	static std::atomic<uint16_t> g_nextTaskLocalSlot{ 0 };

	static std::atomic<TaskLocalDeleter> g_slotDeleter[TaskRecord::kLocalSlots] = {};
	static std::atomic<uint32_t>         g_slotDeleterMask{ 0 };

	uint16_t AllocTaskLocalSlot() noexcept {
		const uint16_t s = g_nextTaskLocalSlot.fetch_add(1, std::memory_order_relaxed);
		if (s >= (uint16_t)TaskRecord::kLocalSlots) return kNoTaskLocalSlot;
		return s;
	}

	void* TaskLocalGet(uint16_t slot) noexcept {
		if (slot >= (uint16_t)TaskRecord::kLocalSlots) return nullptr;
		TaskRecord* r = TaskScheduler::CurrentRecord();
		return (r && r->locals) ? r->locals[slot] : nullptr;
	}

	void TaskLocalSet(uint16_t slot, void* p) noexcept {
		if (slot >= (uint16_t)TaskRecord::kLocalSlots) return;
		if (void** block = TaskScheduler::CurrentLocals()) block[slot] = p;
	}

	void SetTaskLocalDeleter(size_t slot, TaskLocalDeleter fn) noexcept {
		if (slot >= Fiber::kLocalSlots) return;
		g_slotDeleter[slot].store(fn, std::memory_order_release);
		// The mask keeps the common case (no deleters at all) to one relaxed load at task death.
		if (fn) g_slotDeleterMask.fetch_or(1u << slot, std::memory_order_release);
		else    g_slotDeleterMask.fetch_and(~(1u << slot), std::memory_order_release);
	}

	namespace detail {
		void ReleaseFiberSlots(void** slots, size_t n) noexcept {
			const uint32_t mask = g_slotDeleterMask.load(std::memory_order_acquire);
			if (!mask || !slots) return;

			for (size_t i = 0; i < n && i < Fiber::kLocalSlots; ++i) {
				if (!(mask & (1u << i))) continue;
				void* p = slots[i];
				if (!p) continue;

				const TaskLocalDeleter fn = g_slotDeleter[i].load(std::memory_order_acquire);
				if (!fn) continue;

				slots[i] = nullptr;
				fn(p);
			}
		}
	}

	// ---- debts owed at task death ----
	//
	// A dying record hands its list to a global stack, and one pool task drains it. Not run inline
	// at death because a release function is the caller's code: it may allocate, take a lock, or
	// touch the scheduler, none of which belong on the path that is freeing a record.

	static std::atomic<bool>       g_reclaimQueued{ false };
	static std::atomic<TaskDebt*> g_pendingDebts{ nullptr };

	namespace detail {
		void HandOffTaskDebts(TaskDebt* head) noexcept {
			if (!head) return;

			TaskDebt* tail = head;
			while (tail->next) tail = tail->next;

			TaskDebt* old = g_pendingDebts.load(std::memory_order_relaxed);
			do {
				tail->next = old;
			} while (!g_pendingDebts.compare_exchange_weak(old, head,
						std::memory_order_release, std::memory_order_relaxed));

			QueueDebtRelease();
		}
	}

	static size_t ReleasePendingDebts() {
		TaskDebt* d = g_pendingDebts.exchange(nullptr, std::memory_order_acq_rel);
		size_t n = 0;
		while (d) {
			TaskDebt* nxt = d->next;
			d->next = nullptr;
			if (d->release && d->obj) d->release(d->obj);
			++n;
			d = nxt;
		}
		return n;
	}

	void QueueDebtRelease() {
		bool expected = false;
		if (!g_reclaimQueued.compare_exchange_strong(expected, true,
				std::memory_order_acq_rel, std::memory_order_relaxed))
			return;                                   // one release task in flight is enough

		TaskScheduler* s = TaskScheduler::IsInitialized() ? &TaskScheduler::Instance() : nullptr;
		if (!s) { g_reclaimQueued.store(false, std::memory_order_release); return; }

		Task* t = s->CreateInternalTask([] {
			ReleasePendingDebts();
			g_reclaimQueued.store(false, std::memory_order_seq_cst);

			// Anything that arrived while we ran needs another pass.
			if (g_pendingDebts.load(std::memory_order_seq_cst) != nullptr)
				QueueDebtRelease();
		});

		if (!t) { g_reclaimQueued.store(false, std::memory_order_release); return; }

		if (!s->Push(t)) {
			g_reclaimQueued.store(false, std::memory_order_release);
			return;
		}
	}

}
