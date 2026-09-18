// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Fiber.h"
#include <atomic>
#include <cstddef>
#include <vector>

namespace JLib {

	class GlobalFiberPool;
	class Task;

	class FiberRegistry {
	public:
		static FiberRegistry& Instance();

		void Build(GlobalFiberPool* pool, size_t workerCount);
		void Reset();

		// The cleanup chain runs on a dead task's RECORD: it hops to each holder named in the
		// record's debts, that holder releases its own debts, and when none remain the fiber is
		// recycled and the record freed.
		bool AdvanceCleanup(TaskRecord* r);
		static size_t DischargeDebts(TaskRecord* r, size_t holder) noexcept;

		using DispatchFn = bool (*)(size_t worker, TaskRecord* r);

		using RecycleFn  = void (*)(TaskRecord* r);
		void SetDispatch(DispatchFn fn);
		void SetRecycle(RecycleFn fn);

		static void CleanupHop(void* record);

		bool ReturnToPool(Fiber* f);

		static constexpr size_t kExternalReaders = 64;
		static constexpr size_t kNoHolder        = ~size_t(0);

		size_t HolderCount()             const { return workers + kExternalReaders; }
		size_t HolderOfWorker(size_t w)  const { return w; }          
		bool   IsWorkerHolder(size_t h)  const { return h < workers; }

		size_t ClaimExternal();

		size_t CurrentHolder();

		bool        Deliver(size_t holder, TaskRecord* r);

		TaskRecord* TakeAll(size_t holder);
		
		bool   HolderHasWork(size_t holder) const;

		size_t DrainHolder(size_t holder);

		size_t DrainAllForTeardown();

		using ReleaseFn = void (*)(size_t holder, TaskRecord* r);
		void SetRelease(ReleaseFn fn);

		static void QueueDebtRelease();

		static constexpr uint16_t kNoSlot = 0xFFFF;

		static uint16_t FlsAlloc() noexcept;
		static void*    FlsGet(uint16_t slot) noexcept;
		static void     FlsSet(uint16_t slot, void* p) noexcept;

		static size_t GetID(const Fiber* f) noexcept;
		static size_t GetID() noexcept;          

		using SlotDeleter = void (*)(void*);
		void SetSlotDeleter(size_t slot, SlotDeleter fn);

	private:
		FiberRegistry() = default;
		
		DispatchFn Dispatcher();
		RecycleFn  Recycler();

		std::vector<std::atomic<TaskRecord*>> inbound;
		size_t workers = 0;
		std::atomic<size_t> externalNext{ 0 };

		std::atomic<DispatchFn> dispatchFn{ nullptr };
		std::atomic<RecycleFn>  recycleFn{ nullptr };
		std::atomic<ReleaseFn>  releaseFn{ nullptr };
		
		GlobalFiberPool* pool = nullptr;
		DispatchFn dispatch = nullptr;
		RecycleFn  recycle  = nullptr;
	};

	// Task-local storage: the slots live on the running task's TaskRecord, so they follow the task
	// across migration and work for any body type. FiberLocal is the old name.
	template <typename T>
	struct TaskLocal {
		uint16_t slot = FiberRegistry::kNoSlot;

		T*   get() const noexcept       { return static_cast<T*>(FiberRegistry::FlsGet(slot)); }
		void set(T* p) const noexcept   { FiberRegistry::FlsSet(slot, p); }
		T*   operator->() const noexcept { return get(); }
		explicit operator bool() const noexcept { return get() != nullptr; }
	};
	template <typename T> using FiberLocal = TaskLocal<T>;

	template <typename T>
	inline TaskLocal<T> MakeTaskLocal() noexcept {
		return TaskLocal<T>{ FiberRegistry::FlsAlloc() };
	}
	template <typename T>
	inline TaskLocal<T> MakeFiberLocal() noexcept { return MakeTaskLocal<T>(); }

}
