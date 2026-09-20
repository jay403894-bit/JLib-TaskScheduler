// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Context.h"
#include "TsanFiber.h"   
#include "platform.h"
#include "Task.h"
#include "Epochs.h"
#include "Stats.h"
#include <atomic>
#include <cstdint>
namespace JLib {
	// Native tasks and coroutines run on the worker's stack with no fiber, so neither can suspend.
	// Called at every blocking wait:
	//   native    -> abort with a message.
	//   coroutine -> debug: assert naming the task (it must co_await instead).
	//                release: nothing; the wait blocks the worker for its duration.
	[[noreturn]] void NativeSuspendViolation(const char* where) noexcept;
	void CoroutineBlockingWait(const Task* t, const char* where) noexcept;
	inline void CheckSuspendable(const Task* t, const char* where) noexcept {
		if (!t) return;
		if (t->native) NativeSuspendViolation(where);
		if (t->type == TaskType::Coroutine) CoroutineBlockingWait(t, where);
	}
	void CheckSuspendableCurrent(const char* where) noexcept;   // uses the current thread's task
}
namespace JLib {
	enum class FiberStatus {
		READY,         
		RUNNING,       
		WANTS_YIELD,   
		WANTS_SUSPEND, 
		SUSPEND_SIGNALED, 
		SUSPENDED,     

		TRANSFERRED,
		DEAD           
	};
	
	struct FiberDebt {
		FiberDebt* next = nullptr;
		void*      obj  = nullptr;
		void     (*release)(void*) noexcept = nullptr;

		static constexpr size_t kAnyHolder = (size_t)-1;
		size_t holder = kAnyHolder;
	};

	struct Fiber;   
	namespace detail {
		
		void ReleaseFiberSlots(void** slots, size_t n) noexcept;

		void SetCurrentFiber(Fiber* f) noexcept;

#if defined(JLIB_TSAN)
		void TsanSwitchToScheduler() noexcept;
#endif

		void HandOffFiberDebts(FiberDebt* head) noexcept;
	}

	struct alignas(16) Fiber {
		Context ctx;
		uint64_t id;
		void* stackBase;
		size_t stackSize;
		
		StackClass stackClass = StackClass::Standard;
		
		size_t poolIndex = SIZE_MAX;
		Task* owningTask = nullptr; 

		Fiber* last_yielder = nullptr;
		Context* homeCtx = nullptr;

		// Pin, debts and task-local slots live on the task's TaskRecord, not here.
		static constexpr size_t kLocalSlots = TaskRecord::kLocalSlots;


		void* tsanFiber = nullptr;

		// Kinds of cleanup a task can owe its holders (stored on TaskRecord::owedKinds).
		enum OwedKind : uint32_t {
			kOwesNothing = 0,
			kOwesSlab    = 1u << 0,
			kOwesEpoch   = 1u << 1,
			kOwesHazard  = 1u << 2,
		};

		void ResetForReuse() {
			owningTask = nullptr;
			last_yielder = nullptr;
			homeCtx    = nullptr;
			status.store(FiberStatus::READY, std::memory_order_release);
		}

		void* operator new(std::size_t) = delete;
		void* operator new[](std::size_t) = delete;
		void  operator delete(void*) = delete;
		void  operator delete[](void*) = delete;

		std::atomic<FiberStatus>  status;
		
		inline static std::atomic<uint64_t> idGenerator{ 0 };
		void (*taskFunction)();
		Fiber() : stackBase(nullptr), stackSize(0), taskFunction(nullptr), status(FiberStatus::READY), id(idGenerator.fetch_add(1, std::memory_order_relaxed)) {

		}
		Fiber(Fiber&& other) noexcept
			: ctx(other.ctx), stackBase(other.stackBase), stackSize(other.stackSize),
			  taskFunction(other.taskFunction), status(other.status.load(std::memory_order_relaxed)), id(idGenerator.fetch_add(1, std::memory_order_relaxed)) {}
		Fiber& operator=(Fiber&&) = delete;
		Fiber(const Fiber&) = delete;
		Fiber& operator=(const Fiber&) = delete;
		void Init(void (*entryPoint)());
		
		// The suspend primitives. Every suspending API calls one of these before it registers the
		// waiter (a waker may resume the task at once), passing its Pin through untouched.
		inline void BeginSuspend(Pin pin) noexcept {
			StampPin(owningTask, pin);
			status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
		}
		inline void BeginYield(Pin pin) noexcept {
			JLIB_STAT(Yields);
			StampPin(owningTask, pin);
			status.store(FiberStatus::WANTS_YIELD, std::memory_order_release);
		}

		inline void CoYield(Pin pin = Pin::None) {
			CheckSuspendable(this->owningTask, "Fiber::CoYield");
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::CoYield");
			BeginYield(pin);
#if defined(JLIB_TSAN)
			detail::TsanSwitchToScheduler();
#endif
			ContextSwitch(&this->ctx, this->homeCtx);
		}

		inline void Suspend(Pin pin = Pin::None) {
			CheckSuspendable(this->owningTask, "Fiber::Suspend");
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::Suspend");
			BeginSuspend(pin);
#if defined(JLIB_TSAN)
			detail::TsanSwitchToScheduler();
#endif
			ContextSwitch(&this->ctx, this->homeCtx);
		}
		
		inline bool ResumeQueueless() {
			while (true) {
				FiberStatus s = status.load(std::memory_order_acquire);
				if (s == FiberStatus::SUSPENDED) {
					FiberStatus exp = FiberStatus::SUSPENDED;
					return status.compare_exchange_strong(exp, FiberStatus::READY,
						std::memory_order_acq_rel);
				}
				else if (s == FiberStatus::WANTS_SUSPEND) {
					FiberStatus exp = FiberStatus::WANTS_SUSPEND;
					if (status.compare_exchange_strong(exp, FiberStatus::SUSPEND_SIGNALED,
							std::memory_order_acq_rel))
						return false;           
					
				}
				else {
					
					return false;
				}
			}
		}
		
		// The SwitchToFiber equivalent: hand this thread to another fiber directly. It is a SUSPEND
		// POINT like Suspend/CoYield -- this fiber stops running here and may resume on a different
		// thread -- so it takes the same two checks. The epoch one matters most: a guard held
		// across it would be released by ~SlotEpochGuard into the slot of whatever thread finishes
		// this fiber, clearing an innocent thread's pin.
		inline bool SwitchTo(Fiber* f) {
			if (!f || f == this) return false;
			CheckSuspendable(this->owningTask, "Fiber::SwitchTo");
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::SwitchTo");

			FiberStatus exp = FiberStatus::READY;
			if (!f->status.compare_exchange_strong(exp, FiberStatus::RUNNING,
					std::memory_order_acq_rel, std::memory_order_relaxed)) {
				
				exp = FiberStatus::TRANSFERRED;
				if (!f->status.compare_exchange_strong(exp, FiberStatus::RUNNING,
						std::memory_order_acq_rel, std::memory_order_relaxed))
					return false;
			}

			f->last_yielder = this;

			f->homeCtx = this->homeCtx;

			this->status.store(FiberStatus::TRANSFERRED, std::memory_order_release);

			detail::SetCurrentFiber(f);

			ContextSwitch(&this->ctx, &f->ctx);

			return true;
		}

		inline Fiber* LastYielder() const { return last_yielder; }

		void Resume();   

		bool IsReady() const { return status == FiberStatus::READY; }
	};
} 

namespace JLib {
	
	void RequeueResumedBatch(Task** tasks, size_t n, Lane lane);
}
