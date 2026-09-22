// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Context.h"
#include "platform.h"
#include "Task.h"
#include "Epochs.h"
#include "Stats.h"
#include <atomic>
#include <cstdint>
extern "C" void FiberTrampoline();
namespace JLib {
	[[noreturn]] void NativeSuspendViolation(const char* where) noexcept;
	void CoroutineBlockingWait(const Task* t, const char* where) noexcept;
	inline void CheckSuspendable(const Task* t, const char* where) noexcept {
		if (!t) return;
		if (t->type == TaskType::Native) NativeSuspendViolation(where);
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

	struct Fiber;
	namespace detail {

		void ReleaseFiberSlots(void** slots, size_t n) noexcept;

		void SetCurrentFiber(Fiber* f) noexcept;
	}

	// Defined in Fiber.cpp, so this header does not need TaskScheduler: Fiber::Resume can place a
	// task it claimed. (The suspend primitives stamp their pin with StampPin, from Task.h.)
	void RequeueResume(Task* task);

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
#if JLIB_PLATFORM_WINDOWS
		// The TEB stack bounds the context switch installs on entry (see ContextSwitch.asm 1b). The
		// region is [stackBase, stackBase+stackSize): one uncommitted guard page, then the stack.
		uintptr_t TebStackBase()  const { return (uintptr_t)stackBase + stackSize; }
		uintptr_t TebStackLimit() const { return (uintptr_t)stackBase + platform::PageSize(); }
		uintptr_t TebDealloc()    const { return (uintptr_t)stackBase; }
#endif
#if JLIB_PLATFORM_WINDOWS && JLIB_ARCH_X86_64
		inline void Init(void(*entryPoint)()) {

			uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
			uintptr_t* sp = (uintptr_t*)top;

			sp -= 4;

			*(--sp) = (uintptr_t)&FiberTrampoline;

			*(--sp) = (uintptr_t)entryPoint;
			*(--sp) = 0;
			*(--sp) = 0;
			*(--sp) = 0;
			*(--sp) = 0;
			*(--sp) = 0;
			*(--sp) = 0;
			*(--sp) = 0;

			*(--sp) = TebStackBase();
			*(--sp) = TebStackLimit();
			*(--sp) = TebDealloc();
			*(--sp) = 0;

			*(--sp) = 0x0000037F00001F80ULL;

			for (int k = 0; k < 20; ++k) *(--sp) = 0;

			ctx.rsp = (void*)sp;
		}
#elif JLIB_ARCH_X86_64      
		inline void Init(void(*entryPoint)()) {

			uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
			uintptr_t* sp = (uintptr_t*)top;

			*(--sp) = (uintptr_t)&FiberTrampoline;

			*(--sp) = 0;
			*(--sp) = (uintptr_t)entryPoint;
			*(--sp) = 0;
			*(--sp) = 0;
			*(--sp) = 0;
			*(--sp) = 0;

			*(--sp) = 0x0000037F00001F80ULL;

			ctx.rsp = (void*)sp;
		}
#elif JLIB_ARCH_AARCH64
		inline void Init(void(*entryPoint)()) {

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

#if JLIB_PLATFORM_WINDOWS
			// Windows ARM64: the TEB block (src/win32/aarch64/ContextSwitch.asm step 1b).
			*(--sp) = TebStackBase();
			*(--sp) = TebStackLimit();
			*(--sp) = 0;
			*(--sp) = TebDealloc();
#endif

			*(--sp) = 0;

			*(--sp) = 0;

			for (int i = 0; i < 8; ++i)
				*(--sp) = 0;

			ctx.rsp = (void*)sp;
		}
#endif
	
		// The suspend primitives. Every suspending API calls one of these before it registers the
		// waiter (a waker may resume the task at once), passing its Pin through untouched.
#undef Yield
		inline void BeginYield(Pin pin = Pin::None) {
			StampPin(owningTask, pin);
			status.store(FiberStatus::WANTS_YIELD, std::memory_order_release);

		}
		inline void Yield(Pin pin = Pin::None) {
			// Native and Coroutine bodies cannot suspend; only a fiber can.
			if (this->owningTask->type == TaskType::Native
			 || this->owningTask->type == TaskType::Coroutine)
				return;
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::Yield");
			BeginYield(pin);
			ContextSwitch(&this->ctx, this->homeCtx);
		}
		inline void BeginSuspend(Pin pin = Pin::None) {
			// Native and Coroutine bodies cannot suspend; only a fiber can.
			if (this->owningTask->type == TaskType::Native
			 || this->owningTask->type == TaskType::Coroutine)
				return;
			StampPin(owningTask, pin);
			status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);

		}
		inline void Suspend(Pin pin = Pin::None) {
			// Native and Coroutine bodies cannot suspend; only a fiber can.
			if (this->owningTask->type == TaskType::Native
			 || this->owningTask->type == TaskType::Coroutine)
				return;
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::Suspend");
			BeginSuspend(pin);
			ContextSwitch(&this->ctx, this->homeCtx);
		}
		
		inline bool Resume() {
			while (true) {
				FiberStatus s = status.load(std::memory_order_acquire);
				if (s == FiberStatus::SUSPENDED) {
					FiberStatus exp = FiberStatus::SUSPENDED;
					if (status.compare_exchange_strong(exp, FiberStatus::READY, std::memory_order_acq_rel)) {
						RequeueResume(owningTask);
						return true;
					}
					return false;
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
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::SwitchTo");
			// Native and Coroutine bodies cannot suspend; only a fiber can.
			if (this->owningTask->type == TaskType::Native
			 || this->owningTask->type == TaskType::Coroutine)
				return false;
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

			// A direct hand-over (no scheduler pass): the thread running this fiber runs f next.
			if (f->owningTask && f->owningTask->record && this->owningTask->record)
				f->owningTask->record->home = this->owningTask->record->home;

			ContextSwitch(&this->ctx, &f->ctx);

			return true;
		}

		inline Fiber* LastYielder() const { return last_yielder; }
	
		bool IsReady() const { return status == FiberStatus::READY; }
	};
} 

namespace JLib {
	
	void RequeueResumedBatch(Task** tasks, size_t n, Lane lane);
	void RequeueResume(Task* task);
}
