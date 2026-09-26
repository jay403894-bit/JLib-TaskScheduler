#pragma once
// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Context.h"
#include "platform.h"
#include "Task.h"
#include "Epochs.h"
#include "Stats.h"
#include "WaitRecord.h"
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

		// THE HAND-OFF, in two halves, for the same reason the park is in two halves: a fiber's
		// context is not saved until the switch actually happens, so the fiber that gives up the
		// thread cannot place ITSELF -- anything that picked it up would switch into a live stack.
		// So it names itself here, and whoever LANDS on this thread next places it. Thread-local:
		// only the OS thread doing the switch ever touches it, and only while mid-switch.
		void SetPendingHandoff(Task* t) noexcept;
		void PlacePendingHandoff() noexcept;
	}

	// Defined in Fiber.cpp, so this header does not need TaskScheduler: Fiber::Resume can place a
	// task it claimed. (The suspend primitives stamp their pin with StampPin, from Task.h.)
	void RequeueResume(Task* task);

	// Wake a waiter a primitive was holding. Defined in TaskScheduler.cpp, so this header does not
	// need Thread or TaskScheduler: a fiber goes through its park handle, anything else through the
	// scheduler.
	void WakeWaiter(Task* t) noexcept;

	// The fiber whose stack the caller is standing on, or null off a fiber stack. Derived from the
	// stack pointer and the pool's block ranges, so it holds at any depth, on any thread, after any
	// migration -- no thread-local state to keep in step. Defined in GlobalFiberPool.cpp.
	Fiber* FiberFromStack() noexcept;

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
			homeCtx = nullptr;
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
			taskFunction(other.taskFunction), status(other.status.load(std::memory_order_relaxed)), id(idGenerator.fetch_add(1, std::memory_order_relaxed)) {
		}
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
			* (--sp) = TebStackBase();
			*(--sp) = TebStackLimit();
			*(--sp) = 0;
			*(--sp) = TebDealloc();
#endif

			* (--sp) = 0;

			*(--sp) = 0;

			for (int i = 0; i < 8; ++i)
				*(--sp) = 0;

			ctx.rsp = (void*)sp;
		}
#endif

		// ---- park data -------------------------------------------------------------------------
		// Read by the scheduler after the switch and by the observer. NOT a second handshake.

		// The observer's registration for this fiber's current park. Embedded and reused: a fiber
		// is parked at most once at a time, so the park path allocates nothing.
		WaitRecord waitRecord;
		uint64_t   parkTimeoutMs = 0;   // deadline for this park; 0 = none

		// A flag this park waits on, watched by the observer. THE ADDRESS MUST OUTLIVE THE WAIT --
		// a flag in the waiting fiber's own frame is safe by construction, anything else is the
		// caller's rule to keep.
		const std::atomic<uint64_t>* parkFlag = nullptr;
		uint64_t                     parkMask = 0;

		// Deadline for the next suspend from this fiber, consumed by it. 0 = none.
		inline void StampDeadline(uint64_t ms) noexcept { parkTimeoutMs = ms; }

		// ---- the park --------------------------------------------------------------------------
		// PUBLISH, THEN SWITCH. Three calls in order, so the waiter is linked while the stack is
		// still live and a waker can find it:
		//
		//   BeginSuspend(pin)   -> WANTS_SUSPEND: discoverable, a waker may mark it
		//   link the waiter     -> Event node, mutex queue, or a lot slot via StoreTask
		//   FinishSuspend()     -> switch out
		//
		// Only a WAKER writes SUSPEND_SIGNALED; only the worker landing writes SUSPENDED, after the
		// switch. A task reaches a run queue only from SUSPENDED, so a waker that arrives before
		// that marks it and places nothing.
		inline bool CanSuspend() const {
			return owningTask
			    && owningTask->type != TaskType::Native
			    && owningTask->type != TaskType::Coroutine;
		}

		inline void BeginSuspend(Pin pin = Pin::None) {
			StampPin(owningTask, pin);
			status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
		}

		// The waiter could not be linked, or a waker already won: no switch, stay runnable.
		inline void CancelSuspend() noexcept {
			status.store(FiberStatus::RUNNING, std::memory_order_release);
		}

		inline bool FinishSuspend() {
			ContextSwitch(&this->ctx, this->homeCtx);
			// Back on this stack: the wait ended. If we were resumed by a direct hand-off, the fiber
			// that gave us the thread is waiting to be placed -- it could not place itself.
			detail::PlacePendingHandoff();
			parkFlag = nullptr;
			parkMask = 0;
			parkTimeoutMs = 0;
			return true;
		}

		// A wait nothing else holds: the lot slot is its waiter list, reached by handle.
		inline void Suspend(Pin pin = Pin::None, uint64_t timeoutMs = 0) {
			if (!CanSuspend()) return;
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::Suspend");
			StampDeadline(timeoutMs);
			BeginSuspend(pin);
			if (!StoreTask(owningTask, parkTimeoutMs)) { CancelSuspend(); return; }
			FinishSuspend();
		}

		// Wait until (*flag & mask) != 0, or the deadline passes. A wake means "look again": the
		// caller re-checks its own condition.
		inline void WaitOnFlag(const std::atomic<uint64_t>* flag, uint64_t mask,
			Pin pin = Pin::None, uint64_t timeoutMs = 0) {
			if (!CanSuspend()) return;
			if (flag && (flag->load(std::memory_order_acquire) & mask) != 0) return;
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::WaitOnFlag");
			parkFlag = flag;
			parkMask = mask;
			StampDeadline(timeoutMs);
			BeginSuspend(pin);
			if (!StoreTask(owningTask, parkTimeoutMs)) { CancelSuspend(); return; }
			FinishSuspend();
		}

		// ---- yield -----------------------------------------------------------------------------
		// Still runnable: a different primitive from the park.
#undef Yield
		inline void BeginYield(Pin pin = Pin::None) {
			StampPin(owningTask, pin);
			status.store(FiberStatus::WANTS_YIELD, std::memory_order_release);
		}
		inline void Yield(Pin pin = Pin::None) {
			if (!CanSuspend()) return;
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::Yield");
			BeginYield(pin);
			ContextSwitch(&this->ctx, this->homeCtx);
			detail::PlacePendingHandoff();   // see FinishSuspend
		}

		enum class ClaimResult { None, Signaled, Claimed };
		inline ClaimResult ClaimForWake() {
			for (;;) {
				const FiberStatus s = status.load(std::memory_order_acquire);
				if (s == FiberStatus::SUSPENDED) {
					FiberStatus exp = FiberStatus::SUSPENDED;
					if (!status.compare_exchange_weak(exp, FiberStatus::READY,
						std::memory_order_acq_rel))
						continue;
					// Only for a caller holding just the fiber (Resume). A caller that came through the
					// lot has already claimed the slot and cleared the handle, so this is a no-op for
					// it -- but winning the CAS is what makes this the wake, so the return is NOT
					// conditional on it.
					if (owningTask && owningTask->record
						&& owningTask->record->handle.load(std::memory_order_acquire).parked)
						ReleaseTask(owningTask);
					return ClaimResult::Claimed;
				}
				if (s == FiberStatus::WANTS_SUSPEND) {
					FiberStatus exp = FiberStatus::WANTS_SUSPEND;
					if (!status.compare_exchange_weak(exp, FiberStatus::SUSPEND_SIGNALED,
						std::memory_order_acq_rel))
						continue;
					return ClaimResult::Signaled;
				}
				return ClaimResult::None;
			}
		}

		inline bool Resume() {
			if (ClaimForWake() != ClaimResult::Claimed) return false;
			ReleaseTask(owningTask);
			WakeWaiter(owningTask);     // Kick/handle or home.resume � not Requeue()
			return true;
		}
		
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

			// f returns to THIS thread's scheduler, not to whatever thread it last ran on.
			f->homeCtx = this->homeCtx;

			// READY, not TRANSFERRED: this fiber is going on a run queue, and the run path switches
			// into whatever it finds. Named as the pending hand-off rather than pushed, because the
			// ctx below is not written until the switch happens -- see detail::SetPendingHandoff.
			this->status.store(FiberStatus::READY, std::memory_order_release);
			detail::SetPendingHandoff(this->owningTask);

			detail::SetCurrentFiber(f);

			// A direct hand-over (no scheduler pass): the thread running this fiber runs f next.
			if (f->owningTask && f->owningTask->record && this->owningTask->record)
				f->owningTask->record->home = this->owningTask->record->home;

			ContextSwitch(&this->ctx, &f->ctx);

			// Back on this stack, on whatever thread resumed us: drain whoever handed off to IT.
			detail::PlacePendingHandoff();
			return true;
		}

		// Defined in Fiber.cpp: both forward through record->home, so this header never needs
		// Thread's complete type.
		bool StoreTask(Task* task, uint64_t timeoutMs = 0);
		bool ReleaseTask(Task* task);

		inline Fiber* LastYielder() const { return last_yielder; }
		inline bool IsReady() const { return status == FiberStatus::READY; }
	};
}