// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/FiberRegistry.h"
#include "../include/GlobalFiberPool.h"
#include "../include/TaskScheduler.h"
#include "../include/Thread.h"
#include "../include/Epochs.h"    
#include "../include/Hazard.h"    

namespace JLib {

	FiberRegistry& FiberRegistry::Instance() {
		
		static FiberRegistry* r = new FiberRegistry();
		return *r;
	}

	void FiberRegistry::Build(GlobalFiberPool* p, size_t workerCount) {
		pool = p;
		workers = workerCount;
		externalNext.store(0, std::memory_order_release);

		std::vector<std::atomic<TaskRecord*>> fresh(HolderCount());
		for (auto& h : fresh) h.store(nullptr, std::memory_order_relaxed);
		inbound.swap(fresh);
	}

	void FiberRegistry::Reset() {
		inbound.clear();
		pool = nullptr;
		workers = 0;
		externalNext.store(0, std::memory_order_release);
	}

	size_t FiberRegistry::ClaimExternal() {
		
		const size_t slot = externalNext.fetch_add(1, std::memory_order_acq_rel);
#if defined(JLIB_FIBERHOLDER_CTL_NO_EXHAUST_GUARD)
		return workers + (slot % kExternalReaders);   
#else
		if (slot >= kExternalReaders) return kNoHolder;
		return workers + slot;
#endif
	}

	size_t FiberRegistry::CurrentHolder() {
		
		if (Thread* w = Thread::Current()) {
			const size_t h = HolderOfWorker((size_t)w->qIndex);
			if (h < workers) return h;
			
		}
		thread_local size_t mine = kNoHolder;
		if (mine == kNoHolder) mine = ClaimExternal();
		return mine;
	}

	bool FiberRegistry::Deliver(size_t holder, TaskRecord* r) {
		if (!r || holder >= inbound.size()) return false;

		TaskRecord* head = inbound[holder].load(std::memory_order_relaxed);
		do {
			r->cleanupNext = head;
		} while (!inbound[holder].compare_exchange_weak(head, r,
					std::memory_order_release, std::memory_order_relaxed));

#if !defined(JLIB_FIBERHOLDER_CTL_NO_NOTIFY)
		
		if (holder < workers) TaskScheduler::NotifyHolder(holder);
#endif
		return true;
	}

	TaskRecord* FiberRegistry::TakeAll(size_t holder) {
		if (holder >= inbound.size()) return nullptr;
		
		return inbound[holder].exchange(nullptr, std::memory_order_acq_rel);
	}

	bool FiberRegistry::HolderHasWork(size_t holder) const {
		if (holder >= inbound.size()) return false;
		
		return inbound[holder].load(std::memory_order_acquire) != nullptr;
	}

	size_t FiberRegistry::DrainHolder(size_t holder) {
		TaskRecord* r = TakeAll(holder);
		size_t n = 0;
		while (r) {
			TaskRecord* nxt = r->cleanupNext;
			r->cleanupNext = nullptr;

			if (ReleaseFn fn = releaseFn.load(std::memory_order_relaxed)) fn(holder, r);
			DischargeDebts(r, holder);   // this holder's debts, released on this holder's thread
			AdvanceCleanup(r);
			++n;
			r = nxt;
		}
		return n;
	}

	size_t FiberRegistry::DrainAllForTeardown() {
		// Each hop discharges one holder's debts, so a record needs at most HolderCount() hops.
		size_t n = 0;
		for (size_t sweep = 0; sweep <= HolderCount(); ++sweep) {
			size_t moved = 0;
			for (size_t h = 0; h < inbound.size(); ++h) moved += DrainHolder(h);
			if (moved == 0) break;      
			n += moved;
		}
		return n;
	}

	void FiberRegistry::SetRelease(ReleaseFn fn) { releaseFn.store(fn, std::memory_order_relaxed); }

	static std::atomic<FiberRegistry::SlotDeleter> g_slotDeleter[TaskRecord::kLocalSlots] = {};
	static std::atomic<uint32_t> g_slotDeleterMask{ 0 };

	void FiberRegistry::SetSlotDeleter(size_t slot, SlotDeleter fn) {
		if (slot >= Fiber::kLocalSlots) return;   
		g_slotDeleter[slot].store(fn, std::memory_order_release);
		
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
				
				const FiberRegistry::SlotDeleter fn =
					g_slotDeleter[i].load(std::memory_order_acquire);
				if (!fn) continue;
				
				slots[i] = nullptr;
				fn(p);
			}
		}
	}

	bool FiberRegistry::ReturnToPool(Fiber* f) {
		if (!f || !pool) return false;
		
		FiberStatus expected = FiberStatus::DEAD;
		if (!f->status.compare_exchange_strong(expected, FiberStatus::READY,
				std::memory_order_acq_rel, std::memory_order_acquire))
			return false;
		
		pool->ReturnBatch(&f, 1);
		return true;
	}

	void FiberRegistry::SetDispatch(DispatchFn fn) { dispatchFn.store(fn, std::memory_order_relaxed); }
	void FiberRegistry::SetRecycle(RecycleFn fn)   { recycleFn.store(fn, std::memory_order_relaxed); }

	static bool DefaultDispatch(size_t holder, TaskRecord* r) {
		return FiberRegistry::Instance().Deliver(holder, r);
	}

	// End of the chain: return the body to the pool, then release the record itself.
	static void DefaultRecycle(TaskRecord* r) {
		Fiber* f = r->fiber;
		r->fiber = nullptr;
		if (f) FiberRegistry::Instance().ReturnToPool(f);
		TaskScheduler::Instance().ReleaseRecord(r);
	}

	FiberRegistry::DispatchFn FiberRegistry::Dispatcher() {
		DispatchFn d = dispatchFn.load(std::memory_order_relaxed);
		return d ? d : &DefaultDispatch;
	}
	FiberRegistry::RecycleFn FiberRegistry::Recycler() {
		RecycleFn r = recycleFn.load(std::memory_order_relaxed);
		return r ? r : &DefaultRecycle;
	}

	static std::atomic<uint16_t> g_nextFlsSlot{ 0 };

	uint16_t FiberRegistry::FlsAlloc() noexcept {
		const uint16_t s = g_nextFlsSlot.fetch_add(1, std::memory_order_relaxed);

		if (s >= (uint16_t)TaskRecord::kLocalSlots) return kNoSlot;
		return s;
	}

	void* FiberRegistry::FlsGet(uint16_t slot) noexcept {
		if (slot >= (uint16_t)TaskRecord::kLocalSlots) return nullptr;
		TaskRecord* r = TaskScheduler::CurrentRecord();
		return (r && r->locals) ? r->locals[slot] : nullptr;
	}

	void FiberRegistry::FlsSet(uint16_t slot, void* p) noexcept {
		if (slot >= (uint16_t)TaskRecord::kLocalSlots) return;
		if (void** block = TaskScheduler::CurrentLocals()) block[slot] = p;
	}

	size_t FiberRegistry::GetID(const Fiber* f) noexcept {
		return f ? f->poolIndex : SIZE_MAX;
	}

	size_t FiberRegistry::GetID() noexcept {
		Thread* t = Thread::GetCurrent();
		return GetID(t ? t->currentFiber : nullptr);
	}

	static std::atomic<bool> g_reclaimQueued{ false };

	static std::atomic<FiberDebt*> g_pendingDebts{ nullptr };

	namespace detail {
		void HandOffFiberDebts(FiberDebt* head) noexcept {
			if (!head) return;
			
			FiberDebt* tail = head;
			while (tail->next) tail = tail->next;

			FiberDebt* old = g_pendingDebts.load(std::memory_order_relaxed);
			do {
				tail->next = old;
			} while (!g_pendingDebts.compare_exchange_weak(old, head,
						std::memory_order_release, std::memory_order_relaxed));

			FiberRegistry::QueueDebtRelease();
		}
	}

	static size_t ReleasePendingDebts() {
		
		FiberDebt* d = g_pendingDebts.exchange(nullptr, std::memory_order_acq_rel);
		size_t n = 0;
		while (d) {
			
			FiberDebt* nxt = d->next;
			d->next = nullptr;
			if (d->release && d->obj) d->release(d->obj);
			++n;
			d = nxt;
		}
		return n;
	}

	void FiberRegistry::QueueDebtRelease() {
		bool expected = false;
		if (!g_reclaimQueued.compare_exchange_strong(expected, true,
				std::memory_order_acq_rel, std::memory_order_relaxed))
			return;                                   

		TaskScheduler* s = TaskScheduler::IsInitialized() ? &TaskScheduler::Instance() : nullptr;
		
		if (!s) { g_reclaimQueued.store(false, std::memory_order_release); return; }

		Task* t = s->CreateInternalTask([] {
			
			ReleasePendingDebts();
			
			g_reclaimQueued.store(false, std::memory_order_seq_cst);

			if (g_pendingDebts.load(std::memory_order_seq_cst) != nullptr)
				QueueDebtRelease();
		}, Lane::Normal);

		if (!t) { g_reclaimQueued.store(false, std::memory_order_release); return; }

		if (!s->Push(t)) {
			g_reclaimQueued.store(false, std::memory_order_release);
			return;
		}
	}

	size_t FiberRegistry::DischargeDebts(TaskRecord* r, size_t holder) noexcept {
		if (!r) return 0;
		size_t n = 0;
		FiberDebt** link = &r->debts;
		while (FiberDebt* d = *link) {
			if (d->holder != holder) { link = &d->next; continue; }
			*link = d->next;
			d->next = nullptr;
			if (d->release && d->obj) d->release(d->obj);
			++n;
		}
		return n;
	}

	bool FiberRegistry::AdvanceCleanup(TaskRecord* r) {
		if (!r) return false;
		for (;;) {
			// Next holder still owed something. "Any holder" debts are left for the global list.
			size_t holder = SIZE_MAX;
			for (FiberDebt* d = r->debts; d; d = d->next)
				if (d->holder != FiberDebt::kAnyHolder) { holder = d->holder; break; }

			if (holder == SIZE_MAX) {
				Recycler()(r);
				return false;
			}
			if (Dispatcher()(holder, r))
				return true;

			// That holder cannot be reached: release its debts here rather than leak them.
			DischargeDebts(r, holder);
		}
	}

	void FiberRegistry::CleanupHop(void* record) {
		if (TaskRecord* r = static_cast<TaskRecord*>(record))
			Instance().AdvanceCleanup(r);
	}

}
