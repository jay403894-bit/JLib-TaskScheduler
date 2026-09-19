// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <memory>
#include <thread>
#include <vector>
#include <mutex>
#include <cstdio>    
#include <cassert>   
#include <cstdlib>   
#include "Task.h"
#include "Stats.h"
#include "platform.h"   
#include "Reclaimer.h"
#include "Hazard.h"

namespace JLib {
	struct EpochParticipant {
		std::atomic<size_t> localEpoch{ SIZE_MAX };
	};
	using DeleterFunc = void(*)(void*);
	struct LNodeBase;
	struct LMarkableReference;
	struct SNMarkableReference;
	struct SNodeBase;
	struct DelayedTask;
	struct PeriodicTask;
	extern thread_local size_t thread_id;   // written by the pool; read through CurrentThreadId()
	JLIB_NOINLINE size_t CurrentThreadId() noexcept;
	// thread_id of a thread with no epoch slot: not main, not a pool thread, no ThreadScope.
	inline constexpr size_t kNoThreadSlot = SIZE_MAX;

	namespace detail {
		struct EpochRetired {
			void*  ptr;
			size_t epoch;
			void (*deleter)(void*);
		};

		inline constexpr size_t kPublishAt = 64;   // bag growth between gates

		struct EpochOrphanStore {
			std::mutex mtx;
			std::vector<EpochRetired> items;
		};
		inline EpochOrphanStore* const g_epochOrphans = new EpochOrphanStore();

		inline std::atomic<size_t> g_epochOrphanCount{ 0 };

		struct EpochRetireBatch {
			std::vector<EpochRetired> items;
			~EpochRetireBatch();
		};
		// Per-thread retire state (Epochs.cpp), behind out-of-line accessors -- see JLIB_NOINLINE.
		JLIB_NOINLINE EpochRetireBatch& EpochBag() noexcept;
		JLIB_NOINLINE bool EpochBagDead() noexcept;     // this thread's bag was destroyed (thread exit)
		// Next bag size that triggers a gate on the retire path.
		JLIB_NOINLINE size_t& BagLimit() noexcept;
		// Set on slotted threads (main, pool workers), which keep a bag and clear it at their gates.
		JLIB_NOINLINE bool& RunsGates() noexcept;

		inline void OrphanEpochEntries(std::vector<EpochRetired>& v) {
			if (v.empty()) return;
			std::lock_guard<std::mutex> lk(g_epochOrphans->mtx);
			for (auto& e : v) g_epochOrphans->items.push_back(e);
			g_epochOrphanCount.store(g_epochOrphans->items.size(), std::memory_order_release);
			v.clear();
		}


		inline size_t SweepEpochOrphans(size_t safeEpoch) {
			std::lock_guard<std::mutex> lk(g_epochOrphans->mtx);
			auto& v = g_epochOrphans->items;
			size_t kept = 0, freed = 0;
			for (size_t i = 0; i < v.size(); ++i) {
				if (v[i].epoch < safeEpoch) { v[i].deleter(v[i].ptr); ++freed; }
				else                        { v[kept++] = v[i]; }
			}
			v.resize(kept);
			g_epochOrphanCount.store(v.size(), std::memory_order_release);
			return freed;
		}
	}

	class EpochManager {
	private:
		std::vector<std::atomic<size_t>*> participants;
	
		struct GlobalRetired {
			void* ptr;        
			size_t epoch;     
			void (*deleter)(void*);
		};
		
		std::atomic<size_t> retiredCount{ 0 };     

		std::atomic<size_t> devRetiredNeverSwept{ 0 };
		std::atomic<bool>   devNoTickWarned{ false };

		std::atomic<size_t> globalEpoch{ 0 };


		struct ThreadEpoch {
			std::atomic<size_t> localEpoch{ SIZE_MAX };   
		};
		std::vector<ThreadEpoch*> threadEpochs;

		size_t externalBase_  = 0;
		size_t externalCount_ = 0;
		std::unique_ptr<std::atomic<bool>[]> externalClaimed_;

		EpochManager() = default;
	public:
		EpochManager(const EpochManager&) = delete;
		EpochManager& operator=(const EpochManager&) = delete;
		~EpochManager() {
			for (auto* te : threadEpochs) {
				delete te;
			}
			threadEpochs.clear();
		}
		
		int ParticipantCount() const { return (int)participants.size(); }

		void RegisterParticipant(std::atomic<size_t>* slot) {
		
			participants.push_back(slot);
		}

		static EpochManager& Instance() {
			
			static EpochManager* mgr = new EpochManager();
			return *mgr;
		}
		void Tick()
		{
			AdvanceEpoch();
			TryReclaim();
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
			
			devRetiredNeverSwept.store(0, std::memory_order_relaxed);
#endif
		}
	
		// poolSlots: main + workers (ids 0..poolSlots-1). externalSlots: ids after them, claimed and
		// released by ThreadScope; idle ones read as SIZE_MAX and never hold back reclamation.
		void Init(size_t poolSlots, size_t externalSlots = 0)
		{
			const size_t maxThreads = poolSlots + externalSlots;
			externalBase_  = poolSlots;
			externalCount_ = externalSlots;
			externalClaimed_.reset(externalSlots ? new std::atomic<bool>[externalSlots] : nullptr);
			for (size_t i = 0; i < externalSlots; ++i)
				externalClaimed_[i].store(false, std::memory_order_relaxed);

			const size_t had = threadEpochs.size();
			if (maxThreads > had) {
				threadEpochs.resize(maxThreads);
				for (size_t i = had; i < maxThreads; ++i) threadEpochs[i] = new ThreadEpoch();
			}

			participants.clear();
			for (size_t i = 0; i < maxThreads; ++i) {
				
				threadEpochs[i]->localEpoch.store(SIZE_MAX, std::memory_order_relaxed);
				RegisterParticipant(&threadEpochs[i]->localEpoch);
			}
		}
		
		// A free external slot's id, or kNoThreadSlot if all are taken.
		size_t ClaimExternalSlot() noexcept {
			for (size_t i = 0; i < externalCount_; ++i) {
				bool expected = false;
				if (externalClaimed_[i].compare_exchange_strong(expected, true,
						std::memory_order_acq_rel, std::memory_order_relaxed))
					return externalBase_ + i;
			}
			return kNoThreadSlot;
		}
		void ReleaseExternalSlot(size_t tid) noexcept {
			if (tid >= externalBase_ && tid - externalBase_ < externalCount_)
				externalClaimed_[tid - externalBase_].store(false, std::memory_order_release);
		}
		size_t ExternalSlotCount() const noexcept { return externalCount_; }

		// nullptr for a thread with no slot (kNoThreadSlot).
		std::atomic<size_t>* ThreadSlot(size_t tid) {
			if (tid == kNoThreadSlot) return nullptr;

#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
			if (tid >= threadEpochs.size()) {
				std::fprintf(stderr,
					"[JLib::Scheduler] FATAL: epoch slot %zu requested but only %zu exist. "
					"A thread is using an epoch guard without a reserved slot -- see thread_id's "
					"assignment in Thread::StartWorker and TaskScheduler::StartPool.\n",
					tid, threadEpochs.size());
				std::fflush(stderr);
				assert(false && "epoch slot index out of range -- see stderr");
			}
#endif
			return &threadEpochs[tid]->localEpoch;
		}
		
		// Reclaims this thread's bag; with orphans set, also sweeps the shared orphan store.
		void TryReclaim(bool orphans = true) {
			auto& bag = detail::EpochBag();
			if (bag.items.empty()
			    && (!orphans || detail::g_epochOrphanCount.load(std::memory_order_relaxed) == 0))
				return;

			const size_t safeEpoch = MinActiveEpoch();
			size_t freed = 0;

			// Work on a detached copy: a deleter may retire more into this thread's bag.
			std::vector<detail::EpochRetired> work;
			work.swap(bag.items);
			size_t kept = 0;
			for (size_t i = 0; i < work.size(); ++i) {
				if (work[i].epoch < safeEpoch) {
					work[i].deleter(work[i].ptr);
					++freed;
				} else {
					work[kept++] = work[i];
				}
			}
			work.resize(kept);
			if (bag.items.empty()) bag.items.swap(work);
			else bag.items.insert(bag.items.end(), work.begin(), work.end());

			if (orphans && detail::g_epochOrphanCount.load(std::memory_order_acquire) != 0)
				freed += detail::SweepEpochOrphans(safeEpoch);

			if (freed) retiredCount.fetch_sub(freed, std::memory_order_relaxed);
		}
		
		size_t RetiredCount() const { return retiredCount.load(std::memory_order_relaxed); }

		// A reclaim pass ran somewhere (Gates mode has no Tick); re-arms the never-swept warning.
		void NoteSwept() {
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
			devRetiredNeverSwept.store(0, std::memory_order_relaxed);
#endif
		}

		size_t ReclaimThreshold() const {
			constexpr size_t kFloor = 512;
			const size_t p = participants.size();
			return p > kFloor ? p : kFloor;
		}
		
		size_t CurrentEpoch() { return globalEpoch.load(std::memory_order_seq_cst); }
		size_t MinActiveEpoch() {
			size_t minEpoch = globalEpoch.load(std::memory_order_seq_cst);
			
			for (auto* slot : participants) {
				size_t e = slot->load(std::memory_order_seq_cst);
				if (e != SIZE_MAX && e < minEpoch) minEpoch = e;
			}

			return minEpoch;
		}
	
		template<typename T>
		void RetirePtr(T* p, size_t epoch, DeleterFunc d) {
			
			// Only slotted threads (main, pool workers) keep a bag. A thread without a slot has no
			// sweep point; it must be given a thread id. Release builds orphan the retire so it is
			// still freed.
			const bool unslotted = !detail::RunsGates();
			assert(!unslotted && "retire from a thread with no epoch slot -- give it a thread id");
			if (detail::EpochBagDead() || unslotted) {
				std::vector<detail::EpochRetired> one{ detail::EpochRetired{ (void*)p, epoch, d } };
				detail::OrphanEpochEntries(one);
			} else {
				auto& bag = detail::EpochBag();
				bag.items.push_back(detail::EpochRetired{ (void*)p, epoch, d });
				// Bag limit: reclaim this thread's bag (the gate re-arms the limit past whatever is
				// still protected).
				if (bag.items.size() >= detail::BagLimit()) Reclaimer::Gate(false, false);
			}
			
			retiredCount.fetch_add(1, std::memory_order_relaxed);
			JLIB_STAT(Retired);
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
			
			{
				const size_t n = devRetiredNeverSwept.fetch_add(1, std::memory_order_relaxed) + 1;
				if (n > 100000 && !devNoTickWarned.exchange(true, std::memory_order_relaxed)) {
					std::fprintf(stderr,
						"[JLib::Scheduler] %zu pointers retired and no reclaim gate has run. Every\n"
						"  slotted thread reclaims its own bag at its bag limit and when it goes idle,\n"
						"  so seeing this is a scheduler bug (or the pool was never started).\n"
						"  This warning prints once.\n", n);
					std::fflush(stderr);
				}
			}
#endif
		}

	private:
		
	public:
		
		bool AdvanceEpoch() {
			size_t e = globalEpoch.load(std::memory_order_acquire);
			return globalEpoch.compare_exchange_strong(e, e + 1, std::memory_order_seq_cst,
			                                           std::memory_order_relaxed);
		}

	};
};

namespace JLib {
	
	using EpochSuspendViolationFn = void (*)();
	inline std::atomic<EpochSuspendViolationFn> g_epochSuspendViolation{ nullptr };
	inline void SetEpochSuspendViolationHandlerForTest(EpochSuspendViolationFn fn) {
		g_epochSuspendViolation.store(fn, std::memory_order_relaxed);
	}

	inline void EpochGuardSuspendCheck(const char* where) {
		// Hazard guards use the thread's row, so they may not span a suspension either.
		if (const std::size_t d = HazardDomain::SuspendUnsafeDepth())
			HazardDomain::FatalSuspendWithGuard(d);
		std::atomic<size_t>* slot = EpochManager::Instance().ThreadSlot(CurrentThreadId());
		if (!slot) return;
		if (slot->load(std::memory_order_acquire) == SIZE_MAX) return;   
		if (EpochSuspendViolationFn h = g_epochSuspendViolation.load(std::memory_order_relaxed)) {
			h();
			return;
		}
		fprintf(stderr,
			"[JLib::Scheduler] INVARIANT VIOLATED: suspended inside an EpochGuard at %s.\n"
			"  Every reader uses its THREAD's epoch slot. This guard announced on the thread it\n"
			"  started on, and will be destructed on whichever thread resumes it -- clearing a slot\n"
			"  that was never set there, which un-announces a live traversal and frees nodes\n"
			"  underneath it.\n"
			"  Fix: end the guarded traversal and let the EpochGuard destruct BEFORE waiting.\n"
			"  If it genuinely must span the suspension, use a HazardGuard -- hazard cells are\n"
			"  indexed by the reader and survive a park; epochs do not, and that is why both\n"
			"  schemes exist.\n",
			where);
		fflush(stderr);
		std::abort();
	}
}

#define JLIB_EPOCH_GUARD_ENTER()        ((void)0)
#define JLIB_EPOCH_GUARD_LEAVE()        ((void)0)
#define JLIB_EPOCH_CHECK_NO_GUARD(where) JLib::EpochGuardSuspendCheck(where)
namespace JLib {
	
	inline void CoroEpochGuardSuspendCheck() { EpochGuardSuspendCheck("co_await"); }

	inline std::atomic<bool> g_exitGuardWarned{ false };
	inline void EpochGuardExitCheck(const char* where) {
		std::atomic<size_t>* slot = EpochManager::Instance().ThreadSlot(CurrentThreadId());
		if (!slot) return;
		if (slot->load(std::memory_order_acquire) == SIZE_MAX) return;
		bool expected = false;
		if (!g_exitGuardWarned.compare_exchange_strong(expected, true,
				std::memory_order_acq_rel, std::memory_order_relaxed))
			return;                                  
		fprintf(stderr,
			"[JLib::Scheduler] NOTE: a task returned at %s with this thread's epoch slot still\n"
			"  announced. Either the task leaked an EpochGuard -- in which case reclamation stalls\n"
			"  from here on -- or an OUTER guard on this thread is legitimately still live\n"
			"  (TaskDAG::ForEachDependent holds one while it fires dependents). This warns once and\n"
			"  does not abort, because those two cases are indistinguishable from one slot read.\n",
			where);
		fflush(stderr);
	}

	inline void SetCoroSuspendViolationHandlerForTest(EpochSuspendViolationFn fn) {
		SetEpochSuspendViolationHandlerForTest(fn);
	}
}
#define JLIB_EPOCH_CHECK_NO_GUARD_CORO() JLib::CoroEpochGuardSuspendCheck()

#define JLIB_EPOCH_CHECK_NO_GUARD_AT_EXIT(where) JLib::EpochGuardExitCheck(where)

struct SlotEpochGuard {
	std::atomic<size_t>* slot;
	bool outermost;   // only the outermost guard pins and unpins; nested guards reuse its pin

	SlotEpochGuard(std::atomic<size_t>* s)
		: slot(s), outermost(s->load(std::memory_order_relaxed) == SIZE_MAX) {
		if (outermost)
			slot->store(JLib::EpochManager::Instance().CurrentEpoch(),
				std::memory_order_seq_cst);
		JLIB_EPOCH_GUARD_ENTER();
	}

	~SlotEpochGuard() {
		if (outermost)
			slot->store(SIZE_MAX, std::memory_order_release);
		JLIB_EPOCH_GUARD_LEAVE();
	}
};
