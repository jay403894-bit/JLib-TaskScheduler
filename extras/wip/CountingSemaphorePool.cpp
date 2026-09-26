// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "CountingSemaphorePool.h"

#include <algorithm>
#include <chrono>
#if defined(_MSC_VER)
#include <immintrin.h>   
#endif

namespace JLib {

struct CountingSemaphorePool::Worker {
	csp::TaskDeque deque;
	csp::Inbox     inbox;
	size_t          index = 0;

	size_t          victim = 0;
	int             stickyRun = 0;

	size_t          lapMisses = 0;

	bool            advertised = false;

	bool            blinked = false;
};

static constexpr int kStickyCap = 4;

static inline unsigned PopCount(uint64_t v) noexcept {
	unsigned c = 0;
	for (; v; v &= (v - 1)) ++c;
	return c;
}   

CountingSemaphorePool::CountingSemaphorePool() = default;
CountingSemaphorePool::~CountingSemaphorePool() { Stop(); }

void CountingSemaphorePool::Start(size_t workers, double permitFrac, std::chrono::nanoseconds stagger) {
	if (running_.load(std::memory_order_acquire)) return;

	size_t n = workers;
	if (n == 0) {
		const unsigned hw = std::thread::hardware_concurrency();
		n = (hw > 1) ? (size_t)(hw - 1) : 1;
	}
	if (n > 64) n = 64;   

	workers_.clear();
	workers_.reserve(n);
	for (size_t i = 0; i < n; ++i) {
		auto w = std::unique_ptr<Worker>(new Worker());
		w->index = i;
		w->victim = (i + 1) % n;   
		workers_.push_back(std::move(w));
	}

	(void)permitFrac;
	wait_.Init((int)n, stagger);

	running_.store(true, std::memory_order_release);
	threads_.reserve(n);
	for (size_t i = 0; i < n; ++i)
		threads_.emplace_back([this, i] { WorkerLoop(i); });
}

void CountingSemaphorePool::Stop() {
	if (!running_.exchange(false, std::memory_order_acq_rel)) return;
	
	wait_.ReleaseAll((int)workers_.size() * 4);
	for (auto& t : threads_) if (t.joinable()) t.join();
	threads_.clear();
	workers_.clear();
}

void CountingSemaphorePool::Push(csp::Task* task) {
	if (!task || workers_.empty()) return;

	const unsigned long long ticket = nextTarget_.fetch_add(1, std::memory_order_relaxed);
	int dest = wait_.PickDest(ticket);
	if (dest < 0 || (size_t)dest >= workers_.size()) dest = 0;
	workers_[(size_t)dest]->inbox.push(task);
}

void CountingSemaphorePool::PushWide(csp::Task** tasks, size_t count) {
	if (!tasks || count == 0 || workers_.empty()) return;

	const size_t n = workers_.size();
	const int seats = JLib::SeatSemaphore::kMinSeats;
	size_t width = count;
	if (width > n) width = n;

	for (size_t i = 0; i < count; ++i) {
		const unsigned long long ticket = nextTarget_.fetch_add(1, std::memory_order_relaxed);
		int dest = wait_.PickDest(ticket);
		if (dest < 0 || (size_t)dest >= n) dest = 0;
		workers_[(size_t)dest]->inbox.push(tasks[i]);
	}

	if ((int)width > seats) {
		const int parked = wait_.Sleepers();
		int k = (int)width - seats;
		if (k > parked) k = parked;
		if (k > 0) wait_.TryGrow(k);
	}
}

namespace {
	struct ForState {
		std::atomic<int> live{ 0 };
		void (*body)(void*, int, int) = nullptr;
		void*            ctx  = nullptr;
	};
	struct Block {
		csp::Task task;
		ForState*     st = nullptr;
		int           lo = 0, hi = 0;
	};
	void BlockRunner(void* p) noexcept {
		auto* b = static_cast<Block*>(p);
		b->st->body(b->st->ctx, b->lo, b->hi);
		b->st->live.fetch_sub(1, std::memory_order_acq_rel);
	}
}   

void CountingSemaphorePool::GrowWide(int extra) {
	if (extra <= 0) return;
	const int parked = wait_.Sleepers();
	if (extra > parked) extra = parked;
	if (extra <= 0) return;

	const unsigned before = wait_.DestCount();
	wait_.Release(extra);
	const unsigned want = before + (unsigned)extra;

	const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(300);
	while (wait_.DestCount() < want) {
		if (std::chrono::steady_clock::now() >= deadline) return;
		std::this_thread::yield();
	}
}

void CountingSemaphorePool::HelpUntil(std::atomic<int>& live) {
	while (live.load(std::memory_order_acquire) > 0) {
		bool ran = false;
		const uint64_t adv = wait_.Advertised();
		for (size_t i = 0; i < workers_.size() && adv; ++i) {
			if ((adv & (1ull << i)) == 0) continue;
			auto got = workers_[i]->deque.steal();
			if (got) { (*got)->Execute(); executed_.fetch_add(1, std::memory_order_relaxed); ran = true; break; }
		}
		if (!ran) std::this_thread::yield();
	}
}

void CountingSemaphorePool::ParallelFor(int begin, int end, int grain,
                              void (*body)(void* ctx, int lo, int hi), void* ctx) {
	if (!body || end <= begin) return;
	if (grain < 1) grain = 1;
	const int N = end - begin;
	if (workers_.empty() || N <= grain) { body(ctx, begin, end); return; }

	int want = (N + grain - 1) / grain;
	if (want > (int)workers_.size()) want = (int)workers_.size();
	if (want < 1) want = 1;

	ForState st;
	st.body = body;
	st.ctx  = ctx;
	st.live.store(0, std::memory_order_relaxed);

	wait_.WideEnter();
	{
		const int extra = want - (int)wait_.DestCount();
		if (extra > 0) GrowWide(extra);
	}

	int nChunks = (int)wait_.DestCount();
	if (nChunks < 1)  nChunks = 1;
	if (nChunks > 64) nChunks = 64;

	const int even = (N + nChunks - 1) / nChunks;
	const int use  = (even < grain) ? grain : even;   

	Block blocks[64];
	int lo = begin;
	for (int i = 0; i < nChunks && lo < end; ++i) {
		int hi = lo + use;
		if (hi > end) hi = end;
		blocks[i].st = &st;
		blocks[i].lo = lo;
		blocks[i].hi = hi;
		blocks[i].task.fn  = &BlockRunner;
		blocks[i].task.ctx = &blocks[i];
		st.live.fetch_add(1, std::memory_order_relaxed);
		Push(&blocks[i].task);        
		lo = hi;
	}
	
	if (lo < end) body(ctx, lo, end);

	HelpUntil(st.live);
	wait_.WideLeave();
}

void CountingSemaphorePool::WorkerLoop(size_t index) {
	Worker& self = *workers_[index];
	const size_t n = workers_.size();

	wait_.NoteAwake();   

	while (running_.load(std::memory_order_acquire)) {
		
		{
			csp::Task* t = nullptr;
			while (self.inbox.pop(t)) {
				if (!self.deque.push_bottom(t)) { t->Execute(); executed_.fetch_add(1, std::memory_order_relaxed); }
			}
		}

		{
			const bool has = !self.deque.empty();
			if (has != self.advertised) {
				if (has) wait_.Advertise((int)index); else wait_.Unadvertise((int)index);
				self.advertised = has;
			}
		}

		bool didWork = false;
		{
			for (;;) {
				auto got = self.deque.pop_bottom();
				if (!got) break;
				(*got)->Execute();
				executed_.fetch_add(1, std::memory_order_relaxed);
				didWork = true;
			}
		}

		if (self.advertised && self.deque.empty()) {
			wait_.Unadvertise((int)index);
			self.advertised = false;
		}

		if (!didWork && n > 1) {
			
			const uint64_t adv = wait_.Advertised() & ~(1ull << index);
			if (adv == 0) {
				self.lapMisses += n;   
			}
			else if ((adv & (1ull << self.victim)) == 0) {
				++self.lapMisses;
				do { self.victim = (self.victim + 1) % n; } while (self.victim == index);
			}
			else {
			auto got = workers_[self.victim]->deque.steal();
			if (got) {
				(*got)->Execute();
				executed_.fetch_add(1, std::memory_order_relaxed);
				steals_.fetch_add(1, std::memory_order_relaxed);
				didWork = true;
				self.lapMisses = 0;   
				if (++self.stickyRun >= kStickyCap) { self.stickyRun = 0; self.victim = (self.victim + 1) % n; }
			}
			else {
				stealMiss_.fetch_add(1, std::memory_order_relaxed);
				self.stickyRun = 0;
				++self.lapMisses;   
				do { self.victim = (self.victim + 1) % n; } while (self.victim == index);
			}
			}
		}

		if (!didWork) {
			if (!self.inbox.empty() || !self.deque.empty()) continue;
			
			if (wait_.IsSeat((int)index)) {
				if (self.lapMisses >= n && wait_.Sleepers() > 0 && wait_.ArmHandoff((int)index)) {
					self.lapMisses = 0;
					wait_.Release(1);   
				}
				continue;
			}
			if (!wait_.MayReap()) continue;          

			wait_.RetireDest((int)index);

			if (self.lapMisses < n) continue;
			self.lapMisses = 0;

			if (wait_.WideLive()) continue;

			if (!self.inbox.empty() || !self.deque.empty()) continue;

			parks_.fetch_add(1, std::memory_order_relaxed);
			wait_.NoteAsleep();
			wait_.Wait(running_);
			wait_.NoteAwake();
			if (!running_.load(std::memory_order_acquire)) break;

			wait_.AddDest((int)index);
			self.lapMisses = 0;

			(void)wait_.SignalReady();
			
			(void)wait_.ClaimSeat((int)index);
		}
	}
	wait_.NoteAsleep();
}

}   
