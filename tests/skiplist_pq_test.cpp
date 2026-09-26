// extras/wip/SkipListPQ.h (the Linden-Jonsson priority queue) under load.
//
// 1. Sequential: drain order is priority, then insertion order among equal priorities.
// 2. Concurrent insert + deleteMin: every inserted item is popped exactly once (during the run or
//    by the final drain); a double pop or a lost item fails.
// 3. Concurrent deleteMin only, from a filled queue: each thread's pops strictly increase, which a
//    linearizable queue with no concurrent inserts must give.
// Each phase runs at boundOffset 1 (a batch cut on every pop) and 32. The model is
// tests/verify/skiplist_pq_model.c; x86 cannot exercise the orderings, this checks the port.
#include <TaskScheduler.h>
#include <Reclaimer.h>
#include "../extras/wip/SkipListPQ.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

struct Item {
	static constexpr uint32_t kMagic = 0x5CA1AB1E;
	uint64_t id    = 0;
	uint64_t order = 0;   // insertion order, for the FIFO checks
	uint32_t prio  = 0;
	uint32_t magic = kMagic;
};

using PQ = SkipListPQ<Item>;

static inline uint32_t NextRand(uint32_t& s) {
	s ^= s << 13; s ^= s >> 17; s ^= s << 5;
	return s;
}

static bool Before(const Item& a, const Item& b) {   // strict (prio, order)
	return a.prio < b.prio || (a.prio == b.prio && a.order < b.order);
}

static bool WaitFor(std::atomic<int>& v, int target, int ms) {
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
	while (v.load(std::memory_order_acquire) < target) {
		if (std::chrono::steady_clock::now() > deadline) return false;
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
	}
	return true;
}

// ---- 1. sequential order --------------------------------------------------------------------
static void Sequential(std::size_t bound) {
	PQ q(bound);
	constexpr int N = 5000;
	uint32_t s = 12345;
	for (int i = 0; i < N; ++i) {
		Item it;
		it.id = (uint64_t)i; it.order = (uint64_t)i; it.prio = NextRand(s) % 8;
		q.insert(it.prio, it);
	}
	Item prev, cur;
	int n = 0;
	bool ordered = true, magic = true;
	while (q.deleteMin(cur)) {
		if (cur.magic != Item::kMagic) magic = false;
		if (n > 0 && !Before(prev, cur)) ordered = false;
		prev = cur;
		++n;
	}
	Check(n == N, "sequential: drain returns every item");
	Check(ordered, "sequential: priority order, FIFO among equal priorities");
	Check(magic, "sequential: no corrupted payload");
}

// ---- 2. concurrent insert + deleteMin ---------------------------------------------------------
static constexpr int kTasks = 16;
static constexpr int kOps   = 20000;

struct MixCtx {
	PQ* q;
	int task;
	std::atomic<uint8_t>* popCount;
	std::atomic<long long>* inserted;
	std::atomic<long long>* badMagic;
	std::atomic<int>* done;
};

static void MixBody(void* p) {
	MixCtx& c = *static_cast<MixCtx*>(p);
	uint32_t s = 0x9E3779B9u + 2654435761u * (uint32_t)(c.task + 1);
	long long ins = 0;
	for (int i = 0; i < kOps; ++i) {
		if (NextRand(s) & 1) {
			Item it;
			it.id = (uint64_t)c.task * kOps + (uint64_t)i;
			it.prio = NextRand(s) % 16;
			c.q->insert(it.prio, it);
			++ins;
		} else {
			Item got;
			if (c.q->deleteMin(got)) {
				if (got.magic != Item::kMagic || got.id >= (uint64_t)kTasks * kOps)
					c.badMagic->fetch_add(1, std::memory_order_relaxed);
				else
					c.popCount[got.id].fetch_add(1, std::memory_order_relaxed);
			}
		}
	}
	c.inserted->fetch_add(ins, std::memory_order_relaxed);
	c.done->fetch_add(1, std::memory_order_release);
}

static void Mixed(TaskScheduler& sched, std::size_t bound) {
	PQ q(bound);
	const std::size_t ids = (std::size_t)kTasks * kOps;
	std::unique_ptr<std::atomic<uint8_t>[]> popCount(new std::atomic<uint8_t>[ids]);
	for (std::size_t i = 0; i < ids; ++i) popCount[i].store(0, std::memory_order_relaxed);
	std::atomic<long long> inserted{ 0 }, badMagic{ 0 };
	std::atomic<int> done{ 0 };

	MixCtx ctx[kTasks];
	for (int t = 0; t < kTasks; ++t) {
		ctx[t] = MixCtx{ &q, t, popCount.get(), &inserted, &badMagic, &done };
		sched.Push(sched.CreateTask(&MixBody, &ctx[t]));
	}
	Check(WaitFor(done, kTasks, 120000), "mixed: all tasks finished");

	Item got;
	long long drained = 0;
	while (q.deleteMin(got)) {
		if (got.magic != Item::kMagic || got.id >= ids) badMagic.fetch_add(1);
		else popCount[got.id].fetch_add(1, std::memory_order_relaxed);
		++drained;
	}
	// Recompute which ids were inserted: same RNG sequence as MixBody.
	long long once = 0, twice = 0, lost = 0, phantom = 0;
	for (int t = 0; t < kTasks; ++t) {
		uint32_t s = 0x9E3779B9u + 2654435761u * (uint32_t)(t + 1);
		for (int i = 0; i < kOps; ++i) {
			const bool isInsert = (NextRand(s) & 1) != 0;
			const std::size_t id = (std::size_t)t * kOps + (std::size_t)i;
			const uint8_t c = popCount[id].load(std::memory_order_relaxed);
			if (isInsert) {
				(void)NextRand(s);
				if (c == 1) ++once; else if (c == 0) ++lost; else ++twice;
			} else if (c != 0) {
				++phantom;
			}
		}
	}
	std::printf("    inserted %lld, drained at end %lld; once %lld, lost %lld, double %lld, phantom %lld\n",
		inserted.load(), drained, once, lost, twice, phantom);
	Check(badMagic.load() == 0, "mixed: no corrupted payload");
	Check(twice == 0, "mixed: no item popped twice");
	Check(lost == 0, "mixed: no item lost");
	Check(phantom == 0 && once == inserted.load(), "mixed: popped exactly the inserted items");
}

// ---- 3. concurrent deleteMin only -------------------------------------------------------------
struct DrainCtx {
	PQ* q;
	std::atomic<long long>* popped;
	std::atomic<long long>* outOfOrder;
	std::atomic<uint8_t>* popCount;
	std::atomic<int>* done;
};

static void DrainBody(void* p) {
	DrainCtx& c = *static_cast<DrainCtx*>(p);
	Item prev, cur;
	bool first = true;
	long long n = 0, bad = 0;
	while (c.q->deleteMin(cur)) {
		if (!first && !Before(prev, cur)) ++bad;
		c.popCount[cur.id].fetch_add(1, std::memory_order_relaxed);
		prev = cur; first = false; ++n;
	}
	c.popped->fetch_add(n, std::memory_order_relaxed);
	c.outOfOrder->fetch_add(bad, std::memory_order_relaxed);
	c.done->fetch_add(1, std::memory_order_release);
}

static void DeleteOnly(TaskScheduler& sched, std::size_t bound) {
	PQ q(bound);
	constexpr int N = 200000;
	std::unique_ptr<std::atomic<uint8_t>[]> popCount(new std::atomic<uint8_t>[N]);
	uint32_t s = 777;
	for (int i = 0; i < N; ++i) {
		popCount[i].store(0, std::memory_order_relaxed);
		Item it;
		it.id = (uint64_t)i; it.order = (uint64_t)i; it.prio = NextRand(s) % 64;
		q.insert(it.prio, it);
	}
	std::atomic<long long> popped{ 0 }, outOfOrder{ 0 };
	std::atomic<int> done{ 0 };
	DrainCtx ctx[kTasks];
	for (int t = 0; t < kTasks; ++t) {
		ctx[t] = DrainCtx{ &q, &popped, &outOfOrder, popCount.get(), &done };
		sched.Push(sched.CreateTask(&DrainBody, &ctx[t]));
	}
	Check(WaitFor(done, kTasks, 120000), "delete-only: all tasks finished");
	long long notOnce = 0;
	for (int i = 0; i < N; ++i) if (popCount[i].load() != 1) ++notOnce;
	std::printf("    popped %lld of %d, out of order %lld, not popped exactly once %lld\n",
		popped.load(), N, outOfOrder.load(), notOnce);
	Check(popped.load() == N && notOnce == 0, "delete-only: every item popped exactly once");
	Check(outOfOrder.load() == 0, "delete-only: each thread's pops strictly increase");
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	TaskScheduler::Config cfg;
	cfg.workers = 8;
	cfg.main    = MainMode::OutOfPool;
	TaskScheduler::Init(cfg);
	auto& sched = TaskScheduler::Instance();

	for (std::size_t bound : { std::size_t(1), std::size_t(32) }) {
		std::printf("boundOffset %zu\n", bound);
		Sequential(bound);
		Mixed(sched, bound);
		DeleteOnly(sched, bound);
		for (int i = 0; i < 16; ++i) Reclaimer::Flush();
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
