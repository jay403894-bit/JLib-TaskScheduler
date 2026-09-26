// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES MIGRATION LEAK? Not a leak in the C sense -- every block here is freed -- but retention:
// memory the allocator is holding and has not given back.
//
// mimalloc routes a cross-thread free to the OWNING heap's deferred list, and that list is only
// processed when the owning heap's thread allocates again. Before parked tasks could be stolen,
// `home` was stable: a task allocated and freed against one theap and this barely happened. Now a
// resume can be delivered by any worker, so a block allocated on A is routinely freed on B, and an
// A that goes idle never drains.
//
// This test does not assert a threshold. It reports four numbers so the question "does a collector
// thread earn its keep" has an answer instead of an argument:
//
//   baseline   before any work
//   peak       with the churn running
//   idle       after the pool has been quiet for a while, untouched
//   collected  after mi_collect has run ON EVERY WORKER
//
// idle == peak means nothing comes back on its own. collected << idle means a collector would
// help -- and because mi_collect only acts on the calling thread's heap, it says the collector has
// to be SCHEDULED ONTO each worker, not run on a thread of its own.
#include <TaskScheduler.h>
#include <Thread.h>
#include <Memory.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

using Clock = std::chrono::steady_clock;
static long long MsSince(Clock::time_point t0) {
	return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
}
template <class F>
static bool WaitUntil(F pred, int limitMs) {
	const auto t0 = Clock::now();
	while (!pred()) {
		if (MsSince(t0) > limitMs) return false;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}

static constexpr uint32_t kWake  = 1u;
static constexpr size_t   kBlock = 4096;   // big enough to matter, small enough to stay in bins

// argv[1]: how many blocks cross a park. It is a KNOB so the growth can be attributed: if commit
// tracks this count rather than the bytes allocated, what grew is the fiber pool (one checked-out
// stack per simultaneously-parked task), not anything the allocator is holding.
static int kParkers = 2000;

// One task that allocates, parks, and frees AFTER it comes back -- so the free happens on whatever
// worker delivered the resume, which is the whole point.
struct Crosser {
	std::atomic<Task*>      self{ nullptr };
	std::atomic<TaskHandle> handle{ TaskHandle{} };
	std::atomic<int>        done{ 0 };
	int                     parkedOnQ = -1;
	int                     wokeOnQ   = -1;
};

static std::atomic<int> g_migrated{ 0 };
static std::atomic<int> g_allocFail{ 0 };

// The sanctioned identity path: fiber -> owningTask -> home, validated against the workers array.
// Not Thread::GetCurrent() -- TLS names the thread executing the stack, which after a steal is not
// the thread that owns the theap this task allocates from.
static int SelfQ() {
	Thread* w = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
	return w ? w->qIndex : -1;
}

static void CrossBody(void* p) {
	Crosser& c = *static_cast<Crosser*>(p);

	Thread* home = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
	unsigned char* b = home ? static_cast<unsigned char*>(home->Alloc(kBlock)) : nullptr;
	if (!b) { g_allocFail.fetch_add(1, std::memory_order_relaxed); c.done.store(1); return; }
	std::memset(b, 0xA5, kBlock);

	c.parkedOnQ = SelfQ();
	c.self.store(TaskScheduler::Instance().GetCurrentTask(), std::memory_order_release);
	if (Fiber* f = FiberFromStack()) f->Suspend(Pin::None, 0);

	c.wokeOnQ = SelfQ();
	if (c.wokeOnQ != c.parkedOnQ) g_migrated.fetch_add(1, std::memory_order_relaxed);

	mi_free(b);   // on the waking worker, against the parking worker's heap
	c.done.store(1, std::memory_order_release);
}

// Keeps the owners too busy to sweep their own lots, which is what makes the parks stealable
// instead of locally delivered. Without this the test measures the no-migration path.
static std::atomic<bool> g_loadOn{ true };
static std::atomic<long long> g_loadRan{ 0 };
static void LoadBody(void*) {
	volatile double x = 1.0;
	for (int i = 0; i < 4000; ++i) x = x * 1.000001 + 0.5;
	g_loadRan.fetch_add(1, std::memory_order_relaxed);
	if (g_loadOn.load(std::memory_order_relaxed)) {
		TaskScheduler& s = TaskScheduler::Instance();
		s.Push(s.CreateTask(&LoadBody, nullptr, TaskType::Fiber));
	}
}

static void CollectBody(void*) { detail::MemoryCollect(true); }

static void Report(const char* label, const detail::Usage& u) {
	std::printf("    %-10s rss %7.1f MB   commit %7.1f MB\n", label,
	            u.currentRss / (1024.0 * 1024.0), u.currentCommit / (1024.0 * 1024.0));
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	if (argc > 1) {
		const int n = std::atoi(argv[1]);
		if (n > 0) kParkers = n;
	}

	TaskScheduler::Config cfg;
	cfg.mode = Mode::Migrate;
	cfg.main = MainMode::OutOfPool;   // main drives and wakes; it is not a worker here
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();
	const size_t nw = s.GetWorkerCount();
	std::printf("retention_test -- workers=%zu, %d parked blocks of %zu B\n", nw, kParkers, kBlock);

	const detail::Usage baseline = detail::MemoryUsageNow();

	// Load first, so the owners are already busy when the parkers arrive.
	std::printf("[churn: allocate, park, resume elsewhere, free]\n");
	for (size_t i = 0; i < nw * 2; ++i)
		s.Push(s.CreateTask(&LoadBody, nullptr, TaskType::Fiber));
	std::this_thread::sleep_for(std::chrono::milliseconds(200));

	std::vector<Crosser> cs(kParkers);
	for (int i = 0; i < kParkers; ++i)
		s.Push(s.CreateTask(&CrossBody, &cs[i], TaskType::Fiber));

	// Collect handles as they publish, then wake everyone from outside the pool.
	int armed = 0;
	for (int i = 0; i < kParkers; ++i) {
		Crosser& c = cs[i];
		if (!WaitUntil([&] { return c.self.load(std::memory_order_acquire) != nullptr; }, 20000))
			continue;
		Task* t = c.self.load(std::memory_order_acquire);
		if (!WaitUntil([&] {
				const TaskHandle h = t->record->handle.load(std::memory_order_acquire);
				if (!h.parked) return false;
				c.handle.store(h, std::memory_order_release);
				return true;
			}, 20000))
			continue;
		++armed;
	}
	std::printf("    %d of %d parked and addressable\n", armed, kParkers);

	const detail::Usage peak = detail::MemoryUsageNow();

	for (int i = 0; i < kParkers; ++i)
		if (cs[i].handle.load().parked) Thread::Kick(cs[i].handle.load(), kWake);

	int done = 0;
	WaitUntil([&] {
		done = 0;
		for (int i = 0; i < kParkers; ++i) done += cs[i].done.load(std::memory_order_acquire);
		return done == armed;
	}, 30000);
	Check(done == armed, "every parked task came back and freed its block");
	std::printf("    %d of %d resumed on a DIFFERENT worker than they parked on\n",
	            g_migrated.load(), armed);
	Check(g_allocFail.load() == 0, "no allocation failed");

	// Quiet the pool: the load tasks stop respawning and everything drains.
	g_loadOn.store(false, std::memory_order_relaxed);
	std::printf("[idle for 3 s, untouched]\n");
	std::this_thread::sleep_for(std::chrono::milliseconds(3000));
	const detail::Usage idle = detail::MemoryUsageNow();

	// Now the thing a collector would do -- ON each worker, because mi_collect only drains the
	// calling thread's heap. This is the measurement that decides the collector's SHAPE.
	std::printf("[mi_collect scheduled onto every worker]\n");
	{
		WaitGroup wg;
		wg.n.store((int)nw, std::memory_order_relaxed);
		for (size_t q = 0; q < nw; ++q) {
			Task* t = s.CreateTask(&CollectBody, nullptr, TaskType::Fiber);
			t->waitGroup = &wg;
			s.PushTo((uint16_t)q, t);
		}
		wg.BlockThread();   // main is out of the pool here, so this is a thread wait
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	const detail::Usage collected = detail::MemoryUsageNow();

	std::printf("[retention]\n");
	Report("baseline",  baseline);
	Report("peak",      peak);
	Report("idle",      idle);
	Report("collected", collected);

	// The scale check that keeps this test honest. Everything the ALLOCATOR could still be holding
	// from this workload is bounded by the bytes that passed through it. If the growth above is
	// orders of magnitude larger, it is not allocator retention -- it is the fiber pool, which grew
	// to hold every simultaneously-parked task and does not shrink (a priced trade, not garbage).
	std::printf("    blocks that crossed a park: %.1f MB -- anything far above this is not the allocator\n",
	            (double)kParkers * (double)kBlock / (1024.0 * 1024.0));

	const double idleHeldMB      = (double)idle.currentCommit      - (double)baseline.currentCommit;
	const double collectedHeldMB = (double)collected.currentCommit - (double)baseline.currentCommit;
	std::printf("    held over baseline: idle %.1f MB, after collect %.1f MB\n",
	            idleHeldMB / (1024.0 * 1024.0), collectedHeldMB / (1024.0 * 1024.0));
	if (idleHeldMB > 0)
		std::printf("    a scheduled collect returned %.1f%% of what idling did not\n",
		            100.0 * (idleHeldMB - collectedHeldMB) / idleHeldMB);

	detail::TeardownForTesting(s);
	detail::DestroyForTesting();
	std::printf("RESULT: %s\n", g_fail == 0 ? "all checks passed" : "FAILURES");
	return g_fail == 0 ? 0 : 1;
}

