// BlockInPlace on a worker: its normal inbox is adopted by a free worker while it blocks in code
// that cannot suspend. Model: tests/verify/adopt_model.c.
//
// 1. A task on worker W blocks for 300 ms; batches pushed straight into W's inbox all run while
//    W is still blocked (the adopter drains them).
// 2. More blockers than free adopters: every task still runs exactly once (the ones behind a
//    blocker with no adopter run after it returns).
// 3. Nested BlockInPlace is fine.
// 4. (child) BlockInPlace inside an EpochGuard is fatal.
// 5. A native task (CreateNativeTask) runs with no fiber and may BlockInPlace.
// 6. (child) A native task that suspends (WaitFor) is fatal.
#include <TaskScheduler.h>
#include <Thread.h>
#include "spawn_self.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static std::atomic<int> g_done{ 0 };
static void Body(void*) { g_done.fetch_add(1, std::memory_order_relaxed); }

static bool Until(std::atomic<int>& v, int target, int ms) {
	const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
	while (v.load() < target) {
		if (std::chrono::steady_clock::now() > end) return false;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}

static std::atomic<int>  g_blockerQ{ -1 };
static std::atomic<bool> g_blocking{ false };
static std::atomic<bool> g_blockerBack{ false };
static int               g_blockMs = 300;

static void BlockerBody(void*) {
	g_blockerQ.store(Thread::GetCurrent()->qIndex);
	TaskScheduler::BlockInPlace([] {
		g_blocking.store(true);
		std::this_thread::sleep_for(std::chrono::milliseconds(g_blockMs));
		TaskScheduler::BlockInPlace([] {});   // nested: a no-op inside the outer block
	});
	g_blockerBack.store(true);
}

static std::atomic<int> g_manyBack{ 0 };
static void ManyBlocker(void*) {
	TaskScheduler::BlockInPlace([] { std::this_thread::sleep_for(std::chrono::milliseconds(200)); });
	g_manyBack.fetch_add(1);
}

// Mode "s": two workers and one spare. Worker B is busy computing, worker A blocks; tasks pushed
// into A's inbox must run at once -- only the spare is free to take them. Without a spare they
// would wait for B.
static std::atomic<bool> g_busyStarted{ false }, g_busyDone{ false };
static void BusyBody(void*) {
	g_busyStarted.store(true);
	const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(400);
	while (std::chrono::steady_clock::now() < end) {}
	g_busyDone.store(true);
}
// spares == 0 is the control: the same setup must NOT be fast (the tasks wait for worker B).
static int SpareMode(size_t spares) {
	TaskScheduler::Config cfg;
	cfg.workers = 2;
	cfg.main    = MainMode::OutOfPool;
	cfg.spareThreads = spares;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	g_blockMs = 400;

	Task* busy = s.CreateTask(&BusyBody, nullptr);
	Task* blocker = s.CreateTask(&BlockerBody, nullptr);
	s.PushBatch(&busy, 1, (size_t)1, 0);
	s.PushBatch(&blocker, 1, (size_t)0, 0);
	const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while ((!g_blocking.load() || !g_busyStarted.load()) && std::chrono::steady_clock::now() < end)
		std::this_thread::sleep_for(std::chrono::milliseconds(1));

	constexpr int kTasks = 32;
	g_done = 0;
	Task* ts[kTasks];
	for (int i = 0; i < kTasks; ++i) ts[i] = s.CreateTask(&Body, nullptr);
	s.PushBatch(ts, kTasks, (size_t)g_blockerQ.load(), 0);
	const bool fast = Until(g_done, kTasks, 150) && !g_busyDone.load() && !g_blockerBack.load();
	std::printf("  both workers occupied, %zu spare(s); %d of %d tasks ran within 150 ms\n", spares, g_done.load(), kTasks);
	if (spares) Check(fast, "with a spare, a blocked worker's inbox runs while every worker is occupied");
	else        Check(!fast, "control: with no spare the same tasks wait for a worker (the test can tell)");
	Until(g_done, kTasks, 5000);
	while (!g_blockerBack.load() || !g_busyDone.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	if (argc > 1 && std::strcmp(argv[1], "s") == 0)  return SpareMode(1);
	if (argc > 1 && std::strcmp(argv[1], "s0") == 0) return SpareMode(0);
	TaskScheduler::Config cfg;
	cfg.workers = 6;
	cfg.main    = MainMode::OutOfPool;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();

	if (argc > 1 && std::strcmp(argv[1], "child-guard") == 0) {
		WaitGroup wg;
		wg.n.store(1);
		Task* t = s.CreateTask([](void* p) {
			{ EpochGuard g; TaskScheduler::BlockInPlace([] {}); }
			static_cast<WaitGroup*>(p)->Done();
		}, &wg);
		s.Push(t);
		s.WaitFor(wg);
		std::printf("CHILD DID NOT ABORT\n");
		return 0;
	}
	if (argc > 1 && std::strcmp(argv[1], "child-native-wait") == 0) {
		WaitGroup wg;
		wg.n.store(1);
		Task* t = s.CreateNativeTask([](void* p) {
			WaitGroup inner;
			inner.n.store(1);
			TaskScheduler::Instance().WaitFor(inner);   // suspends: fatal in a native task
			static_cast<WaitGroup*>(p)->Done();
		}, &wg);
		s.Push(t);
		s.WaitFor(wg);
		std::printf("CHILD DID NOT ABORT\n");
		return 0;
	}

	// 1 + 3.
	{
		s.Push(s.CreateTask(&BlockerBody, nullptr));
		const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!g_blocking.load() && std::chrono::steady_clock::now() < end) std::this_thread::sleep_for(std::chrono::milliseconds(1));
		const int w = g_blockerQ.load();
		constexpr int kBatches = 16, kPer = 8;
		g_done = 0;
		for (int b = 0; b < kBatches; ++b) {
			Task* ts[kPer];
			for (int i = 0; i < kPer; ++i) ts[i] = s.CreateTask(&Body, nullptr);
			s.PushBatch(ts, kPer, (size_t)w, 0);   // straight into the blocked worker's inbox
		}
		const bool ranWhileBlocked = Until(g_done, kBatches * kPer, 250) && !g_blockerBack.load();
		std::printf("  worker %d blocked; %d of %d tasks pushed to its inbox ran meanwhile\n",
			w, g_done.load(), kBatches * kPer);
		Check(ranWhileBlocked, "tasks pushed into a blocked worker's inbox ran while it was blocked");
		Until(g_done, kBatches * kPer, 5000);
		const auto end2 = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!g_blockerBack.load() && std::chrono::steady_clock::now() < end2) std::this_thread::sleep_for(std::chrono::milliseconds(1));
		Check(g_blockerBack.load(), "the blocker returned (nested BlockInPlace included)");
	}

	// 2. Five of six workers block at once: at most half can be adopted.
	{
		constexpr int kBlockers = 5, kTasks = 400;
		for (int i = 0; i < kBlockers; ++i) s.Push(s.CreateTask(&ManyBlocker, nullptr));
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		g_done = 0;
		for (int i = 0; i < kTasks; ++i) s.Push(s.CreateTask(&Body, nullptr));
		const bool all = Until(g_done, kTasks, 10000);
		Until(g_manyBack, kBlockers, 10000);
		std::printf("  %d of %d tasks ran with %d of 6 workers blocked\n", g_done.load(), kTasks, kBlockers);
		Check(all && g_done.load() == kTasks, "with most of the pool blocked, every task still ran exactly once");
	}

	// 4.
	{
		const JLibTest::ChildResult r = JLibTest::RunSelf("child-guard", argv[0]);
		Check(r.started && r.aborted, "BlockInPlace inside an EpochGuard is fatal");
	}

	// 5.
	{
		constexpr int kNative = 16;
		static std::atomic<int> onFiber{ 0 }, blocked{ 0 };
		WaitGroup wg;
		wg.n.store(kNative);
		for (int i = 0; i < kNative; ++i) {
			s.Push(s.CreateNativeTask([](void* p) {
				if (TaskScheduler::Instance().IsOnFiber()) onFiber.fetch_add(1);
				TaskScheduler::BlockInPlace([] { std::this_thread::sleep_for(std::chrono::milliseconds(20)); });
				blocked.fetch_add(1);
				static_cast<WaitGroup*>(p)->Done();
			}, &wg));
		}
		s.WaitFor(wg);
		std::printf("  %d native tasks: %d blocked in place, %d ran on a fiber\n", kNative, blocked.load(), onFiber.load());
		Check(blocked.load() == kNative && onFiber.load() == 0, "native tasks run with no fiber and may BlockInPlace");

		// Control: the same body as a plain fn+ctx task does run on a fiber (the probe can tell).
		static std::atomic<int> fiberSeen{ 0 };
		WaitGroup wg2;
		wg2.n.store(1);
		s.Push(s.CreateTask([](void* p) {
			if (TaskScheduler::Instance().IsOnFiber()) fiberSeen.fetch_add(1);
			static_cast<WaitGroup*>(p)->Done();
		}, &wg2));
		s.WaitFor(wg2);
		Check(fiberSeen.load() == 1, "control: a CreateTask(fn, ctx) task does run on a fiber");
	}

	// 6.
	{
		const JLibTest::ChildResult r = JLibTest::RunSelf("child-native-wait", argv[0]);
		Check(r.started && r.aborted, "a native task that suspends is fatal");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
