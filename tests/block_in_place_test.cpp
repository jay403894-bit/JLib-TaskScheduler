// BlockInPlace on a worker: the slot is busy until the call returns and nobody stands in for it.
// What BlockBegin does is stop the stall from hiding work -- it unloads the normal inbox onto the
// deque (stealable) and marks the thread away so unplaced pushes pick someone else.
//
// 1. A task on worker W queues work into its own inbox, then blocks for 300 ms: that work runs
//    meanwhile (unloaded to the deque), and unplaced pushes run meanwhile too (they skip W).
// 2. Five of six workers block at once: every task still runs exactly once.
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

// Queues work into its OWN inbox, then blocks: BlockBegin must unload that inbox onto the deque,
// where other workers steal it. Without the unload it would sit unstealable for the whole block.
static constexpr int kPreQueued = 32;
static void BlockerBody(void*) {
	auto& s = TaskScheduler::Instance();
	g_blockerQ.store(Thread::GetCurrent()->qIndex);
	Task* ts[kPreQueued];
	for (int i = 0; i < kPreQueued; ++i) ts[i] = s.CreateTask(&Body, nullptr);
	s.PushBatch(ts, kPreQueued, (size_t)Thread::GetCurrent()->qIndex, 0);
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

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
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
		g_done = 0;
		s.Push(s.CreateTask(&BlockerBody, nullptr));
		const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!g_blocking.load() && std::chrono::steady_clock::now() < end) std::this_thread::sleep_for(std::chrono::milliseconds(1));
		const int w = g_blockerQ.load();

		// What it had queued when it left is on its deque now: other workers steal and run it.
		const bool queuedRan = Until(g_done, kPreQueued, 250) && !g_blockerBack.load();
		std::printf("  worker %d blocked; %d of %d tasks it had queued ran meanwhile\n",
			w, g_done.load(), kPreQueued);
		Check(queuedRan, "work queued before the block was unloaded to the deque and stolen");

		// Unplaced pushes skip an away thread, so they run while it is still blocked.
		constexpr int kUnplaced = 128;
		g_done = 0;
		for (int i = 0; i < kUnplaced; ++i) s.Push(s.CreateTask(&Body, nullptr));
		const bool unplacedRan = Until(g_done, kUnplaced, 250) && !g_blockerBack.load();
		std::printf("  %d of %d unplaced tasks ran while it was blocked\n", g_done.load(), kUnplaced);
		Check(unplacedRan, "unplaced work goes to live threads while one is away");

		const auto end2 = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!g_blockerBack.load() && std::chrono::steady_clock::now() < end2) std::this_thread::sleep_for(std::chrono::milliseconds(1));
		Check(g_blockerBack.load(), "the blocker returned (nested BlockInPlace included)");
	}

	// 2. Five of six workers block at once.
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
