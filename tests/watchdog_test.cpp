// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// The watchdog: the observer's optional companion (Config::watchdog), which runs hooks the
// application pushes so it can look at live pool state -- including while the pool is stalled.
//
// The claims:
//   off by default -- no thread, and Push refuses
//   a pushed hook runs, on a thread that is neither a worker nor the observer
//   it still runs when every worker is blocked, which is the only time it is worth having
//   it holds no scheduling authority: the hook reads, the pool is unaffected
#include <TaskScheduler.h>
#include <Thread.h>
#include <Observer.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

using Clock = std::chrono::steady_clock;
template <class F>
static bool WaitUntil(F pred, int limitMs) {
	const auto t0 = Clock::now();
	while (!pred()) {
		if (std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count() > limitMs)
			return false;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}

// What a real hook looks like: reads live structures, writes only its own context.
struct Probe {
	std::atomic<int>  ran{ 0 };
	std::atomic<bool> sawWorkers{ false };
	std::atomic<bool> onAWorker{ true };   // must end up false: hooks do not run on the pool
};

static void ProbeHook(void* p) noexcept {
	Probe& pr = *static_cast<Probe*>(p);
	// Live state, not a counter someone incremented for us.
	pr.sawWorkers.store(TaskScheduler::GetWorkers().size() > 0, std::memory_order_relaxed);
	pr.onAWorker.store(TaskScheduler::SelfWorker(TaskScheduler::GetWorkers()) != nullptr,
	                   std::memory_order_relaxed);
	pr.ran.fetch_add(1, std::memory_order_release);
}

// Occupies every worker so the pool cannot make progress while the hook runs.
static std::atomic<int>  g_blocked{ 0 };
static std::atomic<bool> g_release{ false };
static void BlockBody(void*) {
	g_blocked.fetch_add(1, std::memory_order_release);
	while (!g_release.load(std::memory_order_acquire))
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	std::printf("watchdog_test\n");

	// 1. Off by default: no thread, and a push is refused rather than silently dropped.
	std::printf("[off unless asked for]\n");
	{
		TaskScheduler::Config cfg;
		cfg.mode = Mode::Migrate;
		cfg.main = MainMode::OutOfPool;
		TaskScheduler::Init(cfg);
		TaskScheduler& s = TaskScheduler::Instance();

		Check(!Watchdog::Running(), "no watchdog thread without Config::watchdog");
		Probe pr;
		Watchdog::HookEntry e; e.fn = &ProbeHook; e.ctx = &pr;
		Check(!Watchdog::Push(&e), "Push refuses when the watchdog is not running");
		Check(pr.ran.load() == 0, "and nothing ran");

		detail::TeardownForTesting(s);
		detail::DestroyForTesting();
	}

	// 2. Enabled: the hook runs, off the pool.
	std::printf("[a pushed hook runs, and not on a worker]\n");
	{
		TaskScheduler::Config cfg;
		cfg.mode     = Mode::Migrate;
		cfg.main     = MainMode::OutOfPool;
		cfg.watchdog = true;
		TaskScheduler::Init(cfg);
		TaskScheduler& s = TaskScheduler::Instance();
		const size_t nw = s.GetWorkerCount();
		std::printf("  workers=%zu\n", nw);

		Check(Watchdog::Running(), "Config::watchdog started the thread");

		Probe pr;
		Watchdog::HookEntry e; e.fn = &ProbeHook; e.ctx = &pr;
		Check(Watchdog::Push(&e), "Push accepted");
		Check(WaitUntil([&] { return pr.ran.load(std::memory_order_acquire) == 1; }, 5000),
		      "the hook ran");
		Check(pr.sawWorkers.load(), "the hook read live pool state");
		Check(!pr.onAWorker.load(), "it ran on neither a worker nor main");

		// 3. The point of the thing: it runs while every worker is stuck.
		std::printf("[it runs while every worker is blocked]\n");
		// PushTo, not Push: a plain push from outside the pool round-robins, so some workers get
		// two and some get none and the pool never fully occupies.
		for (size_t i = 0; i < nw; ++i)
			s.PushTo((uint16_t)i, s.CreateTask(&BlockBody, nullptr, TaskType::Fiber));
		const bool allBlocked =
			WaitUntil([&] { return (size_t)g_blocked.load(std::memory_order_acquire) == nw; }, 10000);
		if (!allBlocked)
			std::printf("    only %d of %zu workers occupied\n", g_blocked.load(), nw);
		Check(allBlocked, "every worker is occupied and cannot progress");

		Probe stalled;
		Watchdog::HookEntry e2; e2.fn = &ProbeHook; e2.ctx = &stalled;
		Check(Watchdog::Push(&e2), "Push accepted with the pool stalled");
		Check(WaitUntil([&] { return stalled.ran.load(std::memory_order_acquire) == 1; }, 5000),
		      "the hook still ran with no worker available");
		Check(stalled.sawWorkers.load(), "and could still read pool state");

		g_release.store(true, std::memory_order_release);
		Check(Watchdog::Ran() >= 2, "both hooks are accounted for");

		detail::TeardownForTesting(s);
		detail::DestroyForTesting();
		Check(!Watchdog::Running(), "teardown stopped the watchdog");
	}

	std::printf("RESULT: %s\n", g_fail == 0 ? "all checks passed" : "FAILURES");
	return g_fail == 0 ? 0 : 1;
}
