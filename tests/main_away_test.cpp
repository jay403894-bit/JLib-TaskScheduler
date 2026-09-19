// MainAway: main in the pool, about to block in its own code, leaves the round-robin and moves its
// normal inbox onto its deque, so nothing pushed to it waits for it. Model:
// tests/verify/mainaway_model.c.
//
// 1. Tasks pushed while main is in its own code (not away) partly land in inbox 0. MainAway then
//    drains them: all of them finish while main keeps sleeping.
// 2. Tasks pushed while main is away finish while main keeps sleeping.
// 3. A main-only task pushed while main is away does not run until main is back, and runs on main.
// Before 1, the same pushes without MainAway are reported (not scored): some wait for main.
#include <TaskScheduler.h>
#include <Thread.h>

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

static std::atomic<int> g_done{ 0 };
static void Body(void*) { g_done.fetch_add(1, std::memory_order_relaxed); }

static std::atomic<int>  g_mainRan{ 0 };
static std::atomic<bool> g_mainOnMain{ false };
static std::thread::id   g_mainId;
static void MainBody(void*) {
	g_mainOnMain.store(std::this_thread::get_id() == g_mainId);
	g_mainRan.fetch_add(1);
}

// Pushes n tasks from a thread outside the pool, so the round-robin (not main's own deque
// shortcut) decides where they go.
static void PushFromOutside(int n) {
	std::thread([n] {
		auto& s = TaskScheduler::Instance();
		for (int i = 0; i < n; ++i) s.Push(s.CreateTask(&Body, nullptr));
	}).join();
}

// Main sleeps in its own code until `target` tasks are done or `ms` pass; returns whether they did.
static bool SleepUntilDone(int target, int ms) {
	const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
	while (g_done.load() < target) {
		if (std::chrono::steady_clock::now() > end) return false;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	TaskScheduler::Config cfg;
	cfg.workers = 5;
	cfg.main    = MainMode::InPool;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	g_mainId = std::this_thread::get_id();
	constexpr int N = 64;

	// Contrast: without MainAway, whatever landed in inbox 0 waits for main.
	g_done = 0;
	PushFromOutside(N);
	SleepUntilDone(N, 300);
	std::printf("  without MainAway: %d of %d ran while main slept 300 ms (the rest wait in inbox 0)\n",
		g_done.load(), N);
	{   // main back in the pool: it takes its own leftovers
		const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (g_done.load() < N && std::chrono::steady_clock::now() < end) TaskScheduler::MainWorker();
	}

	// 1 + 2 + 3.
	g_done = 0;
	PushFromOutside(N);                     // some land in inbox 0 while main is present
	bool before = false, during = false;
	int mainRanWhileAway = 0;
	{
		TaskScheduler::MainAway away;       // drains inbox 0 onto deque 0
		before = SleepUntilDone(N, 5000);
		PushFromOutside(N);
		Task* m = s.CreateTask(&MainBody, nullptr);
		s.PushMain(m);
		during = SleepUntilDone(2 * N, 5000);
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		mainRanWhileAway = g_mainRan.load();
	}
	Check(before, "tasks pushed before MainAway all ran while main was away");
	Check(during, "tasks pushed during MainAway all ran while main was away");
	Check(mainRanWhileAway == 0, "the main-only task waited for main");

	// Back in the pool: main runs its own work.
	const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (g_mainRan.load() == 0 && std::chrono::steady_clock::now() < end) TaskScheduler::MainWorker();
	Check(g_mainRan.load() == 1 && g_mainOnMain.load(), "after MainAway the main-only task ran, on main");

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
