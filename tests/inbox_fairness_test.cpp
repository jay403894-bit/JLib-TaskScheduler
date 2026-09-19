// A worker whose own deque never empties (every task pushes its successor) must still run what is
// sent to its inbox: the fairness tick drains the inbox every TaskScheduler::kFairTickEvery passes.
// Without it (build with -DJLIB_INBOX_CTL_NO_TICK, the negative control) the inbox is read only
// when the deque is empty, and these tasks never run.
#include <TaskScheduler.h>
#include <Thread.h>
#include <Stats.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

using namespace JLib;

static std::atomic<bool> g_stop{ false };
static std::atomic<int>  g_chains{ 0 };
static std::atomic<int>  g_ran{ 0 };

static void Spin(int us) {
	const auto end = std::chrono::steady_clock::now() + std::chrono::microseconds(us);
	while (std::chrono::steady_clock::now() < end) {}
}

static void Chain(void*) {
	Spin(100);
	if (!g_stop.load(std::memory_order_relaxed)) {
		auto& s = TaskScheduler::Instance();
		s.Push(s.CreateTask(&Chain, nullptr));   // a worker's push: onto its own deque
	} else {
		g_chains.fetch_sub(1);
	}
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	TaskScheduler::Config cfg;
	cfg.main = MainMode::OutOfPool;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	const int W = (int)s.GetWorkerCount();

	g_chains = W;
	for (int i = 0; i < W; ++i) { Task* t = s.CreateTask(&Chain, nullptr); s.PushBatch(&t, 1, (size_t)i, 0); }
	std::this_thread::sleep_for(std::chrono::milliseconds(20));

	// Two per worker straight into its inbox, plus plain pushes from main (round-robin inboxes).
	constexpr int kPlain = 32;
	const int total = 2 * W + kPlain;
	for (int i = 0; i < W; ++i)
		for (int k = 0; k < 2; ++k) {
			Task* t = s.CreateTask([](void*) { g_ran.fetch_add(1); }, nullptr);
			s.PushBatch(&t, 1, (size_t)i, 0);
		}
	for (int i = 0; i < kPlain; ++i) s.Push(s.CreateTask([](void*) { g_ran.fetch_add(1); }, nullptr));

	const auto t0 = std::chrono::steady_clock::now();
	const auto deadline = t0 + std::chrono::seconds(3);
	auto nextReport = t0 + std::chrono::milliseconds(250);
	while (g_ran.load() < total && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		if (std::getenv("JLIB_TEST_VERBOSE") && std::chrono::steady_clock::now() > nextReport) {
			nextReport += std::chrono::milliseconds(250);
			std::printf("    t=%4lld ms  ran %d of %d\n", (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
			            std::chrono::steady_clock::now() - t0).count(), g_ran.load(), total);
		}
	}
	const int ran = g_ran.load();

	g_stop = true;
	while (g_chains.load() > 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));

	std::printf("  %d workers, every deque kept non-empty; %d of %d inbox tasks ran within 3 s\n", W, ran, total);
	if (Stats::Enabled()) Stats::Print(stdout);
	const bool ok = ran == total;
	std::printf("  %s tasks sent to a busy worker's inbox still run\n", ok ? "ok  " : "FAIL");
	std::printf(ok ? "RESULT: all checks passed\n" : "RESULT: 1 FAILURE(S)\n");
	std::fflush(stdout);
	std::_Exit(ok ? 0 : 1);
}
