// PushBatch Wide / Narrow.
#include <TaskScheduler.h>
#include <Thread.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static constexpr int kWorkers = 8;
static std::atomic<int> g_ranOn[kWorkers];
static std::atomic<int> g_ran{ 0 };

static void Work(void*) {
	if (Thread* t = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers()); t && t->qIndex >= 0) g_ranOn[t->qIndex].fetch_add(1, std::memory_order_relaxed);
	g_ran.fetch_add(1, std::memory_order_relaxed);
	std::this_thread::sleep_for(std::chrono::microseconds(200));   // slow enough to be stolen
}

static int Run(TaskScheduler::BatchSpread spread, int n) {
	auto& s = TaskScheduler::Instance();
	for (auto& c : g_ranOn) c.store(0);
	g_ran.store(0);
	WaitGroup wg; wg.n.store(n);
	std::vector<Task*> ts(n);
	for (int i = 0; i < n; ++i) { ts[i] = s.CreateTask(&Work, nullptr); ts[i]->waitGroup = &wg; }
	s.PushBatch(ts.data(), (size_t)n, spread);
	s.WaitFor(wg);
	int used = 0;
	std::printf("  per worker:");
	for (int q = 0; q < kWorkers; ++q) { std::printf(" %d", g_ranOn[q].load()); if (g_ranOn[q]) ++used; }
	std::printf("\n");
	return used;
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	TaskScheduler::Init(kWorkers);
	constexpr int kN = 2000;

	std::printf("[Narrow: one inbox, one wake, spread by stealing]\n");
	int used = Run(TaskScheduler::BatchSpread::Narrow, kN);
	Check(g_ran == kN, "every task ran exactly once");
	Check(used >= kWorkers / 2, "the pool stole it off the one worker");

	std::printf("[Wide: split across inboxes]\n");
	used = Run(TaskScheduler::BatchSpread::Wide, kN);
	Check(g_ran == kN, "every task ran exactly once");
	Check(used >= kWorkers / 2, "several workers ran it");

	std::printf("[small batches]\n");
	Run(TaskScheduler::BatchSpread::Narrow, 1);  Check(g_ran == 1, "Narrow, 1 task");
	Run(TaskScheduler::BatchSpread::Wide, 3);    Check(g_ran == 3, "Wide, 3 tasks");

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::_Exit(g_fail ? 1 : 0);
}
