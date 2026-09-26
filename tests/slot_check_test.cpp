// Regular mode (no SetMainThread0): main owns epoch slot 0, worker q owns slot q+1,
// and work placed on queue q runs on worker q -- including q == 0.
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <atomic>
#include <cstdio>
#include <cstdint>
#include <cstring>

using namespace JLib;

static constexpr size_t kMax = 64;

struct Rec {
	std::atomic<int>    q{ -1 };
	std::atomic<size_t> tid{ SIZE_MAX };
	std::atomic<int>    ownPinned{ 0 };
	std::atomic<int>    mainPinned{ 0 };
};
static Rec g_rec[kMax];
static std::atomic<int> g_perWorker[kMax];
static std::atomic<int> g_onMain{ 0 };
static int g_fail = 0;

static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

// Runs on the worker it was pushed to. Records who ran it and which epoch slot it pins.
static void Targeted(void* p) {
	const size_t want = (size_t)(intptr_t)p;
	Thread* w = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
	Rec& r = g_rec[want];
	r.q.store(w ? w->qIndex : -2);
	r.tid.store(thread_id);
	EpochGuard g;
	EpochManager& em = EpochManager::Instance();
	r.ownPinned.store(em.ThreadSlot(thread_id)->load() != SIZE_MAX);
	r.mainPinned.store(em.ThreadSlot(0)->load() != SIZE_MAX);
}

static void Counted(void*) {
	Thread* w = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
	if (w && w->qIndex >= 0 && (size_t)w->qIndex < kMax) g_perWorker[w->qIndex].fetch_add(1);
	else g_onMain.fetch_add(1);   // main's helper stole it while waiting
	volatile unsigned x = 0;
	for (unsigned i = 0; i < 20000; ++i) x += i;   // enough work that the whole pool joins in
}

int main(int argc, char** argv) {
	TaskScheduler::Config cfg;
	cfg.mode       = Mode::Migrate;
	cfg.main       = MainMode::OutOfPool;
	cfg.workers    = 4;
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();
	const size_t n = s.GetWorkerCount();
	std::printf("main out of pool, workers=%zu, inPool=%d\n",
		n, (int)(TaskScheduler::GetMainMode() == MainMode::InPool));

	std::printf("[main]\n");
	Check(thread_id == 0, "main has epoch slot 0");
	{
		Thread* m = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
		Check(m && m->isHelper && !m->IsPoolWorker(), "main is its helper, not a pool worker, out of pool");
	}
	{
		EpochGuard g;
		Check(EpochManager::Instance().ThreadSlot(0)->load() != SIZE_MAX, "main's guard pins slot 0");
	}
	Check(EpochManager::Instance().ThreadSlot(0)->load() == SIZE_MAX, "slot 0 released after main's guard");

	std::printf("[targeted: one task per queue, PushTo(q)]\n");
	{
		WaitGroup wg;
		wg.n.store((int)n);
		for (size_t q = 0; q < n; ++q) {
			Task* t = s.CreateTask(&Targeted, (void*)(intptr_t)q, TaskType::Fiber);
			t->waitGroup = &wg;
			if (!s.PushTo(q, t)) { std::printf("  FAIL PushTo(%zu) refused\n", q); ++g_fail; wg.n.fetch_sub(1); }
		}
		s.WaitFor(wg);
		bool seen[kMax + 1] = {};
		for (size_t q = 0; q < n; ++q) {
			const int ranOn = g_rec[q].q.load();
			const size_t tid = g_rec[q].tid.load();
			std::printf("  queue %zu -> ran on worker %d, epoch slot %zu, own slot pinned=%d, slot0 pinned=%d\n",
				q, ranOn, tid, g_rec[q].ownPinned.load(), g_rec[q].mainPinned.load());
			if (tid <= kMax) seen[tid] = true;
			if (ranOn != (int)q) { std::printf("  FAIL queue %zu ran on worker %d\n", q, ranOn); ++g_fail; }
			if (tid != q + 1)    { std::printf("  FAIL worker %zu has slot %zu, want %zu\n", q, tid, q + 1); ++g_fail; }
			if (!g_rec[q].ownPinned.load())  { std::printf("  FAIL worker %zu guard did not pin its slot\n", q); ++g_fail; }
			if (g_rec[q].mainPinned.load())  { std::printf("  FAIL worker %zu guard touched main's slot 0\n", q); ++g_fail; }
		}
		bool exact = !seen[0];
		for (size_t i = 1; i <= n; ++i) exact = exact && seen[i];
		Check(exact, "worker slots are exactly 1..N, none is 0");
	}

	std::printf("[spread: 20000 tasks via Push]\n");
	{
		const int N = 20000;
		WaitGroup wg;
		wg.n.store(N);
		for (int i = 0; i < N; ++i) {
			Task* t = s.CreateTask(&Counted, nullptr, TaskType::Fiber);
			t->waitGroup = &wg;
			s.Push(t);
		}
		s.WaitFor(wg);
		int total = 0;
		bool allCompute = true;
		for (size_t q = 0; q < n; ++q) {
			const int c = g_perWorker[q].load();
			total += c;
			std::printf("  worker %zu ran %d\n", q, c);
			if (c == 0) allCompute = false;
		}
		std::printf("  main helper ran %d\n", g_onMain.load());
		Check(total + g_onMain.load() == N, "every task ran exactly once (workers + main's helper)");
		Check(g_perWorker[0].load() > 0, "worker 0 (queue 0) received work");
		Check(allCompute, "every compute worker received work");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	return g_fail ? 1 : 0;
}
