// TaskRecord: task-local storage and release-on-death debts. Arg 'p' = Pin mode.
// (Holder-affine debts and the record cleanup hop were removed 2026-09-19: nothing in the library
// ever created one, and the holder queues they fed cost three checks in the park gate.)
#include <TaskScheduler.h>
#include <TaskLocal.h>
#include <Thread.h>
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
	std::fflush(stdout);
	if (!ok) ++g_fail;
}
static int CurQ() { Thread* w = Thread::GetCurrent(); return w ? w->qIndex : -1; }

static bool WaitFor(const std::atomic<int>& c, int want, int ms) {
	const auto t0 = std::chrono::steady_clock::now();
	while (c.load() < want && std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(ms))
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	return c.load() >= want;
}

// ---- 1. task-local storage ----
static uint16_t g_slot = 0xFFFF;
static std::atomic<int> g_tlsDeleted{ 0 }, g_tlsBad{ 0 }, g_tlsOk{ 0 }, g_tlsMigrated{ 0 };
static void TlsDeleter(void* p) { delete static_cast<int*>(p); g_tlsDeleted.fetch_add(1); }
static void TlsBody(void* arg) {
	const int id = (int)(intptr_t)arg;
	TaskLocalSet(g_slot, new int(id));
	const int q0 = CurQ();
	for (int i = 0; i < 3; ++i) {
		Thread::Yield();
		int* p = static_cast<int*>(TaskLocalGet(g_slot));
		if (!p || *p != id) { g_tlsBad.fetch_add(1); return; }
	}
	if (CurQ() != q0) g_tlsMigrated.fetch_add(1);
	g_tlsOk.fetch_add(1);
}


// ---- 3. release on death (any holder) ----
static std::atomic<int> g_anyReleased{ 0 };
static void AnyRelease(void*) noexcept { g_anyReleased.fetch_add(1); }
static void AnyBody(void* p) {
	TaskScheduler::ReleaseOnTaskDeath(*static_cast<TaskDebt*>(p), p, &AnyRelease);
}

int main(int argc, char** argv) {
	const bool pin = argc > 1 && std::strchr(argv[1], 'p');
	TaskScheduler::Config cfg;
	cfg.mode    = pin ? Mode::Pinned : Mode::Migrate;
	cfg.main    = MainMode::OutOfPool;
	cfg.workers = 4;
	cfg.fibers.normalPerComputeWorker = 512;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	const int n = (int)s.GetWorkerCount();
	std::printf("record_test mode=%s workers=%d\n", pin ? "Pin" : "Migrate", n);

	std::printf("[task-local storage follows the task]\n");
	{
		g_slot = AllocTaskLocalSlot();
		SetTaskLocalDeleter(g_slot, &TlsDeleter);
		const int N = 2000;
		WaitGroup wg; wg.n.store(N);
		for (int i = 0; i < N; ++i) {
			Task* t = s.CreateTask(&TlsBody, (void*)(intptr_t)i, Lane::Normal, TaskType::Fiber);
			t->waitGroup = &wg;
			s.Push(t);
		}
		s.WaitFor(wg);
		WaitFor(g_tlsDeleted, N, 3000);
		std::printf("  ok=%d bad=%d migrated=%d deleted=%d\n", g_tlsOk.load(), g_tlsBad.load(), g_tlsMigrated.load(), g_tlsDeleted.load());
		Check(g_tlsOk.load() == N && g_tlsBad.load() == 0, "value survived every yield");
		Check(g_tlsDeleted.load() == N, "slot deleter ran once per task");
		if (pin) Check(g_tlsMigrated.load() == 0, "Pin: no task changed worker");
	}

	std::printf("[release-on-death debts]\n");
	{
		const int N = 2000;
		std::vector<TaskDebt> nodes(N);
		WaitGroup wg; wg.n.store(N);
		for (int i = 0; i < N; ++i) {
			Task* t = s.CreateTask(&AnyBody, &nodes[i], Lane::Normal, TaskType::Fiber);
			t->waitGroup = &wg;
			s.Push(t);
		}
		s.WaitFor(wg);
		const bool all = WaitFor(g_anyReleased, N, 5000);
		std::printf("  released=%d\n", g_anyReleased.load());
		Check(all, "every release-on-death debt was released");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);   // skip teardown: the debt contexts above are on this stack
}
