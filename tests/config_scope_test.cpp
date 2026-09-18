// Two pools in one process, one after the other. The second must start from its own Config and
// see nothing the first pool set -- not its shape, and not a tunable changed while it ran.
#include <TaskScheduler.h>
#include <atomic>
#include <cstdio>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static std::atomic<int> g_ran{ 0 };
static void Body(void*) { g_ran.fetch_add(1, std::memory_order_relaxed); }

static bool RunSome(TaskScheduler& s) {
	g_ran = 0;
	WaitGroup wg;
	wg.n.store(256);
	for (int i = 0; i < 256; ++i) {
		Task* t = s.CreateTask(&Body, nullptr);
		t->waitGroup = &wg;
		s.Push(t);
	}
	s.WaitFor(wg);
	return g_ran.load() == 256;
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);

	std::printf("[first pool: K=2, 64 MB fiber limit, tunables changed while it runs]\n");
	{
		TaskScheduler::Config cfg;
		cfg.mode       = Mode::Pinned;
		cfg.main       = MainMode::OutOfPool;
		cfg.workers    = 4;
		cfg.hotWorkers = 2;
		cfg.fiberMemoryLimit = 64u << 20;
		cfg.tunables.leavesPerWorker = 3;
		TaskScheduler::Init(cfg);
		auto& s = TaskScheduler::Instance();
		Check(TaskScheduler::GetHotWorkers() == 2, "K is 2");
		Check(TaskScheduler::GetMode() == Mode::Pinned, "mode is Pinned");
		Check(TaskScheduler::FiberMemoryLimit() == (64u << 20), "fiber limit came from the config");
		Check(s.GetLeavesPerWorker() == 3, "a tunable's starting value came from the config");

		s.SetWakeCostNs(9999);
		s.SetParallelForSerial(true);
		s.SetLaneIntake(false);
		Check(s.GetWakeCostNs() == 9999 && s.ParallelForSerial() && !s.LaneIntakeEnabled(),
		      "tunables change while the pool runs");
		Check(RunSome(s), "the pool runs work");
		detail::DestroyForTesting();
	}

	std::printf("[second pool: a default Config]\n");
	{
		TaskScheduler::Config cfg;
		cfg.workers = 4;
		TaskScheduler::Init(cfg);
		auto& s = TaskScheduler::Instance();
		const TaskScheduler::Config def{};
		Check(TaskScheduler::GetHotWorkers() == 0, "K is back to 0");
		Check(TaskScheduler::GetMode() == Mode::Migrate, "mode is back to Migrate");
		Check(TaskScheduler::FiberMemoryLimit() == def.fiberMemoryLimit, "fiber limit is back to the default");
		Check(s.GetLeavesPerWorker() == def.tunables.leavesPerWorker, "leaves per worker is back to the default");
		Check(s.GetWakeCostNs() == def.tunables.wakeCostNs, "wake cost did not carry over");
		Check(!s.ParallelForSerial(), "pfor-serial did not carry over");
		Check(s.LaneIntakeEnabled(), "lane intake did not carry over");
		Check(RunSome(s), "the second pool runs work");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
