// Config::pumpMain: main's message pump, called by the scheduler while main waits in the pool.
//
//   in    MainMode::InPool   -- the pump runs while main helps, only on main, with the configured
//                               budget, and never re-entrantly (a pump that itself calls WaitFor
//                               must not be pumped again from inside that wait)
//   out   MainMode::OutOfPool -- the pump never runs: main is the app's own message thread there
//
// One mode per process, chosen by argv[1] ("in" by default).
#include <TaskScheduler.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

constexpr std::uint32_t kBudgetUs = 777;
constexpr int kTasks = 4000;

static std::thread::id g_mainId;
static std::atomic<int> g_pumps{ 0 }, g_offMain{ 0 }, g_wrongBudget{ 0 };
static std::atomic<int> g_depth{ 0 }, g_maxDepth{ 0 }, g_nestedWaits{ 0 };
static std::atomic<int> g_done{ 0 };

static void Work(void*) {
	volatile unsigned x = 0; for (unsigned k = 0; k < 20000; ++k) x += k;
	g_done++;
}

static void Pump(std::uint32_t budgetUs) {
	if (std::this_thread::get_id() != g_mainId) g_offMain++;
	if (budgetUs != kBudgetUs) g_wrongBudget++;
	const int d = ++g_depth;
	int m = g_maxDepth.load();
	while (d > m && !g_maxDepth.compare_exchange_weak(m, d)) {}
	// Once, act like a window procedure that waits on the pool: this lands main back in the
	// worker loop, which must not call the pump again while this call is still on the stack.
	if (++g_pumps == 5) {
		auto& s = TaskScheduler::Instance();
		WaitGroup wg; wg.n.store(64);
		for (int i = 0; i < 64; ++i) {
			Task* t = s.CreateTask(&Work, nullptr, TaskType::Fiber);
			t->waitGroup = &wg; s.Push(t);
		}
		s.WaitFor(wg);
		g_nestedWaits++;
	}
	--g_depth;
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const bool inPool = !(argc > 1 && std::strcmp(argv[1], "out") == 0);
	g_mainId = std::this_thread::get_id();

	TaskScheduler::Config cfg;
	cfg.mode             = Mode::Migrate;
	cfg.main             = inPool ? MainMode::InPool : MainMode::OutOfPool;
	cfg.workers          = 4;
	cfg.pumpMain         = &Pump;
	cfg.pumpMainBudgetUs = kBudgetUs;
	cfg.pumpMainEvery    = 8;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	std::printf("pump_main_test main=%s workers=%zu\n", inPool ? "inpool" : "outofpool", s.GetWorkerCount());

	std::thread([] {
		std::this_thread::sleep_for(std::chrono::seconds(30));
		std::printf("  WATCHDOG: hung (pumps=%d done=%d depth=%d)\n",
		            g_pumps.load(), g_done.load(), g_depth.load());
		std::fflush(stdout);
		std::_Exit(7);
	}).detach();

	WaitGroup wg; wg.n.store(kTasks);
	for (int i = 0; i < kTasks; ++i) {
		Task* t = s.CreateTask(&Work, nullptr, TaskType::Fiber);
		t->waitGroup = &wg; s.Push(t);
	}
	s.WaitFor(wg);

	std::printf("  pumps=%d offMain=%d maxDepth=%d nestedWaits=%d done=%d\n",
	            g_pumps.load(), g_offMain.load(), g_maxDepth.load(), g_nestedWaits.load(), g_done.load());
	if (inPool) {
		Check(g_pumps.load() > 0,       "the pump ran while main waited in the pool");
		Check(g_offMain.load() == 0,    "the pump only ever ran on main");
		Check(g_wrongBudget.load() == 0,"the pump got the configured budget");
		Check(g_maxDepth.load() == 1,   "the pump never ran inside itself");
		Check(g_pumps.load() < 5 || g_nestedWaits.load() == 1,
		      "a pump that waits on the pool returns (the nested wait finished)");
	} else {
		Check(g_pumps.load() == 0,      "out of the pool, the pump never ran");
	}
	Check(g_done.load() >= kTasks,      "all the work ran");

	detail::TeardownForTesting(s);
	std::printf("RESULT: %s\n", g_fail ? "FAILURES" : "all checks passed");
	return g_fail ? 1 : 0;
}
