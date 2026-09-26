// The stats build: counts match what was run, histograms fill, Reset baselines.
// Build with -DJLIBSCHED_STATS=1 -DJLIBSCHED_COROUTINES=1 against C:\sds.
#include <TaskScheduler.h>
#include <Coroutine.h>
#include <Stats.h>
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

static void Nop(void*) {}
static void Suspender(void*) {
	auto& s = TaskScheduler::Instance();
	WaitGroup wg; wg.n.store(1);
	Task* t = s.CreateTask([] { std::this_thread::sleep_for(std::chrono::microseconds(200)); });
	t->waitGroup = &wg; s.Push(t);
	s.WaitFor(wg);
}
static Coro CoroYield() { co_await Reschedule{}; }

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	Check(Stats::Enabled(), "stats compiled in");
	TaskScheduler::Init(Mode::Migrate, MainMode::OutOfPool, 4);
	auto& s = TaskScheduler::Instance();

	// Warm up, then baseline: only the work below is counted.
	{ WaitGroup wg; wg.n.store(100); for (int i = 0; i < 100; ++i) { Task* t = s.CreateTask(&Nop, nullptr); t->waitGroup = &wg; s.Push(t); } s.WaitFor(wg); }
	Stats::Reset();

	constexpr int kLambda = 5000, kFiber = 3000, kSusp = 500, kCoro = 800;
	WaitGroup wg;
	wg.n.store(kLambda + kFiber + kSusp);
	for (int i = 0; i < kLambda; ++i) { Task* t = s.CreateTask([] {}); t->waitGroup = &wg; s.Push(t); }
	for (int i = 0; i < kFiber; ++i)  { Task* t = s.CreateTask(&Nop, nullptr); t->waitGroup = &wg; s.Push(t); }
	for (int i = 0; i < kSusp; ++i)   { Task* t = s.CreateTask(&Suspender, nullptr); t->waitGroup = &wg; s.Push(t); }
	WaitGroup cw;
	for (int i = 0; i < kCoro; ++i) Spawn(CoroYield(), &cw);
	s.WaitFor(wg);
	s.WaitFor(cw);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));   // let the last workers settle

	const StatsSnapshot snap = Stats::Snapshot();
	snap.Print();

	const uint64_t lambdas = snap.Count(Stat::RunLambda);
	const uint64_t fibers  = snap.Count(Stat::RunFiber);
	const uint64_t coros   = snap.Count(Stat::RunCoroutine);
	std::printf("  lambdas=%llu fibers=%llu coroutines=%llu\n",
		(unsigned long long)lambdas, (unsigned long long)fibers, (unsigned long long)coros);
	// Each suspender adds one lambda; each suspend adds one extra fiber run; each coroutine runs twice.
	Check(lambdas == (uint64_t)(kLambda + kSusp), "lambda runs = lambdas pushed");
	Check(fibers >= (uint64_t)(kFiber + kSusp) && fibers <= (uint64_t)(kFiber + 2 * kSusp), "fiber runs = fibers + resumes");
	Check(coros == (uint64_t)(2 * kCoro), "coroutine runs = two segments each");
	const uint64_t bySource = snap.Count(Stat::RunOwnDeque) + snap.Count(Stat::RunInbox) + snap.Count(Stat::RunHiPri)
	                        + snap.Count(Stat::RunStolen) + snap.Count(Stat::RunMainQueue) + snap.Count(Stat::RunInjector)
	                        + snap.Count(Stat::RunHelper);
	Check(bySource == lambdas + fibers + coros, "every run has exactly one source");
	Check(snap.Count(Stat::Suspends) >= (uint64_t)(kSusp + kCoro), "suspends counted (fibers and coroutines)");
	Check(snap.Count(Stat::Yields) == (uint64_t)kCoro, "coroutine yields counted");
	Check(snap.Samples(Hist::TaskLife) == (uint64_t)(kLambda + kFiber + 2 * kSusp + kCoro), "one task-life sample per finished task");
	Check(snap.Samples(Hist::Suspended) >= (uint64_t)(kSusp + kCoro), "one suspended sample per resume");
	Check(snap.Samples(Hist::WaitFor) >= (uint64_t)kSusp, "WaitFor timed");
	const double p50us = snap.TicksToMicros(snap.Percentile(Hist::Suspended, 0.5));
	std::printf("  suspended p50 ~ %.1f us (the waited task sleeps 200 us)\n", p50us);
	Check(snap.ticksPerSecond > 1e6, "clock calibrated");

	Stats::Reset();
	const StatsSnapshot empty = Stats::Snapshot();
	Check(empty.Count(Stat::RunLambda) == 0 && empty.Samples(Hist::TaskLife) == 0, "Reset zeroes the view");

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::_Exit(g_fail ? 1 : 0);
}
