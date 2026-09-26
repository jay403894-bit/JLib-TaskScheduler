// Growable fibers + the Treiber Event. Arg "l" = memory-limit run instead.
// Build with -DJLIBSCHED_COROUTINES=1 against the coroutine library for the coroutine checks.
#include <TaskScheduler.h>
#include <GlobalFiberPool.h>
#include <Thread.h>
#if defined(JLIBSCHED_COROUTINES)
#include <Coroutine.h>
#endif
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

using namespace JLib;
using Clock = std::chrono::steady_clock;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static std::atomic<int> g_in{ 0 }, g_done{ 0 }, g_cancelled{ 0 };
static void Reset() { g_in = 0; g_done = 0; g_cancelled = 0; }

// Polls until pred() or the deadline; calls step() each round.
template <class P, class S>
static bool Until(P pred, S step, int ms) {
	const auto end = Clock::now() + std::chrono::milliseconds(ms);
	while (!pred()) {
		if (Clock::now() > end) return false;
		step();
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}

static void Spawn(void (*fn)(void*), void* arg, int n, WaitGroup& wg) {
	auto& s = TaskScheduler::Instance();
	wg.n.fetch_add(n);
	for (int i = 0; i < n; ++i) { Task* t = s.CreateTask(fn, arg, TaskType::Fiber); t->waitGroup = &wg; s.Push(t); }
}

static WaitGroup* g_gate = nullptr;
static void GateBody(void*) { g_in++; TaskScheduler::Instance().WaitFor(*g_gate); g_done++; }

static Event g_ev;
static void EventBody(void*) { g_in++; TaskScheduler::Instance().WaitOnEvent(g_ev); g_done++; }
static void CancelBody(void*) {
	g_in++;
	if (TaskScheduler::Instance().WaitOnEventCancellable(g_ev) == WaitResult::Cancelled) g_cancelled++;
	g_done++;
}

static std::mutex g_directMtx;
static std::vector<DirectEvent*> g_direct;
static void DirectBody(void*) {
	g_in++;
	TaskScheduler::Instance().WaitOnEventDirectArmed([](DirectEvent* e) {
		std::lock_guard<std::mutex> l(g_directMtx); g_direct.push_back(e);
	});
	g_done++;
}
static void SignalDirect() {
	std::vector<DirectEvent*> v;
	{ std::lock_guard<std::mutex> l(g_directMtx); v.swap(g_direct); }
	for (DirectEvent* e : v) e->Signal();
}

#if defined(JLIBSCHED_COROUTINES)
static Coro CoroEvent() { g_in++; co_await WaitEventAsync(g_ev); g_done++; }
static Coro CoroDirect() {
	g_in++;
	co_await WaitDirectAsync([](DirectEvent* e) {
		std::lock_guard<std::mutex> l(g_directMtx); g_direct.push_back(e);
	});
	g_done++;
}
#endif

static constexpr int N = 6000;
static int GrowRun() {
	auto& s = TaskScheduler::Instance();
	auto& pool = s.GetGlobalPool();
	const size_t start = pool.CountOf(StackClass::Standard);
	std::printf("standard fibers at start: %zu\n", start);

	std::printf("[%d fibers suspended at once on one WaitGroup]\n", N);
	{
		Reset();
		WaitGroup gate; gate.n.store(1); g_gate = &gate;
		WaitGroup all;
		Spawn(&GateBody, nullptr, N, all);
		const bool in = Until([] { return g_in.load() == N; }, [] {}, 20000);
		std::printf("  in=%d pool=%zu\n", g_in.load(), pool.CountOf(StackClass::Standard));
		Check(in, "every task reached the gate (the pool grew instead of stalling)");
		Check(pool.CountOf(StackClass::Standard) >= (size_t)N, "the standard pool holds at least N fibers");
		gate.Done();
		s.WaitFor(all);
		Check(g_done == N, "all finished after the gate opened");
	}

	std::printf("[%d fibers on one Event, SignalAll]\n", N);
	{
		Reset();
		WaitGroup all;
		Spawn(&EventBody, nullptr, N, all);
		const bool ok = Until([] { return g_done.load() == N; }, [] { g_ev.SignalAll(); }, 20000);
		std::printf("  in=%d done=%d\n", g_in.load(), g_done.load());
		Check(ok, "every waiter woke");
		s.WaitFor(all);
	}

	std::printf("[2000 fibers on one Event, CancelWaiters]\n");
	{
		Reset();
		WaitGroup all;
		Spawn(&CancelBody, nullptr, 2000, all);
		const bool ok = Until([] { return g_done.load() == 2000; }, [] { g_ev.CancelWaiters(); }, 20000);
		Check(ok && g_cancelled == 2000, "every waiter returned Cancelled");
		s.WaitFor(all);
	}

	std::printf("[3000 fibers on DirectEvents (pool starts at 1024)]\n");
	{
		Reset();
		WaitGroup all;
		Spawn(&DirectBody, nullptr, 3000, all);
		const bool ok = Until([] { return g_done.load() == 3000; }, [] { SignalDirect(); }, 20000);
		Check(ok, "every direct waiter woke");
		s.WaitFor(all);
	}

#if defined(JLIBSCHED_COROUTINES)
	std::printf("[coroutines: 4000 on one Event, 3000 on DirectEvents]\n");
	{
		Reset();
		WaitGroup all;
		for (int i = 0; i < 4000; ++i) JLib::Spawn(CoroEvent(), &all);
		bool ok = Until([] { return g_done.load() == 4000; }, [] { g_ev.SignalAll(); }, 20000);
		Check(ok, "every coroutine Event waiter woke");
		s.WaitFor(all);

		Reset();
		WaitGroup all2;
		for (int i = 0; i < 3000; ++i) JLib::Spawn(CoroDirect(), &all2);
		ok = Until([] { return g_done.load() == 3000; }, [] { SignalDirect(); }, 20000);
		Check(ok, "every coroutine DirectEvent waiter woke");
		s.WaitFor(all2);
	}
#endif
	return 0;
}

static constexpr int NL = 3000;
static int LimitRun() {
#define N NL
	auto& s = TaskScheduler::Instance();
	auto& pool = s.GetGlobalPool();
	std::printf("[limit %zu MB, %d tasks want fibers at once]\n", pool.MemoryLimit() >> 20, N);
	Reset();
	WaitGroup gate; gate.n.store(1); g_gate = &gate;
	WaitGroup all;
	Spawn(&GateBody, nullptr, N, all);
	// Wait for the count to stop rising.
	int last = -1;
	Until([&] { const int v = g_in.load(); const bool still = (v == last); last = v; return still && v > 0; },
	      [] { std::this_thread::sleep_for(std::chrono::milliseconds(300)); }, 20000);
	const size_t fibers = pool.CountOf(StackClass::Standard);
	std::printf("  in=%d standard fibers=%zu committed=%zu MB\n", g_in.load(), fibers, pool.CommittedBytes() >> 20);
	Check(g_in.load() < N, "growth stopped at the limit (not every task got a fiber)");
	Check(pool.CommittedBytes() <= pool.MemoryLimit(), "committed stack memory stayed within the limit");
	gate.Done();
	const bool ok = Until([] { return g_done.load() == N; }, [] {}, 30000);
	Check(ok, "the rest ran as fibers freed up");
	if (ok) s.WaitFor(all);
	return 0;
}
#undef N

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const bool limit = argc > 1 && std::strchr(argv[1], 'l');
	TaskScheduler::Config cfg;
	cfg.mode    = Mode::Migrate;
	cfg.main    = MainMode::OutOfPool;
	cfg.workers = 4;
	cfg.fibers  = { 2, 1 };   // normal, deep: small, so the pool has to grow
	if (limit) cfg.fiberMemoryLimit = 64u << 20;
	TaskScheduler::Init(cfg);
	std::thread([] {
		std::this_thread::sleep_for(std::chrono::seconds(90));
		std::printf("  [wd] TIMEOUT in=%d done=%d\n", g_in.load(), g_done.load());
		std::_Exit(7);
	}).detach();

	limit ? LimitRun() : GrowRun();

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::_Exit(g_fail ? 1 : 0);
}
