// MainMode::OutOfPool: main does NOT steal while it waits -- it runs only what is routed TO it.
//
// Main out of the pool has no slot, so a stealing main is a stand-in thread by another name: the
// same shape as a spare, with the same problems. Nothing can pin to it, and worse, it could steal
// a task that BLOCKS THE THREAD (BlockInPlace, a driver call) and take main out for an unbounded
// time -- a missed frame in the one thread that cannot miss one. Main steals only when it is IN
// the pool, where it is worker 0 with a slot.
//
// So this test asserts the opposite of what it used to: the pool finishes the work, main runs the
// main-only tasks, and main runs NOTHING it was not given.
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

static std::thread::id g_mainId;
static bool OnMain() { return std::this_thread::get_id() == g_mainId; }
static std::atomic<int> g_done{ 0 }, g_onMain{ 0 }, g_suspendedOnMain{ 0 }, g_resumedOnMain{ 0 };
static std::atomic<int> lambdas{ 0 };
static std::atomic<int> g_lambdaOnMain{ 0 }, g_mainOnly{ 0 }, g_mainOnlyWrong{ 0 };

static void Nop(void*) {}
static void Work(void* p) {
	const intptr_t i = (intptr_t)p;
	if (OnMain()) g_onMain++;
	volatile unsigned x = 0; for (unsigned k = 0; k < 20000; ++k) x += k;
	if (i % 4 == 0) {   // suspend on a tiny task, unpinned
		const bool before = OnMain();
		auto& s = TaskScheduler::Instance();
		WaitGroup wg; wg.n.store(1);
		Task* t = s.CreateTask([] {});   // a lambda: needs no fiber, so waiters cannot starve it
		t->waitGroup = &wg; s.Push(t);
		s.WaitFor(wg, Pin::Current);   // Current on main = no pin
		if (before) { g_suspendedOnMain++; if (OnMain()) g_resumedOnMain++; }
	}
	g_done++;
}
static void MustBeMain(void*) { if (OnMain()) g_mainOnly++; else g_mainOnlyWrong++; }
static void PostMain(void*) {
	auto& s = TaskScheduler::Instance();
	std::this_thread::sleep_for(std::chrono::milliseconds(20));   // main is likely parked by now
	Task* t = s.CreateTask(&MustBeMain, nullptr);
	s.PushMain(t);
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	g_mainId = std::this_thread::get_id();
	TaskScheduler::Init(Mode::Migrate, MainMode::OutOfPool, 4);
	auto& s = TaskScheduler::Instance();
	Thread* me = Thread::GetCurrent();
	std::printf("main_help_test workers=%zu helper=%d\n", s.GetWorkerCount(), me && me->isHelper ? 1 : 0);
	Check(me && me->isHelper && !me->IsPoolWorker(), "main has a helper Thread that is not a pool slot");

	std::printf("[fibers and lambdas: the pool does them, main does not steal]\n");
	constexpr int kN = 8000;
	std::thread([&s] {
		for (int k = 0; k < 10; ++k) {
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			std::printf("  [wd] done=%d lambdas=%d onMain=%d suspOnMain=%d\n",
				g_done.load(), lambdas.load(), g_onMain.load(), g_suspendedOnMain.load());
		}
		s.DumpPoolState("main_help_test watchdog");
		std::fflush(stdout);
		std::_Exit(7);
	}).detach();
	WaitGroup wg; wg.n.store(kN + 2000);
	for (int i = 0; i < kN; ++i) {
		Task* t = s.CreateTask(&Work, (void*)(intptr_t)i);
		t->waitGroup = &wg; s.Push(t);
	}
	for (int i = 0; i < 2000; ++i) {
		Task* t = s.CreateTask([] { if (OnMain()) g_lambdaOnMain++; lambdas++; });
		t->waitGroup = &wg; s.Push(t);
	}
	s.WaitFor(wg);
	std::printf("  done=%d onMain=%d lambdasOnMain=%d suspendedOnMain=%d resumedOnMain=%d\n",
		g_done.load(), g_onMain.load(), g_lambdaOnMain.load(), g_suspendedOnMain.load(), g_resumedOnMain.load());
	Check(g_done == kN && lambdas == 2000, "every task completed");
	Check(g_onMain + g_lambdaOnMain == 0, "main ran none of them: it does not steal out of the pool");

	std::printf("[PushMain while main is parked in WaitFor]\n");
	{
		WaitGroup w2; w2.n.store(1);
		Task* p = s.CreateTask(&PostMain, nullptr);
		p->waitGroup = &w2; s.Push(p);
		s.WaitFor(w2);
		// The main-only task may still be queued: main runs it in its next wait.
		WaitGroup w3; w3.n.store(1);
		Task* q = s.CreateTask([] { std::this_thread::sleep_for(std::chrono::milliseconds(30)); });
		q->waitGroup = &w3; s.Push(q);
		s.WaitFor(w3);
		Check(g_mainOnly == 1 && g_mainOnlyWrong == 0, "the main-only task ran, on main");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::_Exit(g_fail ? 1 : 0);
}
