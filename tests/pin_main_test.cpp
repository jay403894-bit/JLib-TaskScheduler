// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Pin::Main: a task suspends on a worker and RESUMES ON THE MAIN THREAD, so the tail of one task
// runs where a device context, a window or a driver lives ("upload this texture on main, then
// carry on"). It must mean the same thing in both main modes, which is why it is its own sentinel:
// Pin::Thread(0) is main only when main is IN the pool.
//   Arg: m = main in the pool (default: out of the pool).
#include <TaskScheduler.h>
#include <Thread.h>
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

// glibc declares pthread_self __attribute_const__, so the compiler may hoist a get_id() across
// our context switch (a "memory" clobber does NOT stop it: const means "does not read memory").
// Every id read that straddles a suspension must go through an opaque call.
#if defined(__GNUC__) && !defined(_MSC_VER)
__attribute__((noinline))
#endif
static std::thread::id CurrentThreadIdOpaque() {
#if defined(__GNUC__) && !defined(_MSC_VER)
	__asm__ __volatile__("" ::: "memory");
#endif
	return std::this_thread::get_id();
}

static std::thread::id g_mainId;
static std::atomic<bool> g_ranBefore{ false }, g_ranAfter{ false };
static std::thread::id g_beforeId, g_afterId;
static WaitGroup* g_inner = nullptr;
static WaitGroup* g_outer = nullptr;
static std::atomic<int> g_resumes{ 0 };

// Suspends with Pin::Main; everything after the wait must be on main.
static void Waiter(void*) {
	g_beforeId = CurrentThreadIdOpaque();
	g_ranBefore.store(true);
	// Wait, then HOP. A resume does not route -- it lands on the thread the task parked on, which is
	// the only thread its handle can name. Coming back on main is SendToMain: a yield with a named
	// placement, taken after the wait has already returned.
	TaskScheduler::Instance().WaitFor(*g_inner);
	Thread::SendToMain();
	g_afterId = CurrentThreadIdOpaque();
	g_ranAfter.store(true);
	g_resumes.fetch_add(1);
	g_outer->Done();
}

// A second hop: suspend again from main, resume on main again (the resume path must survive being
// run BY main, not only reaching it).
static void TwoHop(void*) {
	auto& s = TaskScheduler::Instance();
	WaitGroup a, b;
	a.n.store(1); b.n.store(1);
	Task* t1 = s.CreateTask([&a]() { std::this_thread::sleep_for(std::chrono::milliseconds(5)); a.Done(); });
	s.Push(t1);
	s.WaitFor(a);
	Thread::SendToMain();
	const bool first = CurrentThreadIdOpaque() == g_mainId;
	Task* t2 = s.CreateTask([&b]() { std::this_thread::sleep_for(std::chrono::milliseconds(5)); b.Done(); });
	s.Push(t2);
	s.WaitFor(b);
	Thread::SendToMain();
	const bool second = CurrentThreadIdOpaque() == g_mainId;
	if (first && second) g_resumes.fetch_add(1);
	g_outer->Done();
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const bool inPool = argc > 1 && std::strcmp(argv[1], "m") == 0;
	g_mainId = CurrentThreadIdOpaque();

	TaskScheduler::Config cfg;
	cfg.main    = inPool ? MainMode::InPool : MainMode::OutOfPool;
	cfg.workers = 4;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	std::printf("pin_main_test: main %s the pool\n", inPool ? "IN" : "OUT OF");

	// 1. One hop: worker -> main.
	{
		WaitGroup inner, outer;
		inner.n.store(1);
		outer.n.store(1);
		g_inner = &inner; g_outer = &outer;
		g_resumes = 0;

		// PushTo, not Push: a hi-pri inbox is never stolen, so the head is guaranteed to start on
		// worker 1 (never main, which is slot 0 in pool and a stealing helper out of it). Without
		// that, main steals the task inside its own WaitFor and the test proves nothing.
		s.PushTo(1, s.CreateTask(&Waiter, nullptr, TaskType::Fiber));
		// The release must come from a task that sleeps first. Completing the group from main as
		// soon as the waiter says it is about to wait is a race: WaitFor's fast path returns
		// without suspending if the count is already zero, and then there is no resume to pin --
		// the tail runs on the worker and the test fails for the wrong reason.
		s.Push(s.CreateTask([&inner]() {
			std::this_thread::sleep_for(std::chrono::milliseconds(30));
			inner.Done();
		}));
		s.WaitFor(outer);      // main's own wait drains what is routed to it

		Check(g_ranAfter.load(), "the pinned task resumed at all");
		std::printf("  before=%s  after=%s (main)\n",
		            g_beforeId == g_mainId ? "main" : "worker",
		            g_afterId  == g_mainId ? "main" : "worker");
		Check(g_afterId == g_mainId, "the tail after Pin::Main ran on the main thread");
		Check(g_beforeId != g_mainId, "control: the head ran on a worker, so the hop is real");
	}

	// 2. Two hops in a row, both pinned to main.
	{
		WaitGroup outer;
		outer.n.store(1);
		g_outer = &outer;
		g_resumes = 0;
		s.Push(s.CreateTask(&TwoHop, nullptr, TaskType::Fiber));
		s.WaitFor(outer);
		Check(g_resumes.load() == 1, "two suspensions in a row both resumed on main");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
