// A C++ exception thrown and caught INSIDE a fiber must unwind normally.
//
// On Windows the unwinder checks every frame against the TIB's [StackLimit, StackBase]. If the
// context switch does not keep those fields pointing at the running fiber's stack, every frame on
// a fiber is "out of bounds" and the process dies instead of reaching the catch. Linux has no TIB,
// so there this is a check on the test itself: both halves must pass.
//
// 1. (child) control: throw/catch on the OS thread's own stack -- must always pass.
// 2. (child) N Fiber tasks each throw/catch, one of them from several frames down.
//    Each also records IsOnFiber(): the child fails unless every task really ran on a fiber, so a
//    task that silently ran Native (the CreateTask default) cannot make this pass for free.
#include <TaskScheduler.h>
#include "spawn_self.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <stdexcept>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static constexpr int kTasks = 32;
static constexpr int kDepth = 8;

static std::atomic<int> g_caught{ 0 }, g_onFiber{ 0 };

// Real frames, not a tail call: the unwinder has to walk each one.
JLIB_NOINLINE static int ThrowFrom(int depth) {
	volatile int keep = depth;
	if (depth == 0) throw std::runtime_error("from the bottom");
	const int r = ThrowFrom(depth - 1);
	return r + keep;
}

static void FiberThrow(void* p) {
	const bool deep = p != nullptr;
	if (TaskScheduler::Instance().IsOnFiber()) g_onFiber.fetch_add(1);
	try {
		if (deep) (void)ThrowFrom(kDepth);
		else      throw std::runtime_error("shallow");
	}
	catch (const std::exception&) {
		g_caught.fetch_add(1);
	}
}

static int ChildControl() {
	int caught = 0;
	try { (void)ThrowFrom(kDepth); }
	catch (const std::exception&) { ++caught; }
	return caught == 1 ? 0 : 1;
}

static int ChildFiber() {
	TaskScheduler::Init(Mode::Migrate, MainMode::OutOfPool, 4);
	auto& s = TaskScheduler::Instance();

	WaitGroup wg;
	wg.n.store(kTasks);
	for (int i = 0; i < kTasks; ++i) {
		// Every 4th task throws from kDepth frames down; the rest throw from the task body.
		Task* t = s.CreateTask(&FiberThrow, (i % 4 == 0) ? (void*)1 : nullptr,
		                       TaskType::Fiber);
		t->waitGroup = &wg;
		s.Push(t);
	}
	s.WaitFor(wg);

	std::printf("  child: caught=%d onFiber=%d of %d\n", g_caught.load(), g_onFiber.load(), kTasks);
	return (g_caught.load() == kTasks && g_onFiber.load() == kTasks) ? 0 : 1;
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	if (argc > 1 && std::strcmp(argv[1], "child-control") == 0) std::_Exit(ChildControl());
	if (argc > 1 && std::strcmp(argv[1], "child-fiber")   == 0) std::_Exit(ChildFiber());

	std::printf("fiber_exception_test\n");
	{
		const JLibTest::ChildResult r = JLibTest::RunSelf("child-control", argv[0]);
		Check(r.started && !r.timedOut && !r.aborted, "control: throw/catch on the OS thread's stack");
	}
	{
		const JLibTest::ChildResult r = JLibTest::RunSelf("child-fiber", argv[0]);
		if (r.aborted) std::printf("  child died: code 0x%08lX\n", r.code);
		Check(r.started && !r.timedOut && !r.aborted, "throw/catch inside a fiber, shallow and deep");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
