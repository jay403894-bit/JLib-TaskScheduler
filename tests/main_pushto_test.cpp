// FTL: can PushTo(0) + Pin::Thread(0) replace mainQ for main-only work?
// Plain fiber tasks are pushed with PushTo(0) (main's hi-pri inbox, run directly by main) and
// suspend with Pin::Thread(0). Every step must run on the OS main thread.
#include <TaskScheduler.h>
#include <Thread.h>
#include <atomic>
#include <cstdio>
#include <thread>

using namespace JLib;

static std::thread::id g_mainId;
static std::atomic<int> g_offMain{ 0 }, g_steps{ 0 }, g_done{ 0 };

static void Nop(void*) {}
static void MainOnly(void*) {
	auto& s = TaskScheduler::Instance();
	for (int i = 0; i < 10; ++i) {
		if (std::this_thread::get_id() != g_mainId) g_offMain++;
		g_steps++;
		WaitGroup wg; wg.n.store(1);
		Task* t = s.CreateTask(&Nop, nullptr);   // runs anywhere
		t->waitGroup = &wg; s.Push(t);
		s.WaitFor(wg, Pin::Thread(0));           // resume on main
		if (std::this_thread::get_id() != g_mainId) g_offMain++;
		Thread::CoYield(Pin::Thread(0));
	}
	g_done++;
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	g_mainId = std::this_thread::get_id();
	TaskScheduler::Init(Mode::Migrate, MainMode::InPool, 4);
	auto& s = TaskScheduler::Instance();

	constexpr int kTasks = 64;
	WaitGroup all; all.n.store(kTasks);
	for (int i = 0; i < kTasks; ++i) {
		Task* t = s.CreateTask(&MainOnly, nullptr);
		t->waitGroup = &all;
		if (!s.PushTo(0, t)) { std::printf("PushTo(0) refused\n"); return 1; }
	}
	s.WaitFor(all);   // main runs the pool (and its own inbox) until the group is done

	std::printf("tasks done=%d steps=%d offMain=%d\n", g_done.load(), g_steps.load(), g_offMain.load());
	const bool ok = g_done == kTasks && g_offMain == 0;
	std::printf(ok ? "RESULT: all checks passed\n" : "RESULT: FAILED\n");
	std::_Exit(ok ? 0 : 1);
}
