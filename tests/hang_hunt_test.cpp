// Repeat pool_run's "4096 Native tasks" stage with K=2 until it sticks, then dump the pool.
#include <TaskScheduler.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static std::atomic<int> g_ran{ 0 };
static void Body(void*) { g_ran.fetch_add(1, std::memory_order_relaxed); }

int main() {
	TaskScheduler::Config cfg;
	cfg.mode       = Mode::Migrate;
	cfg.main       = MainMode::OutOfPool;
	cfg.workers    = 4;
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();

	for (int iter = 0; iter < 300; ++iter) {
		const int N = 4096;
		g_ran.store(0);
		WaitGroup wg;
		wg.n.store(N);
		for (int i = 0; i < N; ++i) {
			Task* t = s.CreateTask(&Body, nullptr, TaskType::Fiber);
			t->waitGroup = &wg;
			s.Push(t);
		}
		std::atomic<bool> done{ false };
		std::thread watchdog([&] {
			for (int i = 0; i < 30 && !done.load(); ++i)
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			if (done.load()) return;
			std::printf("HANG at iter %d: ran=%d outstanding=%d searching=%u idle=%zu steals=?\n",
				iter, g_ran.load(), wg.n.load() & WaitGroup::COUNT_MASK,
				TaskScheduler::SearchingCount(), TaskScheduler::IdleWorkerCount());
			s.DumpPoolState("hang_hunt");
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			std::printf("after 500ms: ran=%d outstanding=%d\n", g_ran.load(), wg.n.load() & WaitGroup::COUNT_MASK);
			s.DumpPoolState("hang_hunt +500ms");
			std::fflush(stdout);
			std::_Exit(3);
		});
		s.WaitFor(wg);
		done.store(true);
		watchdog.join();
		if (iter == 0 || (iter & 3) == 0)
			std::this_thread::sleep_for(std::chrono::milliseconds(20));   // let workers park between rounds
	}
	std::printf("no hang in 300 iterations\n");
	return 0;
}
