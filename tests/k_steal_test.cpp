// K steal policy: K=2, Pin ("p") or Migrate (default).
//  quiet lane : Native + suspending Fiber mix. Everything completes; in Pin mode no fiber starts on K.
//  hot lane   : a producer floods the I/O lane while bulk work runs. Everything completes.
#include <TaskScheduler.h>
#include <TaskDAG.h>
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
	if (!ok) ++g_fail;
}

static size_t g_n = 0, g_k = 0;
static bool IsK(int q) { return q >= 0 && (size_t)q >= g_n - g_k; }

static std::atomic<int> g_nativeOnK{ 0 }, g_nativeRan{ 0 };
static std::atomic<int> g_fiberStartOnK{ 0 }, g_fiberResumeOnK{ 0 }, g_fiberRan{ 0 };
static std::atomic<int> g_laneRan{ 0 };

static int CurQ() { Thread* w = Thread::GetCurrent(); return w ? w->qIndex : -1; }

static void NativeBody(void*) {
	if (IsK(CurQ())) g_nativeOnK.fetch_add(1);
	volatile unsigned x = 0;
	for (unsigned i = 0; i < 20000; ++i) x += i;
	g_nativeRan.fetch_add(1);
}

static void Inner(void*) {
	volatile unsigned x = 0;
	for (unsigned i = 0; i < 5000; ++i) x += i;
}

static void FiberBody(void*) {
	if (IsK(CurQ())) g_fiberStartOnK.fetch_add(1);
	TaskScheduler& s = TaskScheduler::Instance();
	WaitGroup inner;
	inner.n.store(1);
	Task* t = s.CreateTask(&Inner, nullptr, Lane::Normal, TaskType::Fiber);
	t->waitGroup = &inner;
	s.Push(t);
	s.WaitFor(inner);                       // suspends this fiber
	if (IsK(CurQ())) g_fiberResumeOnK.fetch_add(1);
	g_fiberRan.fetch_add(1);
}

static void LaneBody(void*) { g_laneRan.fetch_add(1); }

static std::atomic<int> g_yieldRan{ 0 }, g_yieldOnK{ 0 };
static void YieldBody(void*) {
	for (int i = 0; i < 4; ++i) {
		if (IsK(CurQ())) g_yieldOnK.fetch_add(1);
		volatile unsigned x = 0;
		for (unsigned j = 0; j < 5000; ++j) x += j;
		Thread::Yield();
	}
	g_yieldRan.fetch_add(1);
}

static bool WaitWithTimeout(TaskScheduler& s, WaitGroup& wg, int seconds) {
	std::atomic<bool> done{ false };
	std::thread watchdog([&] {
		for (int i = 0; i < seconds * 10 && !done.load(); ++i)
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		if (!done.load()) {
			std::printf("  FAIL HANG: %d task(s) still outstanding after %ds\n",
				wg.n.load() & WaitGroup::COUNT_MASK, seconds);
			std::fflush(stdout);
			std::_Exit(3);
		}
	});
	s.WaitFor(wg);
	done.store(true);
	watchdog.join();
	return true;
}

int main(int argc, char** argv) {
	const bool pin = argc > 1 && std::strchr(argv[1], 'p');
	TaskScheduler::Config cfg;
	cfg.mode       = pin ? Mode::Pinned : Mode::Migrate;
	cfg.main       = MainMode::OutOfPool;
	cfg.workers    = 4;
	cfg.hotWorkers = 2;
	cfg.fibers.normalPerComputeWorker = 1024;   // 1000 fibers suspend at once in this test
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();
	g_n = s.GetWorkerCount();
	g_k = TaskScheduler::GetHotWorkers();
	std::printf("mode=%s workers=%zu K=%zu intake=%d quietWindow=%uus\n",
		pin ? "Pin" : "Migrate", g_n, g_k, (int)s.InjectorEnabled(),
		s.IoQuietWindowUs());

	std::printf("[quiet lane: 4000 Native + 1000 suspending Fiber]\n");
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(20));   // lane is quiet
		const int NN = 4000, NF = 1000;
		WaitGroup wg;
		wg.n.store(NN + NF);
		for (int i = 0; i < NN + NF; ++i) {
			const bool fiber = (i % 5) == 0;
			Task* t = fiber ? s.CreateTask(&FiberBody, nullptr, Lane::Normal, TaskType::Fiber)
			                : s.CreateTask(&NativeBody, nullptr, Lane::Normal, TaskType::Native);
			t->waitGroup = &wg;
			s.Push(t);
		}
		WaitWithTimeout(s, wg, 20);
		uint64_t st = 0, rt = 0;
		TaskScheduler::GetReservedStealStats(st, rt);
		std::printf("  native ran=%d (on K=%d)  fiber ran=%d (started on K=%d, resumed on K=%d)\n",
			g_nativeRan.load(), g_nativeOnK.load(), g_fiberRan.load(),
			g_fiberStartOnK.load(), g_fiberResumeOnK.load());
		std::printf("  K steals=%llu returned=%llu\n", (unsigned long long)st, (unsigned long long)rt);
		Check(g_nativeRan.load() == NN && g_fiberRan.load() == NF, "every task completed");
		if (pin) {
			Check(st == 0, "Pin: K never stole");
			Check(g_nativeOnK.load() == 0 && g_fiberStartOnK.load() == 0 && g_fiberResumeOnK.load() == 0,
				"Pin: no bulk task ran on K");
		}
	}

	std::printf("[yield: 500 fibers that each yield 4 times]\n");
	{
		const int NY = 500;
		WaitGroup wg;
		wg.n.store(NY);
		for (int i = 0; i < NY; ++i) {
			Task* t = s.CreateTask(&YieldBody, nullptr, Lane::Normal, TaskType::Fiber);
			t->waitGroup = &wg;
			s.Push(t);
		}
		WaitWithTimeout(s, wg, 20);
		std::printf("  yield fibers ran=%d  (yield points observed on K=%d)\n", g_yieldRan.load(), g_yieldOnK.load());
		Check(g_yieldRan.load() == NY, "every yielding fiber completed");
		if (pin) Check(g_yieldOnK.load() == 0, "Pin: no yielding fiber ever ran on K");
	}

	std::printf("[hot lane: producer floods LowLatency while 8000 Native + 1000 Fiber run]\n");
	{
		uint64_t st0 = 0, rt0 = 0;
		TaskScheduler::GetReservedStealStats(st0, rt0);
		g_nativeRan = 0; g_fiberRan = 0; g_laneRan = 0;
		g_nativeOnK = 0; g_fiberStartOnK = 0; g_fiberResumeOnK = 0;

		// The producer keeps the lane hot for the WHOLE bulk run: one push, then a short spin,
		// so the gap between pushes stays well under the quiet window.
		std::atomic<bool> bulkDone{ false };
		std::atomic<int>  pushed{ 0 };
		WaitGroup laneWg;
		laneWg.n.store(1);   // held open by the producer until it stops
		std::thread producer([&] {
			while (!bulkDone.load(std::memory_order_relaxed)) {
				laneWg.n.fetch_add(1);
				Task* t = s.CreateTask(&LaneBody, nullptr, Lane::LowLatency, TaskType::Fiber);
				t->waitGroup = &laneWg;
				s.Push(t);
				pushed.fetch_add(1);
				const auto until = std::chrono::steady_clock::now() + std::chrono::microseconds(50);
				while (std::chrono::steady_clock::now() < until) {}
			}
			const int old = laneWg.n.fetch_sub(1);
			if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT)) laneWg.WakeAll();
		});

		const int NN = 8000, NF = 1000;
		WaitGroup wg;
		wg.n.store(NN + NF);
		for (int i = 0; i < NN + NF; ++i) {
			const bool fiber = (i % 9) == 0;
			Task* t = fiber ? s.CreateTask(&FiberBody, nullptr, Lane::Normal, TaskType::Fiber)
			                : s.CreateTask(&NativeBody, nullptr, Lane::Normal, TaskType::Native);
			t->waitGroup = &wg;
			s.Push(t);
		}
		WaitWithTimeout(s, wg, 30);
		bulkDone.store(true);
		producer.join();
		WaitWithTimeout(s, laneWg, 30);
		const int NL = pushed.load();

		uint64_t st = 0, rt = 0;
		TaskScheduler::GetReservedStealStats(st, rt);
		std::printf("  native ran=%d (on K=%d)  fiber ran=%d (started on K=%d)  lane ran=%d\n",
			g_nativeRan.load(), g_nativeOnK.load(), g_fiberRan.load(), g_fiberStartOnK.load(),
			g_laneRan.load());
		std::printf("  K steals=%llu returned=%llu\n",
			(unsigned long long)(st - st0), (unsigned long long)(rt - rt0));
		Check(g_nativeRan.load() == NN && g_fiberRan.load() == NF, "every bulk task completed");
		Check(g_laneRan.load() == NL, "every lane task completed");
		if (pin) Check(st == st0 && g_nativeOnK.load() == 0 && g_fiberStartOnK.load() == 0,
			"Pin: K never stole, no bulk task ran on K");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	return g_fail ? 1 : 0;
}
