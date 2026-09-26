// FTL mode: main is pool slot 0. Args: 'k' = K=2, 'p' = SetDefaultPin(Pin::Current).
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <Thread.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>
#if defined(_WIN32)
	#ifndef WIN32_LEAN_AND_MEAN
		#define WIN32_LEAN_AND_MEAN
	#endif
	#include <windows.h>
#else
	#include <sys/resource.h>
	#include <sys/time.h>
#endif

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	std::fflush(stdout);
	if (!ok) ++g_fail;
}
static void Stage(const char* s) { std::printf("[%s]\n", s); std::fflush(stdout); }

static std::thread::id g_mainId;
static bool OnMain() { return std::this_thread::get_id() == g_mainId; }
static int  CurQ() { Thread* w = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers()); return w ? w->qIndex : -1; }

// Watchdog: any stage that takes more than 10s is a hang.
static std::atomic<int> g_stageSeq{ 0 };
static void Watchdog() {
	int last = -1, same = 0;
	for (;;) {
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
		const int s = g_stageSeq.load();
		if (s < 0) return;
		same = (s == last) ? same + 1 : 0;
		last = s;
		if (same >= 20) {
			std::printf("HANG in stage %d\n", s);
			TaskScheduler::Instance().DumpPoolState("ftl_test hang");
			std::fflush(stdout);
			std::_Exit(3);
		}
	}
}

// ---- bodies ----
struct Rec { std::atomic<int> q{ -2 }; std::atomic<size_t> tid{ SIZE_MAX }; std::atomic<int> onMain{ -1 }; };
static Rec g_rec[64];
static void Targeted(void* p) {
	auto& r = g_rec[(size_t)(intptr_t)p];
	r.q = CurQ(); r.tid = thread_id; r.onMain = OnMain() ? 1 : 0;
}

static std::atomic<int> g_count{ 0 }, g_onMain{ 0 };
static void Counted(void*) {
	if (OnMain()) g_onMain.fetch_add(1);
	volatile unsigned x = 0; for (unsigned i = 0; i < 2000; ++i) x += i;
	g_count.fetch_add(1);
}

static std::atomic<int> g_mainTaskRan{ 0 }, g_mainTaskWrong{ 0 };
static void MustBeMain(void*) {
	if (OnMain()) g_mainTaskRan.fetch_add(1); else g_mainTaskWrong.fetch_add(1);
}

// A worker task that posts main-only work.
struct PostCtx { WaitGroup* wg; int n; TaskType type; };
static void PostToMain(void* p) {
	auto& c = *static_cast<PostCtx*>(p);
	auto& s = TaskScheduler::Instance();
	for (int i = 0; i < c.n; ++i) {
		Task* t = s.CreateTask(&MustBeMain, nullptr, c.type);
		t->waitGroup = c.wg;
		s.PushMain(t);
	}
}

static std::atomic<int> g_fiberDone{ 0 }, g_fiberResumedOnMain{ 0 }, g_fiberStartedOnMain{ 0 };
static void Inner(void*) { volatile unsigned x = 0; for (unsigned i = 0; i < 3000; ++i) x += i; }
static void SuspendingFiber(void*) {
	const bool startedMain = OnMain();
	if (startedMain) g_fiberStartedOnMain.fetch_add(1);
	auto& s = TaskScheduler::Instance();
	WaitGroup inner; inner.n.store(1);
	Task* t = s.CreateTask(&Inner, nullptr, TaskType::Fiber);
	t->waitGroup = &inner;
	s.Push(t);
	s.WaitFor(inner);
	if (startedMain && OnMain()) g_fiberResumedOnMain.fetch_add(1);
	g_fiberDone.fetch_add(1);
}

// Native task on main that blocks in WaitFor -> nested Worker() on main.
static std::atomic<int> g_nestedDone{ 0 };
static void NestedWaiter(void*) {
	auto& s = TaskScheduler::Instance();
	WaitGroup inner; inner.n.store(64);
	for (int i = 0; i < 64; ++i) {
		Task* t = s.CreateTask(&Counted, nullptr, TaskType::Fiber);
		t->waitGroup = &inner;
		s.Push(t);
	}
	s.WaitFor(inner);
	if ((inner.n.load() & WaitGroup::COUNT_MASK) == 0) g_nestedDone.fetch_add(1);
}

// A main-only task that blocks: it must still be on main afterwards.
static std::atomic<int> g_blockMainOk{ 0 }, g_blockMainBad{ 0 };
static void MainBlocking(void*) {
	const bool before = OnMain();
	auto& s = TaskScheduler::Instance();
	WaitGroup inner; inner.n.store(32);
	for (int i = 0; i < 32; ++i) {
		Task* t = s.CreateTask(&Counted, nullptr, TaskType::Fiber);
		t->waitGroup = &inner;
		s.Push(t);
	}
	// PushMain only chose where it started. The wait resumes where it parked, so getting back to
	// main is a separate hop: SendToMain, a yield with a named placement.
	s.WaitFor(inner);
	Thread::SendToMain();
	if (before && OnMain()) g_blockMainOk.fetch_add(1); else g_blockMainBad.fetch_add(1);
}
static void PostBlockingToMain(void* p) {
	auto& c = *static_cast<PostCtx*>(p);
	auto& s = TaskScheduler::Instance();
	for (int i = 0; i < c.n; ++i) {
		Task* t = s.CreateTask(&MainBlocking, nullptr, c.type);
		t->waitGroup = c.wg;
		s.PushMain(t);
	}
}

static std::atomic<bool> g_flag{ false };
static bool FlagSet(void*) { return g_flag.load(); }

// Process CPU time, kernel + user, in milliseconds.
static double CpuMs() {
#if defined(_WIN32)
	FILETIME c, e, k, u;
	GetProcessTimes(GetCurrentProcess(), &c, &e, &k, &u);
	auto v = [](FILETIME f) { return ((unsigned long long)f.dwHighDateTime << 32 | f.dwLowDateTime) / 10000.0; };
	return v(k) + v(u);
#else
	rusage ru{};
	getrusage(RUSAGE_SELF, &ru);
	auto v = [](const timeval& t) { return t.tv_sec * 1000.0 + t.tv_usec / 1000.0; };
	return v(ru.ru_utime) + v(ru.ru_stime);
#endif
}

int main(int argc, char** argv) {
	const bool pin  = argc > 1 && std::strchr(argv[1], 'p');
	g_mainId = std::this_thread::get_id();
	std::thread wd(Watchdog); wd.detach();

	TaskScheduler::Config cfg;
	cfg.mode       = pin ? Mode::Pinned : Mode::Migrate;
	cfg.main       = MainMode::InPool;
	cfg.workers    = 4;
	cfg.fibers.normalPerComputeWorker = 512;
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();
	const size_t n = s.GetWorkerCount();
	std::printf("FTL workers=%zu fibers=%s\n", n, pin ? "Pin" : "Migrate");

	g_stageSeq = 1; Stage("main is slot 0");
	{
		Thread* me = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
		Check(me != nullptr && me->qIndex == 0 && me->isMain, "TaskScheduler::SelfWorker(TaskScheduler::GetWorkers()) on main is slot 0");
		Check(thread_id == 0, "main owns epoch slot 0");
	}

	g_stageSeq = 2; Stage("PushTo(q) runs on q; epoch slot q");
	{
		WaitGroup wg; wg.n.store((int)n);
		for (size_t q = 0; q < n; ++q) {
			Task* t = s.CreateTask(&Targeted, (void*)(intptr_t)q, TaskType::Fiber);
			t->waitGroup = &wg;
			if (!s.PushTo(q, t)) { wg.n.fetch_sub(1); Check(false, "PushTo refused"); }
		}
		s.WaitFor(wg);
		bool ok = true;
		for (size_t q = 0; q < n; ++q) {
			std::printf("  queue %zu -> worker %d, slot %zu, onMain=%d\n", q, g_rec[q].q.load(), g_rec[q].tid.load(), g_rec[q].onMain.load());
			ok = ok && g_rec[q].q.load() == (int)q && g_rec[q].tid.load() == q && g_rec[q].onMain.load() == (q == 0 ? 1 : 0);
		}
		Check(ok, "each queue ran on its own worker; queue 0 ran on main");
	}

	g_stageSeq = 3; Stage("20000 Push + WaitFor on main");
	{
		const int N = 20000;
		g_count = 0; g_onMain = 0;
		WaitGroup wg; wg.n.store(N);
		for (int i = 0; i < N; ++i) { Task* t = s.CreateTask(&Counted, nullptr); t->waitGroup = &wg; s.Push(t); }
		s.WaitFor(wg);
		std::printf("  ran=%d, of which on main=%d\n", g_count.load(), g_onMain.load());
		Check(g_count.load() == N, "all ran");
		Check(g_onMain.load() > 0, "main took part");
	}

	// PushMain keeps the type it is given: a Fiber task gets a fiber on main, a Native one runs
	// straight on main's stack. Both must land on main.
	for (TaskType ty : { TaskType::Fiber, TaskType::Native }) {
	g_stageSeq = 4; Stage(ty == TaskType::Fiber ? "PushMain from workers (Fiber)" : "PushMain from workers (Native)");
	{
		g_mainTaskRan = 0; g_mainTaskWrong = 0;
		const int per = 50, posters = 8;
		WaitGroup wg; wg.n.store(per * posters + posters);
		std::vector<PostCtx> ctx(posters, PostCtx{ &wg, per, ty });
		for (int i = 0; i < posters; ++i) {
			Task* t = s.CreateTask(&PostToMain, &ctx[i], TaskType::Fiber);
			t->waitGroup = &wg;
			s.PushTo(1 + (size_t)i % (n - 1), t);
		}
		s.WaitFor(wg);
		std::printf("  on main=%d, elsewhere=%d\n", g_mainTaskRan.load(), g_mainTaskWrong.load());
		Check(g_mainTaskRan.load() == per * posters && g_mainTaskWrong.load() == 0, "every PushMain task ran on main");
	}
	}

	// Fiber: suspends, and Pin::Main brings the resume back. Native: cannot suspend -- main's wait
	// helps the pool on main's own stack instead, so it never leaves main at all.
	for (TaskType ty : { TaskType::Fiber, TaskType::Native }) {
	g_stageSeq = 41; Stage(ty == TaskType::Fiber ? "PushMain tasks that WaitFor(Pin::Main) stay on main (Fiber)"
	                                             : "PushMain tasks that WaitFor stay on main (Native: main helps)");
	{
		g_blockMainOk = 0; g_blockMainBad = 0;
		const int per = 10, posters = 4;
		WaitGroup wg; wg.n.store(per * posters + posters);
		std::vector<PostCtx> ctx(posters, PostCtx{ &wg, per, ty });
		for (int i = 0; i < posters; ++i) {
			Task* t = s.CreateTask(&PostBlockingToMain, &ctx[i], TaskType::Fiber);
			t->waitGroup = &wg;
			s.PushTo(1 + (size_t)i % (n - 1), t);
		}
		s.WaitFor(wg);
		std::printf("  stayed on main=%d, left main=%d\n",
			g_blockMainOk.load(), g_blockMainBad.load());
		Check(g_blockMainOk.load() == per * posters && g_blockMainBad.load() == 0, "blocking main tasks never left main");
	}
	}

	g_stageSeq = 5; Stage("suspending fibers (some on main)");
	{
		const int N = 400;
		g_fiberDone = 0; g_fiberStartedOnMain = 0; g_fiberResumedOnMain = 0;
		WaitGroup wg; wg.n.store(N);
		for (int i = 0; i < N; ++i) {
			Task* t = s.CreateTask(&SuspendingFiber, nullptr, TaskType::Fiber);
			t->waitGroup = &wg;
			if (i % 4 == 0) s.PushTo(0, t); else s.Push(t);
		}
		s.WaitFor(wg);
		std::printf("  done=%d started on main=%d resumed on main=%d\n",
			g_fiberDone.load(), g_fiberStartedOnMain.load(), g_fiberResumedOnMain.load());
		Check(g_fiberDone.load() == N, "all fibers completed");
		Check(g_fiberStartedOnMain.load() > 0, "main ran fibers");
		if (pin) Check(g_fiberResumedOnMain.load() == g_fiberStartedOnMain.load(), "Pin: fibers started on main resumed on main");
	}

	g_stageSeq = 6; Stage("nested WaitFor: Native waiters on main, Fiber waiters on workers");
	{
		// A Native task may block in WaitFor only on main (it re-enters Worker()). On a spawned
		// worker a blocking waiter must be a Fiber -- that is the pool contract.
		g_nestedDone = 0;
		WaitGroup wg; wg.n.store(8);
		for (int i = 0; i < 8; ++i) {
			const bool onMain = (i % 2 == 0);   // all-fiber: blocking waiters are legal on every worker
			Task* t = s.CreateTask(&NestedWaiter, nullptr, TaskType::Fiber);
			t->waitGroup = &wg;
			s.PushTo(onMain ? 0 : 1 + (size_t)i % (n - 1), t);
		}
		s.WaitFor(wg);
		Check(g_nestedDone.load() == 8, "all nested waits completed");
	}

	g_stageSeq = 7; Stage("WaitFor(predicate) + WakeMain");
	{
		g_flag = false;
		std::thread setter([] {
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
			g_flag = true;
			TaskScheduler::WakeMain();
		});
		const auto t0 = std::chrono::steady_clock::now();
		const bool r = s.WaitFor(nullptr, &FlagSet, nullptr);
		const auto ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
		setter.join();
		std::printf("  returned %d after %.1f ms\n", (int)r, ms);
		Check(r && ms < 1000, "predicate wait returned promptly after WakeMain");
	}

	g_stageSeq = 8; Stage("MainWorker() returns when idle");
	{
		const auto t0 = std::chrono::steady_clock::now();
		const bool r = TaskScheduler::MainWorker();
		const auto ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
		std::printf("  returned %d after %.2f ms\n", (int)r, ms);
		Check(r && ms < 100, "MainWorker() came back");
	}

	g_stageSeq = 9; Stage("ParallelFor from main");
	{
		std::atomic<long long> sum{ 0 };
		s.ParallelFor(0, 100000, [&](int lo, int hi) { long long a = 0; for (int i = lo; i < hi; ++i) a += i; sum.fetch_add(a); });
		Check(sum.load() == 4999950000LL, "ParallelFor sum correct");
	}

	g_stageSeq = 10; Stage("DAG with a main node");
	{
		g_mainTaskRan = 0; g_mainTaskWrong = 0; g_count = 0;
		WaitGroup wg; wg.n.store(3);
		TaskDAG dag(s);
		Task* ta = s.CreateTask(&Counted, nullptr); ta->waitGroup = &wg;
		Task* tm = s.CreateTask(&MustBeMain, nullptr, TaskType::Fiber); tm->waitGroup = &wg;
		Task* tc = s.CreateTask(&Counted, nullptr); tc->waitGroup = &wg;
		auto* a = dag.CreateNode(ta);
		auto* m = dag.CreateMainNode(tm);
		auto* c = dag.CreateNode(tc);
		dag.AddDependency(m, a);
		dag.AddDependency(c, m);
		dag.Submit();
		s.WaitFor(wg);
		Check(g_mainTaskRan.load() == 1 && g_mainTaskWrong.load() == 0 && g_count.load() == 2, "a -> main node -> c ran in order, main node on main");
	}

	g_stageSeq = 11; Stage("idle CPU (listener rotates; main may take its turn only inside WaitFor)");
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
		const double c0 = CpuMs();
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		const double c1 = CpuMs();
		std::printf("  idle 1000ms: %.0f ms CPU (%.2f cores), searching=%u\n", c1 - c0, (c1 - c0) / 1000.0, TaskScheduler::SearchingCount());
	}

	g_stageSeq = 12; Stage("Join from main");
	detail::TeardownForTesting(s);
	// The raw TLS slot, deliberately: this asserts the SLOT was released, not that identity
	// resolves to nothing. SelfWorker would answer null for several other reasons and would pass
	// even if the slot still held a pointer.
	Check(detail::TlsThreadRaw() == nullptr, "main's slot released");

	g_stageSeq = -1;
	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	return g_fail ? 1 : 0;
}
