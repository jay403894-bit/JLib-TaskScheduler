// Per-call pinning. Arg "f" = SetDefaultPin(Pin::Current) (forced). Build with
// -DJLIBSCHED_COROUTINES=1 against the coroutine library to include the coroutine checks.
#include <TaskScheduler.h>
#include <Thread.h>
#if defined(JLIBSCHED_COROUTINES)
#include <Coroutine.h>
#endif
#include <atomic>
#include <cstdio>
#include <cstring>
#include <thread>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}
static int Q() { Thread* t = Thread::GetCurrent(); return t ? (int)t->qIndex : -1; }
static bool IsK(int q) {
	const size_t n = TaskScheduler::Instance().GetWorkerCount();
	return q >= 0 && TaskScheduler::IsReservedIndex((size_t)q, n);
}

static void Nop(void*) {}
// A suspension that another worker resumes: wait on a tiny task.
static void WaitTiny(Pin pin) {
	auto& s = TaskScheduler::Instance();
	WaitGroup wg; wg.n.store(1);
	Task* t = s.CreateTask(&Nop, nullptr);
	t->waitGroup = &wg; s.Push(t);
	s.WaitFor(wg, pin);
}

static SchedulerMutex     g_mtx;
static SchedulerSemaphore g_sem{ 1 };
static std::atomic<int> g_moved{ 0 }, g_runs{ 0 }, g_wrong{ 0 }, g_notK{ 0 };

// Main's helper (Migrate) may run a fiber; a suspension there is unpinned by definition, so
// each suspension is checked against where it happened, and only worker suspensions count.
static void Stayed(int at) { if (at >= 0 && Q() != at) g_moved++; }
static void CurrentBody(void*) {
	for (int i = 0; i < 20; ++i) {
		int at = Q(); WaitTiny(Pin::Current);        Stayed(at);
		at = Q();     Thread::CoYield(Pin::Current); Stayed(at);
		at = Q();     g_mtx.Lock(Pin::Current);      Stayed(at);
		std::this_thread::yield();               // hold briefly so others contend
		g_mtx.Unlock();
		at = Q();     g_sem.Wait(Pin::Current);      Stayed(at);
		g_sem.Signal();
	}
	g_runs++;
}
static void NoneBody(void*) {
	const int home = Q();
	for (int i = 0; i < 20; ++i) { WaitTiny(Pin::None); if (Q() != home) g_moved++; }
	g_runs++;
}
static std::atomic<int> g_target{ 1 };
static void ThreadBody(void*) {
	const int target = g_target.load();
	for (int i = 0; i < 20; ++i) { WaitTiny(Pin::Thread((uint16_t)target)); if (Q() != target) g_wrong++; }
	g_runs++;
}

#if defined(JLIBSCHED_COROUTINES)
// Pin::Current means "resume where I suspended". Main (out of the pool) may run a coroutine's
// segment while helping; a suspension there is unpinned by definition, so only worker
// suspensions are checked.
static Coro CoroCurrent() {
	for (int i = 0; i < 20; ++i) {
		int at = Q();
		co_await Reschedule{ Pin::Current };     if (at >= 0 && Q() != at) g_moved++;
		WaitGroup wg; wg.n.store(1);
		Task* t = TaskScheduler::Instance().CreateTask(&Nop, nullptr);
		t->waitGroup = &wg; TaskScheduler::Instance().Push(t);
		at = Q();
		co_await WaitAsync(wg, Pin::Current);    if (at >= 0 && Q() != at) g_moved++;
		at = Q();
		co_await LockAsync(g_mtx, Pin::Current); if (at >= 0 && Q() != at) g_moved++;
		g_mtx.Unlock();
	}
	g_runs++;
}
static Coro CoroThread(int target) {
	for (int i = 0; i < 20; ++i) {
		co_await Reschedule{ Pin::Thread((uint16_t)target) };
		if (Q() != target) g_wrong++;
	}
	g_runs++;
}
#endif

static void RunMany(void (*fn)(void*), int n) {
	auto& s = TaskScheduler::Instance();
	WaitGroup wg; wg.n.store(n);
	for (int i = 0; i < n; ++i) { Task* t = s.CreateTask(fn, nullptr); t->waitGroup = &wg; s.Push(t); }
	s.WaitFor(wg);
}
static void Reset() { g_moved = 0; g_runs = 0; g_wrong = 0; g_notK = 0; }

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const bool forced = argc > 1 && std::strchr(argv[1], 'f');
	TaskScheduler::Config cfg;
	cfg.mode       = forced ? Mode::Pinned : Mode::Migrate;
	cfg.main       = MainMode::OutOfPool;
	cfg.workers    = 6;
	cfg.hotWorkers = 2;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	std::printf("pin_test workers=%zu K=%zu default=%s\n", s.GetWorkerCount(), TaskScheduler::GetHotWorkers(),
		forced ? "Current (forced)" : "None");

	if (forced) {
		std::printf("[forced: Pin::None is ignored, every suspension stays]\n");
		Reset(); RunMany(&NoneBody, 32);
		Check(g_runs == 32 && g_moved == 0, "no resume changed worker");
		std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
		std::_Exit(g_fail ? 1 : 0);
	}

	std::printf("[fibers: Pin::Current at every suspend point]\n");
	Reset(); RunMany(&CurrentBody, 32);
	std::printf("  moved=%d\n", g_moved.load());
	Check(g_runs == 32 && g_moved == 0, "WaitFor, CoYield, contended Lock and Semaphore::Wait all resumed home");

	std::printf("[fibers: Pin::None may move]\n");
	Reset(); RunMany(&NoneBody, 32);
	std::printf("  moved=%d\n", g_moved.load());
	Check(g_runs == 32 && g_moved > 0, "unpinned resumes migrated");

	std::printf("[fibers: Pin::Thread(n)]\n");
	for (int target : { 0, 1, 3, 4 }) {   // 4 is a K (I/O) worker
		Reset(); g_target = target; RunMany(&ThreadBody, 16);
		char msg[96]; std::snprintf(msg, sizeof msg, "every resume ran on worker %d", target);
		Check(g_runs == 16 && g_wrong == 0, msg);
	}

#if defined(JLIBSCHED_COROUTINES)
	std::printf("[coroutines: same options]\n");
	{
		Reset();
		WaitGroup wg;
		for (int i = 0; i < 32; ++i) Spawn(CoroCurrent(), &wg);
		s.WaitFor(wg);
		Check(g_runs == 32 && g_moved == 0, "Reschedule, WaitAsync and LockAsync with Pin::Current resumed home");
		Reset();
		WaitGroup wg2;
		for (int i = 0; i < 16; ++i) Spawn(CoroThread(2), &wg2);
		s.WaitFor(wg2);
		Check(g_runs == 16 && g_wrong == 0, "Pin::Thread(2) resumed on worker 2");
	}
#endif

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::_Exit(g_fail ? 1 : 0);
}
