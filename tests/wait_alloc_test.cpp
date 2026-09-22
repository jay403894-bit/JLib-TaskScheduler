// Counts heap allocations while fibers and coroutines suspend on SchedulerMutex,
// SchedulerSemaphore and the CV. The queue used to be std::queue<Waiter>, so every empty ->
// non-empty transition allocated a deque chunk, inside the primitive's spinlock. With the node on
// the waiter's own stack, the count must not depend on how many waits happen.
//
// So each phase runs TWICE, with 4x the waits the second time, and the counts must match. A flat
// "expect zero" would be the wrong test: the surrounding machinery (tasks, slab refills) allocates
// a small fixed amount that has nothing to do with waiting.
//
// The counter is validated first: a phase that is KNOWN to allocate must report a non-zero count,
// otherwise a zero elsewhere means only that the hook is dead.
#include <TaskScheduler.h>
#include <Coroutine.h>
#include <Thread.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <thread>
#include <vector>

using namespace JLib;

static std::atomic<bool>          g_on{ false };
static std::atomic<long long>     g_allocs{ 0 };

void* operator new(size_t n) {
	if (g_on.load(std::memory_order_relaxed)) g_allocs.fetch_add(1, std::memory_order_relaxed);
	void* p = std::malloc(n ? n : 1);
	if (!p) throw std::bad_alloc();
	return p;
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }
void* operator new[](size_t n) { return operator new(n); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static SchedulerMutex              g_mtx;
static SchedulerSemaphore          g_sem{ 1 };
static SchedulerMutex              g_cvMtx;
static SchedulerConditionVariable  g_cv;
static std::atomic<int>            g_cvReady{ 0 };
static std::atomic<long long>      g_spins{ 0 };

// Contend hard enough that almost every acquisition suspends.
static void MutexTask(void* arg) {
	const long n = (long)(intptr_t)arg;
	for (long i = 0; i < n; ++i) {
		g_mtx.Lock();
		g_spins.fetch_add(1, std::memory_order_relaxed);
		g_mtx.Unlock();
	}
}

static void SemTask(void* arg) {
	const long n = (long)(intptr_t)arg;
	for (long i = 0; i < n; ++i) {
		g_sem.Wait();
		g_spins.fetch_add(1, std::memory_order_relaxed);
		g_sem.Signal();
	}
}

static void CvTask(void*) {
	g_cvMtx.Lock();
	g_cvReady.fetch_add(1, std::memory_order_relaxed);
	while (g_cvReady.load(std::memory_order_relaxed) >= 0) {
		g_cv.Wait(g_cvMtx);
		break;
	}
	g_cvMtx.Unlock();
}

static Coro CoroMutex(long n) {
	for (long i = 0; i < n; ++i) {
		co_await LockAsync(g_mtx);
		g_spins.fetch_add(1, std::memory_order_relaxed);
		g_mtx.Unlock();
	}
}

static void RunFiberPhase(void (*fn)(void*), int tasks, long per) {
	auto& s = TaskScheduler::Instance();
	WaitGroup wg;
	wg.n.fetch_add(tasks);
	for (int i = 0; i < tasks; ++i) {
		Task* t = s.CreateTask(fn, (void*)(intptr_t)per, Lane::Normal, TaskType::Fiber);
		t->waitGroup = &wg;
		s.Push(t);
	}
	s.WaitFor(wg);
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	TaskScheduler::Init(Mode::Migrate, MainMode::OutOfPool, 8);
	auto& s = TaskScheduler::Instance();
	std::printf("waitalloc_test workers=%zu\n", s.GetWorkerCount());

	constexpr int  kTasks = 32;
	constexpr long kPer   = 400;

	// Warmup: create every fiber and coroutine frame this run will use, so the measured phase is
	// pure waiting.
	RunFiberPhase(&MutexTask, kTasks, kPer);
	RunFiberPhase(&SemTask,   kTasks, kPer);
	{
		WaitGroup wg;
		for (int i = 0; i < kTasks; ++i) { Coro c = CoroMutex(kPer); Spawn(std::move(c), &wg); }
		s.WaitFor(wg);
	}

	// --- the counter is alive ------------------------------------------------------------------
	{
		g_allocs = 0;
		g_on = true;
		// A direct call, not a new-expression: the compiler may remove an unobserved new/delete
		// pair (GCC did), which silently made this control count nothing.
		static void* volatile sink = nullptr;
		sink = ::operator new(1000);
		::operator delete(sink);
		g_on = false;
		Check(g_allocs.load() > 0, "the allocation counter sees a known allocation");
		std::printf("       control phase: %lld allocations\n", g_allocs.load());
	}

	// --- fibers suspending on a contended mutex ------------------------------------------------
	{
		long long a1 = 0, a4 = 0, n1 = 0, n4 = 0;
		g_spins = 0; g_allocs = 0; g_on = true;
		RunFiberPhase(&MutexTask, kTasks, kPer);
		g_on = false; a1 = g_allocs.load(); n1 = g_spins.load();

		g_spins = 0; g_allocs = 0; g_on = true;
		RunFiberPhase(&MutexTask, kTasks, kPer * 4);
		g_on = false; a4 = g_allocs.load(); n4 = g_spins.load();

		std::printf("       %lld acquisitions: %lld allocations | %lld acquisitions: %lld allocations\n",
			n1, a1, n4, a4);
		Check(a4 == a1, "SchedulerMutex: 4x the fiber waits costs no extra allocation");
	}

	// --- fibers suspending on a semaphore ------------------------------------------------------
	{
		long long a1 = 0, a4 = 0, n1 = 0, n4 = 0;
		g_spins = 0; g_allocs = 0; g_on = true;
		RunFiberPhase(&SemTask, kTasks, kPer);
		g_on = false; a1 = g_allocs.load(); n1 = g_spins.load();

		g_spins = 0; g_allocs = 0; g_on = true;
		RunFiberPhase(&SemTask, kTasks, kPer * 4);
		g_on = false; a4 = g_allocs.load(); n4 = g_spins.load();

		std::printf("       %lld acquisitions: %lld allocations | %lld acquisitions: %lld allocations\n",
			n1, a1, n4, a4);
		Check(a4 == a1, "SchedulerSemaphore: 4x the fiber waits costs no extra allocation");
	}

	// --- fibers waiting on a condition variable ------------------------------------------------
	{
		g_cvReady = 0;
		WaitGroup wg;
		wg.n.fetch_add(kTasks);
		for (int i = 0; i < kTasks; ++i) {
			Task* t = s.CreateTask(&CvTask, nullptr, Lane::Normal, TaskType::Fiber);
			t->waitGroup = &wg;
			s.Push(t);
		}
		while (g_cvReady.load() < kTasks) std::this_thread::sleep_for(std::chrono::milliseconds(1));
		std::this_thread::sleep_for(std::chrono::milliseconds(20));

		g_allocs = 0; g_on = true;
		g_cv.Notify_All();
		s.WaitFor(wg);
		g_on = false;
		std::printf("       %d CV waiters notified: %lld allocations\n", kTasks, g_allocs.load());
		// The CV queue holds only stack nodes now, so notifying 32 of them cannot allocate 32 times.
		Check(g_allocs.load() < kTasks, "SchedulerConditionVariable: the queue does not allocate per waiter");
	}

	// --- coroutines awaiting a contended mutex -------------------------------------------------
	{
		long long a[2] = {}, n[2] = {};
		for (int round = 0; round < 2; ++round) {
			const long per = round ? kPer * 4 : kPer;
			g_spins = 0; g_allocs = 0;
			WaitGroup wg;
			std::vector<Coro> coros;
			coros.reserve(kTasks);
			for (int i = 0; i < kTasks; ++i) coros.push_back(CoroMutex(per));   // frames: outside
			g_on = true;
			for (auto& c : coros) Spawn(std::move(c), &wg);
			s.WaitFor(wg);
			g_on = false;
			a[round] = g_allocs.load(); n[round] = g_spins.load();
		}
		std::printf("       %lld coroutine acquisitions: %lld allocations | %lld: %lld allocations\n",
			n[0], a[0], n[1], a[1]);
		Check(a[1] == a[0], "LockAwaiter: 4x the coroutine waits costs no extra allocation");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::_Exit(g_fail ? 1 : 0);
}
