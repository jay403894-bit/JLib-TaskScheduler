// SchedulerSemaphore and SchedulerConditionVariable. Arg "p" = Mode::Pinned.
// Targets: a signal racing a newly queued waiter (the banked-permit lost wake), a notify racing a
// cancelled CV waiter (its semaphore lives on that fiber's stack), and pin-correct cancel wakes.
// Build with -DJLIBSCHED_COROUTINES=1 against the coroutine library.
#include <TaskScheduler.h>
#include <Coroutine.h>
#include <Thread.h>
#include <CancelToken.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

using namespace JLib;
using Clock = std::chrono::steady_clock;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}
template <class P>
static bool Until(P pred, int ms) {
	const auto end = Clock::now() + std::chrono::milliseconds(ms);
	while (!pred()) {
		if (Clock::now() > end) return false;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}

static std::atomic<int> g_woke{ 0 }, g_queued{ 0 }, g_cancelled{ 0 }, g_moved{ 0 };
static void Reset() { g_woke = 0; g_queued = 0; g_cancelled = 0; g_moved = 0; }
static int Q() { Thread* t = Thread::GetCurrent(); return t ? (int)t->qIndex : -1; }

// ---- 1. permits accumulate when nobody is waiting -------------------------------------------
static SchedulerSemaphore g_sem{ 0 };
static void TakeOne(void*) { g_sem.Wait(); g_woke++; }

// ---- 2. a signal racing a waiter that queues during the wake ---------------------------------
// The old Signal() popped a waiter, unlocked, and then decided from a STALE "was it empty" -- a
// waiter that queued in that window was left asleep with a permit banked next to it.
static SchedulerSemaphore g_race{ 0 };
static void RaceWaiter(void*) { g_queued++; g_race.Wait(); g_woke++; }

// ---- 3. the same, with a cancelled waiter at the head (the skip path) -------------------------
static SchedulerSemaphore g_skip{ 0 };
static void SkipWaiter(void*) {
	g_queued++;
	if (g_skip.WaitCancellable() == WaitResult::Cancelled) g_cancelled++;
	else g_woke++;
}

// ---- 4. condition variable: notify_one wakes one, notify_all wakes all ------------------------
static SchedulerMutex g_mtx;
static SchedulerConditionVariable g_cv;
static bool g_ready = false;
static void CvWaiter(void*) {
	g_mtx.Lock();
	g_queued++;
	while (!g_ready) g_cv.Wait(g_mtx);
	g_mtx.Unlock();
	g_woke++;
}
// A waiter that is cancelled while a notify may be in flight: its semaphore is on this stack.
static void CvCancellable(void*) {
	g_mtx.Lock();
	g_queued++;
	const WaitResult r = g_cv.WaitCancellable(g_mtx);
	g_mtx.Unlock();
	if (r == WaitResult::Cancelled) g_cancelled++; else g_woke++;
}

// ---- 5. a cancelled coroutine waiter resumes on its pinned worker -----------------------------
static SchedulerSemaphore g_coroSem{ 0 };
static Coro CoroAcquire() {
	const int home = Q();
	g_queued++;
	const WaitResult r = co_await AcquireAsyncCancellable(g_coroSem, Pin::Current);
	if (r == WaitResult::Cancelled) g_cancelled++; else g_woke++;
	if (home >= 0 && Q() != home) g_moved++;
}

static void Spawn(void (*fn)(void*), int n, WaitGroup& wg, uint32_t token = CancelToken::kNone) {
	auto& s = TaskScheduler::Instance();
	wg.n.fetch_add(n);
	for (int i = 0; i < n; ++i) {
		Task* t = s.CreateTask(fn, nullptr, Lane::Normal, TaskType::Fiber);
		t->waitGroup = &wg;
		t->cancelToken = token;
		s.Push(t);
	}
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const bool pin = argc > 1 && std::strchr(argv[1], 'p');
	TaskScheduler::Init(pin ? Mode::Pinned : Mode::Migrate, MainMode::OutOfPool, 6);
	auto& s = TaskScheduler::Instance();
	std::printf("semcv_test mode=%s workers=%zu\n", pin ? "pinned" : "migrate", s.GetWorkerCount());
	std::thread([] {
		std::this_thread::sleep_for(std::chrono::seconds(60));
		std::printf("  [wd] TIMEOUT queued=%d woke=%d cancelled=%d\n",
			g_queued.load(), g_woke.load(), g_cancelled.load());
		std::_Exit(7);
	}).detach();

	std::printf("[a signal with nobody waiting banks a permit]\n");
	{
		Reset();
		for (int i = 0; i < 8; ++i) g_sem.Signal();       // no waiters: 8 permits banked
		WaitGroup wg;
		Spawn(&TakeOne, 8, wg);
		const bool ok = Until([] { return g_woke.load() == 8; }, 5000);
		s.WaitFor(wg);
		Check(ok, "8 signals with no waiter let 8 later waits through");
	}

	std::printf("[a signal racing a waiter that queues during the wake]\n");
	{
		constexpr int kRounds = 400, kPer = 4;
		Reset();
		WaitGroup wg;
		for (int r = 0; r < kRounds; ++r) {
			g_queued = 0; g_woke = 0;
			Spawn(&RaceWaiter, kPer, wg);
			// Signal as the waiters arrive, so some land inside the wake window.
			for (int i = 0; i < kPer; ++i) { g_race.Signal(); std::this_thread::yield(); }
			if (!Until([] { return g_woke.load() == kPer; }, 5000)) {
				std::printf("  round %d: only %d of %d woke (queued %d)\n",
					r, g_woke.load(), kPer, g_queued.load());
				Check(false, "every waiter woke");
				break;
			}
		}
		s.WaitFor(wg);
		Check(g_fail == 0, "400 rounds x 4 waiters: no wake was lost to a banked permit");
	}

	std::printf("[a cancelled waiter at the head does not eat the signal]\n");
	{
		CancelScope scope;
		Reset();
		WaitGroup wg;
		Spawn(&SkipWaiter, 4, wg, scope.Token().Raw());   // these will be cancelled
		Until([] { return g_queued.load() == 4; }, 5000);
		Spawn(&SkipWaiter, 4, wg);                        // these must still be woken
		Until([] { return g_queued.load() == 8; }, 5000);
		scope.Cancel();
		for (int i = 0; i < 4; ++i) g_skip.Signal();
		const bool ok = Until([] { return g_woke.load() + g_cancelled.load() == 8; }, 10000);
		std::printf("  woke=%d cancelled=%d\n", g_woke.load(), g_cancelled.load());
		s.WaitFor(wg);
		Check(ok && g_woke.load() == 4, "the 4 live waiters woke, the 4 cancelled ones returned");
	}

	std::printf("[condition variable: notify_one, then notify_all]\n");
	{
		Reset();
		g_ready = false;
		WaitGroup wg;
		Spawn(&CvWaiter, 8, wg);
		Until([] { return g_queued.load() == 8; }, 5000);
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		{ g_mtx.Lock(); g_ready = true; g_mtx.Unlock(); }
		g_cv.Notify_One();
		const bool one = Until([] { return g_woke.load() >= 1; }, 5000);
		g_cv.Notify_All();
		const bool all = Until([] { return g_woke.load() == 8; }, 5000);
		s.WaitFor(wg);
		Check(one, "notify_one woke a waiter");
		Check(all, "notify_all woke the rest");
	}

	std::printf("[a notify racing a cancelled CV waiter (its semaphore is on that fiber's stack)]\n");
	{
		constexpr int kRounds = 200;
		Reset();
		bool stuck = false;
		for (int r = 0; r < kRounds && !stuck; ++r) {
			CancelScope scope;
			g_queued = 0; g_woke = 0; g_cancelled = 0;
			WaitGroup wg;
			Spawn(&CvCancellable, 4, wg, scope.Token().Raw());
			Until([] { return g_queued.load() == 4; }, 5000);
			scope.Cancel();                 // unlinks under the queue lock
			g_cv.Notify_All();              // races that unlink
			if (!Until([] { return g_woke.load() + g_cancelled.load() == 4; }, 5000)) stuck = true;
			s.WaitFor(wg);
		}
		Check(!stuck, "200 rounds of cancel racing notify: no waiter stranded, no crash");
	}

	std::printf("[a cancelled coroutine waiter resumes where it suspended]\n");
	{
		CancelScope scope;
		Reset();
		WaitGroup wg;
		for (int i = 0; i < 64; ++i) {
			Coro c = CoroAcquire();
			Spawn(std::move(c), &wg, Lane::Normal, scope.Token().Raw());
		}
		Until([] { return g_queued.load() == 64; }, 5000);
		scope.Cancel();
		g_coroSem.CancelWaiters(scope.Token());
		const bool ok = Until([] { return g_cancelled.load() + g_woke.load() == 64; }, 10000);
		s.WaitFor(wg);
		std::printf("  cancelled=%d woke=%d moved=%d\n", g_cancelled.load(), g_woke.load(), g_moved.load());
		Check(ok, "every cancelled coroutine waiter returned");
		if (pin) Check(g_moved.load() == 0, "Pin: each one resumed on the worker it suspended on");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::_Exit(g_fail ? 1 : 0);
}
