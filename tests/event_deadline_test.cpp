// An Event Deadline must cancel only the waiters under its token.
//
// Task A waits on `ev` inside a CancelScope with a 20 ms Deadline (EjectEvent). Task B waits on the
// SAME event with no deadline. When the deadline fires, A must wake Cancelled and B must KEEP
// WAITING; a later SignalAll then wakes B with Ok.
//
// This FAILED on the old Treiber-stack Event: EjectEvent could only detach the whole list, so B
// woke Cancelled too. The doubly linked list under a lock unlinks only the tasks in the token.
#include <TaskScheduler.h>
#include <Event.h>
#include <Timer.h>
#include <CancelToken.h>
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

static Event* g_ev = nullptr;
struct Waiter { std::atomic<int> result{ -1 }; std::atomic<bool> done{ false }; };
static Waiter g_a, g_b;

static void WaitBody(void* p) {
	Waiter& w = *static_cast<Waiter*>(p);
	w.result.store((int)TaskScheduler::Instance().WaitOnEventCancellable(*g_ev), std::memory_order_relaxed);
	w.done.store(true, std::memory_order_release);
}

template <class F> static bool WaitUntil(F pred, int ms) {
	const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
	while (std::chrono::steady_clock::now() < end) { if (pred()) return true; std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
	return pred();
}

int main() {
	TaskScheduler::Config cfg;
	cfg.main   = MainMode::OutOfPool;
	cfg.timers = true;
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();
	std::printf("event_deadline_test\n");

	Event ev;
	g_ev = &ev;
	CancelScope scopeA;

	Task* a = s.CreateTask(&WaitBody, &g_a, TaskType::Fiber);
	a->cancelToken = scopeA.Token().Raw();          // A is under the deadline's token
	Task* b = s.CreateTask(&WaitBody, &g_b, TaskType::Fiber);   // B is not
	s.Push(a);
	s.Push(b);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));   // both linked and parked

	{
		Deadline d(20'000'000, scopeA.Token(), &EjectEvent, &ev);
		Check(WaitUntil([] { return g_a.done.load(std::memory_order_acquire); }, 2000), "A woke when its deadline fired");
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	Check(g_a.result.load() == (int)WaitResult::Cancelled, "A saw Cancelled");
	Check(!g_b.done.load(std::memory_order_acquire), "B (no deadline) is STILL waiting after A's deadline");

	ev.SignalAll();
	Check(WaitUntil([] { return g_b.done.load(std::memory_order_acquire); }, 2000), "B woke on SignalAll");
	Check(g_b.result.load() == (int)WaitResult::Ok, "B saw Ok, not Cancelled");

	detail::TeardownForTesting(s);
	std::printf("RESULT: %s\n", g_fail ? "FAILED" : "all checks passed");
	return g_fail ? 1 : 0;
}
