// TaskRecord::home and Thread::Alloc across migrations.
//
// A task that migrates must allocate from the heap of the thread it is running on NOW: a heap's
// thread-local part only allocates for its owner. Task code asks its record (task->record->home),
// which the scheduler writes at every pickup/steal/resume, instead of thread-local storage.
//
//   1. pinned hops: a fiber is resumed on worker after worker (Pin::Thread). At every hop home must
//      be the thread it is on, and a block from home->Alloc must come from that thread's heap. The
//      block from the PREVIOUS hop is freed here, on another thread (cross-thread free).
//   2. random migration: many fibers yield with no pin and are stolen wherever; same checks.
//   3. native tasks: cannot migrate, but read home through the same accessor.
//   4. coroutines (C++20 build): pinned to a different worker every hop; same checks.
//   Control: a home kept in a local ACROSS a hop is counted -- it must have gone stale at least
//   once, or this test never exercised a migration.
#include <TaskScheduler.h>
#include <Thread.h>
#include <Memory.h>
#if defined(JLIBSCHED_COROUTINES)
#include <Coroutine.h>
#endif
#include <atomic>
#include <cstdio>
#include <cstring>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static std::atomic<int> g_checks{ 0 }, g_homeWrong{ 0 }, g_heapWrong{ 0 }, g_staleSeen{ 0 },
                        g_hops{ 0 }, g_onTarget{ 0 }, g_badBytes{ 0 };
static size_t g_workers = 0;

// One check at the current position: home == the running thread, and its allocation is from
// that thread's heap. Frees `prev` (allocated on an earlier hop, maybe another thread).
static void* CheckHere(Task* self, void* prev, unsigned char tag) {
	Thread* home = self->record->home;           // re-read from the record, never kept
	Thread* now  = Thread::GetCurrent();         // the reference it is compared against
	if (home != now) g_homeWrong++;
	if (!home) { g_checks++; return prev; }     // never written: counted above, nothing to alloc from
	unsigned char* p = static_cast<unsigned char*>(home->Alloc(96));
	if (!p || detail::HeapOf(p) != static_cast<const void*>(home->heap)) g_heapWrong++;
	if (p) std::memset(p, tag, 96);
	if (prev) {
		if (static_cast<unsigned char*>(prev)[95] != (unsigned char)(tag - 1)) g_badBytes++;
		Free(prev);                               // cross-thread free
	}
	g_checks++;
	return p;
}

constexpr int kHopFibers = 8, kHops = 40;
static void Hopper(void* arg) {
	const int id = (int)(intptr_t)arg;
	Task* self = TaskScheduler::Instance().GetCurrentTask();   // once, before any suspension
	void* block = CheckHere(self, nullptr, 0);
	for (int i = 1; i <= kHops; ++i) {
		Thread* kept = self->record->home;                      // the control: kept across the hop
		const uint16_t target = (uint16_t)((id + i * 7) % g_workers);
		Thread::Yield(Pin::Thread(target));
		g_hops++;
		if (self->record->home && self->record->home->qIndex == (int)target) g_onTarget++;
		if (kept != self->record->home) g_staleSeen++;
		block = CheckHere(self, block, (unsigned char)i);
	}
	Free(block);
}

constexpr int kRoamers = 64, kRoams = 200;
static void Roamer(void*) {
	Task* self = TaskScheduler::Instance().GetCurrentTask();
	void* block = CheckHere(self, nullptr, 0);
	for (int i = 1; i <= kRoams; ++i) {
		Thread* kept = self->record->home;
		Thread::Yield(Pin::None);                                // stolen wherever
		if (kept != self->record->home) g_staleSeen++;
		block = CheckHere(self, block, (unsigned char)i);
	}
	Free(block);
}

// Native tasks cannot migrate, but they read home the same way: one accessor for every task type.
constexpr int kNatives = 2000;
static void NativeOnce(void*) {
	Task* self = TaskScheduler::Instance().GetCurrentTask();
	if (!self) { g_homeWrong++; return; }
	Free(CheckHere(self, nullptr, 0));
}

#if defined(JLIBSCHED_COROUTINES)
// Coroutines migrate too (a suspended frame resumes on whichever worker picks its task up).
constexpr int kCoroHops = 50;
static Coro CoroRoam() {
	Task* self = TaskScheduler::Instance().GetCurrentTask();
	void* block = CheckHere(self, nullptr, 0);
	for (int i = 1; i <= kCoroHops; ++i) {
		co_await Reschedule{ Pin::Thread((uint16_t)(i % g_workers)) };   // forced migration
		block = CheckHere(self, block, (unsigned char)i);
	}
	Free(block);
}
#endif

static void RunAll(TaskScheduler& s, void (*fn)(void*), int n, TaskType type = TaskType::Fiber) {
	WaitGroup wg;
	wg.n.store(n, std::memory_order_relaxed);
	for (int i = 0; i < n; ++i) {
		Task* t = s.CreateTask(fn, (void*)(intptr_t)i, Lane::Normal, type);
		t->waitGroup = &wg;
		s.Push(t);
	}
	s.WaitFor(wg);
}

int main() {
	TaskScheduler::Config cfg;
	cfg.mode = Mode::Migrate;
	cfg.main = MainMode::OutOfPool;   // every slot 0..n-1 is a real worker, so every Pin::Thread is
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();
	g_workers = s.GetWorkerCount();
	std::printf("home_alloc_test -- workers=%zu\n", g_workers);
	if (g_workers < 3) { std::printf("  needs 3+ workers; skipped\nRESULT: all checks passed\n"); return 0; }

	RunAll(s, &Hopper, kHopFibers);
	Check(g_hops.load() == kHopFibers * kHops, "every pinned hop resumed");
	Check(g_onTarget.load() == g_hops.load(), "every pinned hop landed on its target worker (home says so)");
	const int stalePinned = g_staleSeen.exchange(0);

	RunAll(s, &Roamer, kRoamers);
	const int staleRoam = g_staleSeen.load();

	const int beforeNative = g_checks.load();
	RunAll(s, &NativeOnce, kNatives, TaskType::Native);
	Check(g_checks.load() - beforeNative == kNatives, "every native task read its home (same accessor)");
#if defined(JLIBSCHED_COROUTINES)
	{
		const int beforeCoro = g_checks.load();
		WaitGroup wg;
		constexpr int kCoros = 64;
		for (int i = 0; i < kCoros; ++i) Spawn(CoroRoam(), &wg);
		s.WaitFor(wg);
		Check(g_checks.load() - beforeCoro == kCoros * (kCoroHops + 1), "every coroutine hop read its home");
	}
#endif

	std::printf("  %d checks; a home kept across a hop went stale %d times pinned, %d roaming\n",
	            g_checks.load(), stalePinned, staleRoam);
	Check(stalePinned > 0 && staleRoam > 0, "control: tasks really migrated (a kept home went stale)");
	Check(g_homeWrong.load() == 0, "record->home is always the thread running the task");
	Check(g_heapWrong.load() == 0, "home->Alloc always came from that thread's own heap");
	Check(g_badBytes.load() == 0, "blocks survived their hop intact and freed cross-thread");

	detail::TeardownForTesting(s);
	std::printf("RESULT: %s\n", g_fail ? "FAILED" : "all checks passed");
	return g_fail ? 1 : 0;
}
