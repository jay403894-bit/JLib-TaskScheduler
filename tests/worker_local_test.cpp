// Thread::Local<T> -- worker-local storage reached through TaskRecord::home.
//
// Fibers roam the pool; at every stop each bumps a PLAIN (non-atomic) counter in the Local<Bucket>
// of the worker it is on now. Only that worker ever writes its bucket, so no increment may be lost:
// after the join, the per-worker buckets collected through TaskScheduler::GetWorkers() must sum to the
// exact total. Also: each bucket lives in its own worker's heap, honours alignas(64), distinct types
// get distinct objects, a type nobody used was never built, and Join destroys every one.
#include <TaskScheduler.h>
#include <Thread.h>
#include <Memory.h>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static std::atomic<int> g_built{ 0 }, g_destroyed{ 0 };
struct alignas(64) Bucket {
	long long n = 0;                       // plain: only its own worker writes it
	Bucket()  { g_built++; }
	~Bucket() { g_destroyed++; }
};
struct Other { int tag = 7; };
struct NeverUsed { int x = 0; };

constexpr int kTasks = 64, kStops = 200;
static std::atomic<int> g_badHome{ 0 };

static void Roam(void*) {
	Task* self = TaskScheduler::Instance().GetCurrentTask();
	for (int i = 0; i < kStops; ++i) {
		Thread* home = self->record->home;   // re-read after every suspension
		// Raw TLS on purpose. SelfWorker resolves THROUGH home, so comparing the two would be a
		// tautology; the claim here is that home is the thread actually executing this stack.
		if (home != detail::TlsThreadRaw()) g_badHome++;
		home->Local<Bucket>().n++;
		if (home->Local<Other>().tag != 7) g_badHome++;
		// Every 50th stop resumes on main, which is out of the pool: its helper is not in GetWorkers.
		if (Fiber* f = FiberFromStack()) f->Yield(i % 50 == 49 ? Pin::Main : Pin::None);
	}
}

int main() {
	TaskScheduler::Config cfg;
	cfg.mode = Mode::Migrate;
	cfg.main = MainMode::OutOfPool;   // every task runs on a pool worker, all in GetWorkers()
	TaskScheduler::Init(cfg);
	TaskScheduler& s = TaskScheduler::Instance();
	const size_t n = s.GetWorkerCount();
	std::printf("worker_local_test -- workers=%zu\n", n);

	WaitGroup wg;
	wg.n.store(kTasks, std::memory_order_relaxed);
	for (int i = 0; i < kTasks; ++i) {
		Task* t = s.CreateTask(&Roam, nullptr, TaskType::Fiber);
		t->waitGroup = &wg;
		s.Push(t);
	}
	s.WaitFor(wg);

	long long sum = 0;
	int buckets = 0, wrongHeap = 0, misaligned = 0, shared = 0, neverBuilt = 0, helperBuckets = 0;
	const std::vector<Thread*>& workers = TaskScheduler::GetWorkers();
	TaskScheduler::ForEachTaskThread([&](Thread* w) {
		if (Bucket* b = w->PeekLocal<Bucket>()) {
			++buckets;
			sum += b->n;
			if (w->isHelper) ++helperBuckets;
			if (detail::HeapOf(b) != static_cast<const void*>(w->heap)) ++wrongHeap;
			if (reinterpret_cast<std::uintptr_t>(b) % 64 != 0) ++misaligned;
			if (static_cast<void*>(w->PeekLocal<Other>()) == static_cast<void*>(b)) ++shared;
		}
		if (w->PeekLocal<NeverUsed>()) ++neverBuilt;
	});
	std::printf("  %d threads built a bucket (%d on main's helper); sum %lld of %d\n",
	            buckets, helperBuckets, sum, kTasks * kStops);
	Check(workers.size() == n, "GetWorkers is the whole workers array");
	Check(buckets >= 2, "more than one worker holds a bucket (tasks really spread)");
	Check(helperBuckets == 1, "Pin::Main stops built a bucket on main's helper");
	Check(sum == (long long)kTasks * kStops, "no increment lost: ForEachTaskThread saw every bucket");
	Check(g_badHome.load() == 0, "home was the running thread at every stop");
	Check(wrongHeap == 0, "each bucket lives in its own worker's heap");
	Check(misaligned == 0, "alignas(64) honoured");
	Check(shared == 0, "distinct types get distinct objects");
	Check(neverBuilt == 0, "a type nobody asked for was never built");
	Check(g_built.load() == buckets, "one bucket per worker that used it");

	detail::TeardownForTesting(s);
	Check(g_destroyed.load() == g_built.load(), "Join destroyed every bucket");
	std::printf("RESULT: %s\n", g_fail ? "FAILED" : "all checks passed");
	return g_fail ? 1 : 0;
}
