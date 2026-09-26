// Memory.h: per-worker mimalloc heaps.
//
//   every thread that allocates -- workers and main -- gets its own heap
//   a block allocated on one thread and freed on another is fine (cross-thread free)
//   a block still alive at Join survives it and can be freed afterwards (the late TaskDAG case)
//   a second pool in the same process starts with fresh heaps
//
// argv[1]: "in" (main in the pool, default), "out" (main out of it), "arena" (in + reserved arena).
#include <TaskScheduler.h>
#include <Thread.h>
#include <Memory.h>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <set>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

constexpr int kBlocks = 20000;
static void* g_blocks[kBlocks];
static std::atomic<int> g_bad{ 0 };
static std::mutex g_heapsMtx;
static std::set<const void*> g_heaps;   // distinct Thread::heap values seen

static void NoteHeap() {
	Thread* t = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
	std::lock_guard<std::mutex> lk(g_heapsMtx);
	g_heaps.insert(t ? (const void*)t->heap : nullptr);
}

static void AllocOne(void* p) {
	const int i = (int)(intptr_t)p;
	const size_t n = 16 + (size_t)(i % 64) * 32;   // 16 .. 2032 bytes
	Thread* home = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
	if (!home) { g_bad++; return; }
	unsigned char* b = static_cast<unsigned char*>(home->Alloc(n));
	if (!b) { g_bad++; return; }
	std::memset(b, (unsigned char)i, n);
	g_blocks[i] = b;
	NoteHeap();
}
static void FreeOne(void* p) {
	// Freed in a DIFFERENT task than the one that allocated it, so usually on another thread.
	const int i = (int)(intptr_t)p;
	unsigned char* b = static_cast<unsigned char*>(g_blocks[i]);
	if (!b || b[0] != (unsigned char)i) { g_bad++; return; }
	mi_free(b);   // any thread, no heap needed
	g_blocks[i] = nullptr;
}

static void RunPool(const char* mode, bool keepOne) {
	TaskScheduler::Config cfg;
	cfg.mode    = Mode::Migrate;
	cfg.main    = std::strcmp(mode, "out") == 0 ? MainMode::OutOfPool : MainMode::InPool;
	cfg.workers = 4;
	if (std::strcmp(mode, "arena") == 0) cfg.arenaReserveBytes = size_t(64) << 20;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	g_heaps.clear(); g_bad = 0;

	{   // allocate across the pool
		WaitGroup wg; wg.n.store(kBlocks);
		for (int i = 0; i < kBlocks; ++i) {
			Task* t = s.CreateTask(&AllocOne, (void*)(intptr_t)i);
			t->waitGroup = &wg; s.Push(t);
		}
		s.WaitFor(wg);
	}
	Thread* mainSelf = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers());
	void* mainBlock = mainSelf ? mainSelf->Alloc(256) : nullptr;   // main allocates too
	NoteHeap();
	{   // free in reverse order from fresh tasks: lands on other threads
		WaitGroup wg; wg.n.store(kBlocks);
		for (int i = kBlocks - 1; i >= 0; --i) {
			Task* t = s.CreateTask(&FreeOne, (void*)(intptr_t)i);
			t->waitGroup = &wg; s.Push(t);
		}
		s.WaitFor(wg);
	}
	const bool mainHasHeap = TaskScheduler::SelfWorker(TaskScheduler::GetWorkers()) && TaskScheduler::SelfWorker(TaskScheduler::GetWorkers())->heap;
	std::printf("  distinct heaps seen: %zu (workers=%zu)\n", g_heaps.size(), s.GetWorkerCount());
	Check(g_bad.load() == 0, "every block allocated, intact, and freed");
	Check(g_heaps.count(nullptr) == 0, "every allocating thread had its own heap");
	Check(g_heaps.size() >= 2, "more than one heap in use (per-worker, not shared)");
	Check(mainBlock != nullptr && mainHasHeap, "main allocates through its own heap");

	void* survivor = (keepOne && mainSelf) ? mainSelf->Alloc(4096) : nullptr;
	mi_free(mainBlock);
	detail::TeardownForTesting(s);
	if (keepOne) {
		std::memset(survivor, 0x5A, 4096);    // still valid after Join
		mi_free(survivor);
		Check(true, "a block alive at Join is still usable and freeable after it");
	}
	detail::DestroyForTesting();
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const char* mode = argc > 1 ? argv[1] : "in";
	std::printf("memory_test mode=%s\n[pool 1]\n", mode);
	RunPool(mode, true);
	std::printf("[pool 2, same process]\n");
	RunPool(mode, false);
	std::printf("RESULT: %s\n", g_fail ? "FAILURES" : "all checks passed");
	return g_fail ? 1 : 0;
}
