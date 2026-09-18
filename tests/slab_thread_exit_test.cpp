// A thread's slab cache goes back to the pool when the thread exits.
//
// Many short-lived threads each create and free a batch of tasks. Every free lands in that
// thread's cache. If an exiting thread kept its cache, each one would strand up to a batch of
// slots and the slab's high-water mark would climb with the number of threads. Given back, the
// next thread refills from the returned slots and the high-water mark stays flat.
#include <TaskScheduler.h>
#include <cstdio>
#include <thread>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static void Nop(void*) {}

static size_t Resident() {
	const auto u = TaskScheduler::SlabUsage();
	return u.c64.resident + u.c80.resident + u.c128.resident + u.c256.resident + u.c512.resident;
}

static void ChurnOnNewThread(int tasks) {
	std::thread([tasks] {
		auto& s = TaskScheduler::Instance();
		Task* made[64];
		for (int i = 0; i < tasks; ++i) made[i] = s.CreateTask(&Nop, nullptr);
		for (int i = 0; i < tasks; ++i) s.FreeTask(made[i]);
	}).join();
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	TaskScheduler::Config cfg;
	cfg.workers = 2;
	cfg.main    = MainMode::OutOfPool;
	cfg.lazyTaskSlab = true;   // resident = what was actually handed out, not a prefault
	TaskScheduler::Init(cfg);

	constexpr int kThreads = 400, kTasks = 64;
	for (int i = 0; i < 8; ++i) ChurnOnNewThread(kTasks);   // warm up
	const size_t before = Resident();
	for (int i = 0; i < kThreads; ++i) ChurnOnNewThread(kTasks);
	const size_t after = Resident();

	std::printf("  resident slab slots: %zu before, %zu after %d short-lived threads\n",
		before, after, kThreads);
	// Kept caches would add up to ~kTasks per thread per class: tens of thousands of slots.
	Check(after - before < (size_t)kTasks * 4,
	      "an exiting thread gives its cached slots back (high-water mark stays flat)");

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
