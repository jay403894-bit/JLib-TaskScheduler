// ParallelFor in each PforMode (Cursor, LazyPCore) runs every index exactly once: from main
// and from inside a fiber task, over sizes and grains that force splits, uneven tails and single
// grains. Also PushTo(CorePref): every class, both inboxes, all tasks run.
// Arg: "i" main in the pool, "o" main out of it.
#include <TaskScheduler.h>
#include <Thread.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static constexpr int kMaxN = 200000;
static std::atomic<unsigned char> g_hits[kMaxN];

// Every 64th index spins a little so ranges finish unevenly and thieves split them.
static void Body(int lo, int hi) {
	for (int i = lo; i < hi; ++i) {
		g_hits[i].fetch_add(1, std::memory_order_relaxed);
		if ((i & 63) == 0) {
			volatile unsigned x = 0;
			for (int k = 0; k < 2000; ++k) x += k;
		}
	}
}

static bool RunOnce(TaskScheduler& s, int n, int grain) {
	for (int i = 0; i < n; ++i) g_hits[i].store(0, std::memory_order_relaxed);
	s.ParallelFor(0, n, grain, [](int lo, int hi) { Body(lo, hi); });
	for (int i = 0; i < n; ++i)
		if (g_hits[i].load(std::memory_order_relaxed) != 1) {
			std::printf("    n=%d grain=%d: index %d ran %d times\n", n, grain, i, (int)g_hits[i].load());
			return false;
		}
	return true;
}

static bool Sweep(TaskScheduler& s) {
	static const int sizes[]  = { 1, 17, 1000, 65537, kMaxN };
	static const int grains[] = { 1, 7, 256 };
	for (int n : sizes)
		for (int g : grains)
			if (!RunOnce(s, n, g)) return false;
	return true;
}

struct InTask { TaskScheduler* s; bool ok; };

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const bool inPool = argc > 1 && std::strcmp(argv[1], "i") == 0;
	TaskScheduler::Config cfg;
	cfg.workers = 8;
	cfg.main = inPool ? MainMode::InPool : MainMode::OutOfPool;
	cfg.tunables.measuredWidth = false;
	cfg.tunables.minItersPerWorker = 1;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();
	std::printf("main %s the pool\n", inPool ? "in" : "out of");

	using M = TaskScheduler::PforMode;
	const struct { M m; const char* name; } modes[] = {
		{ M::Cursor, "Cursor" }, { M::LazyPCore, "LazyPCore" }, { M::Auto, "Auto" } };
	char msg[128];
	for (const auto& md : modes) {
		s.SetPforMode(md.m);
		bool ok = true;
		for (int rep = 0; rep < 3 && ok; ++rep) ok = Sweep(s);
		std::snprintf(msg, sizeof msg, "%s from main: every index exactly once", md.name);
		Check(ok, msg);

		InTask it{ &s, false };
		WaitGroup wg;
		wg.n.store(1);
		Task* t = s.CreateTask([](void* p) {
			auto* x = static_cast<InTask*>(p);
			x->ok = Sweep(*x->s);
		}, &it, Lane::Normal, TaskType::Fiber);
		t->waitGroup = &wg;
		s.Push(t);
		s.WaitFor(wg);
		std::snprintf(msg, sizeof msg, "%s from a fiber task: every index exactly once", md.name);
		Check(it.ok, msg);
	}

	// Auto with the probe on (the path that picks by piece cost), and the per-call mode overload.
	{
		s.SetMeasuredWidth(true);
		s.SetPforMode(M::Auto);
		bool ok = true;
		for (int leaves : { 8, 4096 }) {
			s.SetLeavesPerWorker((size_t)leaves);
			for (int rep = 0; rep < 3 && ok; ++rep) ok = Sweep(s);
		}
		Check(ok, "Auto with the probe on: every index exactly once");

		bool ok2 = true;
		for (M m : { M::Cursor, M::LazyPCore, M::Auto }) {
			for (int i = 0; i < kMaxN; ++i) g_hits[i].store(0, std::memory_order_relaxed);
			s.ParallelFor(0, kMaxN, 64, [](int lo, int hi) { Body(lo, hi); }, m);
			for (int i = 0; i < kMaxN && ok2; ++i) ok2 = g_hits[i].load() == 1;
		}
		Check(ok2, "per-call PforMode overload: every index exactly once");
		s.SetMeasuredWidth(false);
		s.SetLeavesPerWorker(8);
	}

	// PushTo(CorePref).
	{
		static std::atomic<int> ran{ 0 };
		constexpr int kPer = 40;
		const CorePref prefs[] = { CorePref::P, CorePref::E, CorePref::Any };
		WaitGroup wg;
		int pushed = 0;
		for (CorePref p : prefs)
			for (int hi = 0; hi < 2; ++hi)
				for (int i = 0; i < kPer; ++i) {
					Task* t = s.CreateTask([](void*) { ran.fetch_add(1); }, nullptr);
					wg.n.fetch_add(1);
					t->waitGroup = &wg;
					if (s.PushTo(t, p, hi != 0)) ++pushed;
					else { wg.n.fetch_sub(1); s.FreeTask(t); }
				}
		s.WaitFor(wg);
		std::printf("  PushTo(CorePref): %d pushed, %d ran\n", pushed, ran.load());
		Check(pushed == 6 * kPer && ran.load() == pushed, "PushTo(CorePref) P/E/Any, hi-pri and normal: all ran");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
