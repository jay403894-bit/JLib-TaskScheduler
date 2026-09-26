// Periodic tasks on the TimerQueue. Arg: p = Pin fibers; "child" = Join from inside the callback (must abort).
#include <TaskScheduler.h>
#include <Timer.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include "spawn_self.h"

using namespace JLib;
static constexpr int64_t kMs = 1'000'000;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}
static void SleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

struct Probe {
	std::atomic<long> runs{ 0 }, live{ 0 }, maxLive{ 0 }, skippedSeen{ 0 }, lastSkipped{ 0 };
	int sleepMs = 0;
	long stopAfter = -1;
	uint64_t magic = 0xC0FFEE;
};
static bool Tick(void* p, uint64_t skipped) {
	auto* pr = static_cast<Probe*>(p);
	if (pr->magic != 0xC0FFEE) std::abort();   // ctx used after it was freed
	const long now = pr->live.fetch_add(1) + 1;
	long m = pr->maxLive.load();
	while (now > m && !pr->maxLive.compare_exchange_weak(m, now)) {}
	pr->skippedSeen.fetch_add((long)skipped);
	pr->lastSkipped.store((long)skipped);
	if (pr->sleepMs) SleepMs(pr->sleepMs);
	const long r = pr->runs.fetch_add(1) + 1;
	pr->live.fetch_sub(1);
	return pr->stopAfter < 0 || r < pr->stopAfter;
}

static Periodic* g_self = nullptr;
static bool JoinSelf(void*, uint64_t) { g_self->Cancel(); g_self->Join(); return true; }

// 300 ms stall once, 1 ms interval.
static std::atomic<int> g_stallStage{ 0 };
static std::atomic<long> g_afterStallSkipped{ -1 };
static std::atomic<long long> g_stallStartNs{ 0 }, g_nextStartNs{ 0 };
static bool Stall(void*, uint64_t skipped) {
	const int st = g_stallStage.load();
	if (st == 0) { g_stallStartNs.store(MonotonicNs()); g_stallStage.store(1); SleepMs(300); }
	else if (st == 1) {
		g_nextStartNs.store(MonotonicNs());
		g_afterStallSkipped.store((long)skipped); g_stallStage.store(2); return false;
	}
	return true;
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const bool child = argc > 1 && std::strcmp(argv[1], "child") == 0;
	const bool pin = !child && argc > 1 && std::strchr(argv[1], 'p');
	TaskScheduler::Config cfg;
	cfg.mode    = pin ? Mode::Pinned : Mode::Migrate;
	cfg.main    = MainMode::OutOfPool;
	cfg.workers = 4;
	cfg.timers  = true;
	TaskScheduler::Init(cfg);

	if (child) {
		Periodic h = Periodic::Start(5 * kMs, &JoinSelf, nullptr);
		g_self = &h;
		SleepMs(2000);
		std::printf("CHILD DID NOT ABORT\n");
		std::_Exit(0);
	}
	std::printf("periodic_test fibers=%s\n", pin ? "Pin" : "Migrate");

	std::printf("[rate: 10 ms for ~600 ms]\n");
	{
		Probe pr;
		const auto t0 = std::chrono::steady_clock::now();
		Periodic h = Periodic::Start(10 * kMs, &Tick, &pr);
		Check(h.Valid(), "started");
		SleepMs(600);
		h.Cancel(); h.Join();
		const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
		const double expect = ms / 10.0;
		std::printf("  runs=%ld skipped=%ld expected~%.0f\n", pr.runs.load(), pr.skippedSeen.load(), expect);
		Check(pr.maxLive.load() == 1, "never more than one live instance");
		Check(pr.runs.load() + pr.skippedSeen.load() >= expect - 3 && pr.runs.load() + pr.skippedSeen.load() <= expect + 1,
		      "runs + skipped match elapsed / interval (on the grid, no drift)");
	}

	std::printf("[coalescing: 5 ms interval, 23 ms callback]\n");
	{
		Probe pr; pr.sleepMs = 23;
		const auto t0 = std::chrono::steady_clock::now();
		Periodic h = Periodic::Start(5 * kMs, &Tick, &pr);
		SleepMs(700);
		h.Cancel();
		const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
		h.Join();
		const double expect = ms / 5.0 - 1.0;   // grid points reached by Cancel (first is one interval in)
		std::printf("  runs=%ld skipped=%ld total=%ld expected~%.0f (lifetime Skipped()=%llu)\n",
			pr.runs.load(), pr.skippedSeen.load(), pr.runs.load() + pr.skippedSeen.load(), expect,
			(unsigned long long)h.Skipped());
		Check(pr.maxLive.load() == 1, "a slow callback never overlaps itself");
		Check(pr.skippedSeen.load() >= pr.runs.load() * 3, "the callback is told it lost ticks (~4 per run)");
		const double total = double(pr.runs.load() + (long)h.Skipped());
		Check(total >= expect - 3 && total <= expect + 3, "runs + skipped (lifetime) still track the grid");
	}

	std::printf("[a long stall is caught up in one step]\n");
	{
		const auto t0 = std::chrono::steady_clock::now();
		Periodic h = Periodic::Start(1 * kMs, &Stall, nullptr);
		while (g_stallStage.load() != 2 && std::chrono::steady_clock::now() - t0 < std::chrono::seconds(3)) SleepMs(1);
		// Grid points between the two starts, minus the one that started the second run.
		const long gap = (long)((g_nextStartNs.load() - g_stallStartNs.load()) / kMs);
		std::printf("  skipped reported after the stall: %ld (gap between starts %ld ms)\n", g_afterStallSkipped.load(), gap);
		Check(g_afterStallSkipped.load() >= 290 && std::labs(g_afterStallSkipped.load() - (gap - 1)) <= 3,
		      "the whole stall reported once, matching the measured gap");
		h.Cancel(); h.Join();
	}

	std::printf("[fn returns false: it stops itself]\n");
	{
		Probe pr; pr.stopAfter = 5;
		Periodic h = Periodic::Start(2 * kMs, &Tick, &pr);
		SleepMs(200);
		Check(pr.runs.load() == 5, "exactly 5 runs");
		SleepMs(50);
		Check(pr.runs.load() == 5, "and no more afterwards");
		h.Cancel(); h.Join();   // harmless after self-stop
	}

	std::printf("[Cancel + Join: safe to free the context, even mid-run]\n");
	{
		int bad = 0;
		for (int i = 0; i < 30; ++i) {
			auto* pr = new Probe(); pr->sleepMs = 7;
			Periodic h = Periodic::Start(2 * kMs, &Tick, pr, 0);
			SleepMs(i % 5 + 1);
			h.Cancel(); h.Join();
			const long runsAtJoin = pr->runs.load();
			if (pr->live.load() != 0) ++bad;
			pr->magic = 0;                  // "freed": a later call would abort
			SleepMs(10);
			if (pr->runs.load() != runsAtJoin) ++bad;
			delete pr;
		}
		Check(bad == 0, "no instance was live after Join, none ran after it");
	}

	std::printf("[dropping the handle cancels and joins]\n");
	{
		Probe pr; pr.sleepMs = 3;
		{ Periodic h = Periodic::Start(2 * kMs, &Tick, &pr); SleepMs(40); }
		const long at = pr.runs.load();
		SleepMs(40);
		Check(at > 0 && pr.runs.load() == at && pr.live.load() == 0, "no runs after the handle was destroyed");
	}

	std::printf("[Detach: runs until fn returns false]\n");
	{
		static Probe pr; pr.stopAfter = 4;
		{ Periodic h = Periodic::Start(2 * kMs, &Tick, &pr); h.Detach(); }
		SleepMs(200);
		Check(pr.runs.load() == 4, "a detached task ran to its own stop");
	}

	std::printf("[rejected at registration]\n");
	{
		Check(!Periodic::Start(0, &Tick, nullptr).Valid(), "interval 0 rejected");
		Check(!Periodic::Start(-5, &Tick, nullptr).Valid(), "negative interval rejected");
		Check(!Periodic::Start(kMs, nullptr, nullptr).Valid(), "null fn rejected");
	}

	std::printf("[Join from inside the callback aborts]\n");
	{
		const JLibTest::ChildResult r = JLibTest::RunSelf("child", argv[0]);
		if (!r.started) Check(false, "could not start child");
		else            Check(r.aborted, "the child aborted");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::_Exit(g_fail ? 1 : 0);
}
