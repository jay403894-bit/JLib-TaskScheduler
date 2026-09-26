// ThreadScope: a thread outside the pool gets an epoch slot, a hazard row and a retire bag.
//
// 1. Scoped threads retire through epochs while the pool runs; after the scopes end every retired
//    object is freed (their leftovers went to the orphan store, which the pool sweeps).
// 2. 200 short-lived scoped threads, one after another, each taking a HazardGuard: more than the
//    16 epoch slots and the 64 hazard rows, so both must be given back at the scope's end.
// 3. A scope on main is a no-op; a nested scope is a no-op.
// 4. (child) an EpochGuard on an unscoped thread is fatal -- it used to share main's slot.
// 5. (child) one scope more than Config::externalThreads is fatal.
#include <TaskScheduler.h>
#include <Thread.h>
#include <Reclaimer.h>
#include "spawn_self.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static std::atomic<long long> g_freed{ 0 };
static void CountingDeleter(void* p) { delete static_cast<int*>(p); g_freed.fetch_add(1, std::memory_order_relaxed); }

static void Init() {
	TaskScheduler::Config cfg;
	cfg.workers = 4;
	cfg.main    = MainMode::OutOfPool;
	cfg.externalThreads = 16;
	TaskScheduler::Init(cfg);
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const char* mode = argc > 1 ? argv[1] : "";

	if (std::strcmp(mode, "child-unscoped") == 0) {
		Init();
		std::thread([] { EpochGuard g; (void)g; }).join();
		std::printf("CHILD DID NOT ABORT\n");
		return 0;
	}
	if (std::strcmp(mode, "child-exhaust") == 0) {
		Init();
		std::atomic<int> ready{ 0 };
		std::atomic<bool> release{ false };
		std::vector<std::thread> ts;
		for (int i = 0; i < 17; ++i)
			ts.emplace_back([&] {
				ThreadScope s;
				ready.fetch_add(1);
				while (!release.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
			});
		for (auto& t : ts) t.join();
		std::printf("CHILD DID NOT ABORT\n");
		return 0;
	}

	Init();

	// 1.
	{
		constexpr int kThreads = 8, kEach = 20000;
		std::vector<std::thread> ts;
		for (int t = 0; t < kThreads; ++t)
			ts.emplace_back([kEach] {
				ThreadScope scope;
				EpochManager& em = EpochManager::Instance();
				for (int i = 0; i < kEach; ++i) {
					EpochGuard g;
					em.RetirePtr(new int(i), em.CurrentEpoch(), &CountingDeleter);
				}
			});
		for (auto& t : ts) t.join();
		const long long want = (long long)kThreads * kEach;
		const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(20);
		while (g_freed.load() < want && std::chrono::steady_clock::now() < end) {
			EpochManager::Instance().AdvanceEpoch();
			Reclaimer::Flush();
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		std::printf("  retired %lld from scoped threads, freed %lld\n", want, g_freed.load());
		Check(g_freed.load() == want, "every object retired from a scoped thread was freed");
	}

	// 2.
	{
		bool allClaimed = true;
		for (int i = 0; i < 200; ++i) {
			std::thread([&] {
				ThreadScope scope;
				if (scope.Slot() == kNoThreadSlot) allClaimed = false;
				HazardGuard hg;
				if (hg.Reader() == HazardDomain::kNoReader) allClaimed = false;
			}).join();
		}
		Check(allClaimed, "200 scoped threads in turn each got an epoch slot and a hazard row");
	}

	// 3.
	{
		ThreadScope onMain;
		Check(onMain.Slot() == kNoThreadSlot && CurrentThreadId() == 0, "a scope on main is a no-op");
		bool nestedOk = false;
		std::thread([&] {
			ThreadScope outer;
			{
				ThreadScope inner;
				nestedOk = inner.Slot() == kNoThreadSlot && CurrentThreadId() == outer.Slot();
			}
			nestedOk = nestedOk && CurrentThreadId() == outer.Slot();
		}).join();
		Check(nestedOk, "a nested scope is a no-op and the outer scope keeps its slot");
	}

	// 4 + 5.
	{
		const JLibTest::ChildResult r = JLibTest::RunSelf("child-unscoped", argv[0]);
		Check(r.started && r.aborted, "an EpochGuard on an unscoped thread is fatal");
		const JLibTest::ChildResult r2 = JLibTest::RunSelf("child-exhaust", argv[0]);
		Check(r2.started && r2.aborted, "one scope more than Config::externalThreads is fatal");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
