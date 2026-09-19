// Reclamation (epochs, hazard pointers, Ref<T>). Arg: p = Pin fibers (default Migrate);
// "child" = hazard-across-suspend probe (must abort).
#include <TaskScheduler.h>
#include <Thread.h>
#include <Hazard.h>
#include <Ref.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>
#include "spawn_self.h"

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	std::fflush(stdout);
	if (!ok) ++g_fail;
}
static bool WaitUntil(bool (*pred)(), int ms) {
	const auto t0 = std::chrono::steady_clock::now();
	while (!pred() && std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(ms))
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	return pred();
}

// ---- 1. retire storm: everything retired by tasks is freed once the pool is idle ----
static std::atomic<long> g_retired{ 0 }, g_freed{ 0 };
static void CountedDelete(void* p) { delete static_cast<int*>(p); g_freed.fetch_add(1); }
static void RetireSome(void* n) {
	auto& em = EpochManager::Instance();
	for (intptr_t i = 0; i < (intptr_t)n; ++i) {
		{ EpochGuard g; }   // readers come and go
		em.RetirePtr(new int(1), em.CurrentEpoch(), &CountedDelete);
		g_retired.fetch_add(1);
	}
}

// ---- 2. an object protected by a live guard is not freed ----
static std::atomic<bool> g_unlinkDone{ false }, g_protFreed{ false };
static void ProtDelete(void* p) { *static_cast<int*>(p) = 0; delete static_cast<int*>(p); g_protFreed.store(true); }
static std::atomic<int*> g_protSlot{ nullptr };
static void UnlinkAndRetire(void*) {
	int* p = g_protSlot.exchange(nullptr);
	auto& em = EpochManager::Instance();
	em.RetirePtr(p, em.CurrentEpoch(), &ProtDelete);
	RetireSome((void*)(intptr_t)5000);   // pressure: many gates while the guard is up
	g_unlinkDone.store(true);
}

// ---- 3. Ref<T> across suspension and threads ----
struct Obj : RefCounted {
	static std::atomic<long> made, destroyed;
	uint64_t magic = 0xA11CE;
	Obj() { made.fetch_add(1); }
	~Obj() { magic = 0xDEAD; destroyed.fetch_add(1); }
	// Keep the memory so a use after destruction is visible as magic == 0xDEAD.
	static void operator delete(void*) noexcept {}
};
std::atomic<long> Obj::made{ 0 }, Obj::destroyed{ 0 };
static std::atomic<Obj*> g_shared{ nullptr };
static std::atomic<long> g_sawDead{ 0 }, g_acquired{ 0 }, g_empty{ 0 }, g_moved{ 0 };
static std::atomic<bool> g_stopWriter{ false };

static void Nop(void*) {}
static void Holder(void*) {
	for (int k = 0; k < 20; ++k) {
		Ref<Obj> r;
		{
			EpochGuard g;
			r = Ref<Obj>::Acquire(g, g_shared);
		}
		if (!r) { g_empty.fetch_add(1); Thread::CoYield(); continue; }
		g_acquired.fetch_add(1);
		Thread* before = Thread::GetCurrent();
		for (int y = 0; y < 3; ++y) {
			Thread::CoYield();
			if (r->magic != 0xA11CE) g_sawDead.fetch_add(1);
			// Wait on a tiny task sent to ANOTHER worker: in Migrate the resume lands on the worker
			// that finished it. (A plain Push from a worker stays on its own deque, so the holder
			// would usually resume where it was and the migration this test needs would be rare.)
			auto& s = TaskScheduler::Instance();
			WaitGroup wg; wg.n.store(1);
			Task* t = s.CreateTask(&Nop, nullptr);
			t->waitGroup = &wg;
			const size_t n = s.GetWorkerCount();
			Thread* cur = Thread::GetCurrent();
			const size_t other = (cur && cur->IsPoolWorker()) ? ((size_t)cur->qIndex + 1) % n : 0;
			s.PushBatch(&t, 1, other, 0);
			s.WaitFor(wg);
			if (r->magic != 0xA11CE) g_sawDead.fetch_add(1);
		}
		if (Thread::GetCurrent() != before) g_moved.fetch_add(1);
		Ref<Obj> copy = r;       // copies are fine: r already holds a reference
		if (copy->magic != 0xA11CE) g_sawDead.fetch_add(1);
	}
}
static void Writer(void*) {
	while (!g_stopWriter.load()) {
		Ref<Obj> fresh = Ref<Obj>::Make();
		Obj* old = g_shared.exchange(fresh.ReleaseToShared());
		if (old) { Ref<Obj> gone = Ref<Obj>::AdoptFromShared(old); }   // drops the structure's reference
		Thread::CoYield();
	}
}

// ---- 4. child: a fiber yielding while it holds a hazard guard ----
static std::atomic<void*> g_hzSrc{ nullptr };
static void HazardAcrossYield(void*) {
	HazardGuard g;
	g.Protect(0, g_hzSrc);
	Thread::CoYield();
}

// ---- hazard pointers ----
static std::atomic<int*> g_hzProtSlot{ nullptr };
static std::atomic<bool> g_hzProtFreed{ false };
static std::atomic<long> g_hzRetired{ 0 }, g_hzFreed{ 0 };
static void HzProtDelete(void* p) { *static_cast<int*>(p) = 0; delete static_cast<int*>(p); g_hzProtFreed.store(true); }
static void HzCountedDelete(void* p) { delete static_cast<int*>(p); g_hzFreed.fetch_add(1); }
static void HazardUnlinkAndRetire(void*) {
	HazardRetire(g_hzProtSlot.exchange(nullptr), &HzProtDelete);
	for (int i = 0; i < 4000; ++i) {   // past the scan threshold several times
		{ HazardGuard g; }
		HazardRetire(new int(1), &HzCountedDelete);
		g_hzRetired.fetch_add(1);
	}
}
static bool HzProtFreed() { return g_hzProtFreed.load(); }
static bool HzAllFreed() { return g_hzFreed.load() == g_hzRetired.load(); }

static bool Idle1() { return g_freed.load() == g_retired.load(); }
static bool ProtFreed() { return g_protFreed.load(); }
static bool RefsFreed() { return Obj::destroyed.load() == Obj::made.load(); }

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	const char* a = argc > 1 ? argv[1] : "m";
	const bool child = std::strcmp(a, "child") == 0;
	const bool pin = !child && std::strchr(a, 'p') != nullptr;
	TaskScheduler::Init(pin ? Mode::Pinned : Mode::Migrate, MainMode::OutOfPool, 4);
	auto& s = TaskScheduler::Instance();

	if (child) {
		static int dummy = 0;
		g_hzSrc.store(&dummy);
		WaitGroup wg; wg.n.store(1);
		Task* t = s.CreateTask(&HazardAcrossYield, nullptr);
		t->waitGroup = &wg; s.Push(t);
		s.WaitFor(wg);
		std::printf("CHILD DID NOT ABORT\n");
		std::_Exit(0);
	}

	const bool gates = true;   // one reclamation policy
	std::printf("reclaim_test fibers=%s workers=%zu\n", pin ? "Pin" : "Migrate", s.GetWorkerCount());

	std::printf("[retire storm, then idle]\n");
	{
		WaitGroup wg;
		const int N = 64;
		wg.n.store(N * 2);
		for (int i = 0; i < N; ++i) {
			Task* f = s.CreateTask(&RetireSome, (void*)(intptr_t)(i * 7 + 1));   // fiber
			f->waitGroup = &wg; s.Push(f);
			Task* l = s.CreateTask([i] { RetireSome((void*)(intptr_t)(i * 5 + 1)); });   // direct lambda
			l->waitGroup = &wg; s.Push(l);
		}
		s.WaitFor(wg);
		const bool all = WaitUntil(&Idle1, 3000);
		std::printf("  retired=%ld freed=%ld\n", g_retired.load(), g_freed.load());
		if (gates) Check(all, "everything retired was freed with the pool idle (no shutdown)");
		else std::printf("  (not checked: in this mode partial per-thread bags wait for thread exit)\n");
	}

	std::printf("[a live guard keeps its object]\n");
	{
		// Main is the reader (a spinning worker would strand work queued behind it in Pin mode).
		g_protSlot.store(new int(7));
		{
			EpochGuard g;
			int* p = g_protSlot.load();
			Task* u = s.CreateTask(&UnlinkAndRetire, nullptr);
			s.Push(u);
			while (!g_unlinkDone.load()) { if (*p != 7) std::abort(); std::this_thread::yield(); }
			std::this_thread::sleep_for(std::chrono::milliseconds(100));   // workers gate meanwhile
			Check(!g_protFreed.load(), "not freed while the reader's guard is up (despite gate pressure)");
			Check(*p == 7, "and still readable");
		}
		Check(WaitUntil(&ProtFreed, 3000), "freed after the guard dropped");
	}

	std::printf("[hazard pointers: a protected pointer survives scans, then is freed]\n");
	{
		g_hzProtSlot.store(new int(9));
		{
			HazardGuard g;                      // main's external row
			int* p = g.Protect(0, g_hzProtSlot);
			WaitGroup wg; wg.n.store(1);
			Task* u = s.CreateTask(&HazardUnlinkAndRetire, nullptr);   // plus 4000 more: scans run
			u->waitGroup = &wg; s.Push(u);
			s.WaitFor(wg);
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
			Check(!g_hzProtFreed.load(), "the protected pointer survived the worker's scans");
			Check(*p == 9, "and is still readable");
		}   // guard cleared here; the pointer now sits in an orphan list or a worker's list
		Check(WaitUntil(&HzProtFreed, 3000), "freed after the guard cleared (idle gate / orphan sweep)");
		Check(WaitUntil(&HzAllFreed, 3000), "every other hazard retire was freed with the pool idle");
		std::printf("  hazard retired=%ld freed=%ld\n", g_hzRetired.load(), g_hzFreed.load());
	}

	std::printf("[Ref<T>: acquire under a guard, hold across yields and threads]\n");
	{
		{ Ref<Obj> first = Ref<Obj>::Make(); g_shared.store(first.ReleaseToShared()); }
		WaitGroup wwg; wwg.n.store(2);
		for (int i = 0; i < 2; ++i) { Task* w = s.CreateTask(&Writer, nullptr); w->waitGroup = &wwg; s.Push(w); }
		WaitGroup hwg;
		const int H = 64;
		hwg.n.store(H);
		for (int i = 0; i < H; ++i) { Task* h = s.CreateTask(&Holder, nullptr); h->waitGroup = &hwg; s.Push(h); }
		s.WaitFor(hwg);
		g_stopWriter.store(true);
		s.WaitFor(wwg);
		{ Ref<Obj> last = Ref<Obj>::AdoptFromShared(g_shared.exchange(nullptr)); }
		std::printf("  acquired=%ld empty=%ld moved=%ld made=%ld\n", g_acquired.load(), g_empty.load(), g_moved.load(), Obj::made.load());
		Check(g_sawDead.load() == 0, "no holder ever saw a destroyed object");
		Check(g_acquired.load() > 0, "holders did acquire");
		if (pin) Check(g_moved.load() == 0, "Pin: holders stayed on their worker");
		else     Check(g_moved.load() > 0, "Migrate: holders did change worker while holding");
		// Main released the last Ref into its own bag; main clears it at its own sweep points
		// (here: a WaitFor), so poll through one.
		bool all = RefsFreed();
		for (int i = 0; i < (gates ? 3000 : 300) && !all; ++i) {
			WaitGroup sp; sp.n.store(1);
			Task* t = s.CreateTask(&Nop, nullptr);
			t->waitGroup = &sp; s.Push(t);
			s.WaitFor(sp);
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			all = RefsFreed();
		}
		std::printf("  destroyed=%ld\n", Obj::destroyed.load());
		Check(Obj::destroyed.load() <= Obj::made.load(), "no object destroyed twice");
		if (gates) Check(all, "every object was destroyed after its last Ref, with the pool idle");
	}

	if (gates) {
		std::printf("[Gates: a fiber may not hold a HazardGuard across a yield]\n");
		const JLibTest::ChildResult r = JLibTest::RunSelf("child", argv[0]);
		if (!r.started) Check(false, "could not start child");
		else            Check(r.aborted, "the child aborted");
	}

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
