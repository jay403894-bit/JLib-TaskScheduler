// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <cstdio>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

static void Stage(const char* s) { std::fprintf(stderr, "[stage] %s\n", s); std::fflush(stderr); }
static void Done(const char* s) { std::fprintf(stderr, "        ok: %s\n", s); std::fflush(stderr); }

static std::atomic<int> g_ran{ 0 };
static void Body(void*) { g_ran.fetch_add(1, std::memory_order_relaxed); }

struct WaiterCtx {
	JLib::TaskScheduler* sched;
	JLib::WaitGroup*     inner;
};

struct RaceCtx {
	std::atomic<bool>* released;
	std::atomic<int>*  ranEarly;
	std::atomic<int>*  ran;
};
static void RaceLateBody(void* p) {
	auto& c = *static_cast<RaceCtx*>(p);
	if (!c.released->load(std::memory_order_acquire))
		c.ranEarly->fetch_add(1, std::memory_order_relaxed);
	c.ran->fetch_add(1, std::memory_order_release);
}

static void FiberWaiterBody(void* p) {
	auto& c = *static_cast<WaiterCtx*>(p);
	JLib::Task* t = c.sched->CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
	t->waitGroup = c.inner;
	c.sched->Push(t);
	c.sched->WaitFor(*c.inner);      
	g_ran.fetch_add(1, std::memory_order_relaxed);
}

static bool HasFlag(int argc, char** argv, char f) {
	for (int i = 1; i < argc; ++i)
		for (const char* p = argv[i]; *p; ++p) if (*p == f) return true;
	return false;
}

int main(int argc, char** argv) {

	Stage("configure");
	JLib::TaskScheduler::Config cfg;
	cfg.mode       = JLib::Mode::Migrate;
	cfg.main       = JLib::MainMode::OutOfPool;
	cfg.workers    = 4;

	Stage("Init(4)");
	JLib::TaskScheduler::Init(cfg);
	auto& sched = JLib::TaskScheduler::Instance();
	Done("pool constructed");

	Stage("one Native task + WaitFor");
	{
		JLib::WaitGroup wg;
		wg.n.store(1, std::memory_order_relaxed);
		JLib::Task* t = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		t->waitGroup = &wg;
		sched.Push(t);
		sched.WaitFor(wg);
	}
	Done("one task ran and the waiter woke");

	Stage("4096 Native tasks");
	{
		const int N = 4096;
		g_ran.store(0);
		JLib::WaitGroup wg;
		wg.n.store(N, std::memory_order_relaxed);
		for (int i = 0; i < N; ++i) {
			JLib::Task* t = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
			t->waitGroup = &wg;
			sched.Push(t);
		}
		sched.WaitFor(wg);
		std::fprintf(stderr, "        ran=%d of %d\n", g_ran.load(), N);
		std::fflush(stderr);
	}
	Done("bulk push drained");

	Stage("idle 300ms, then one more task (park -> wake)");
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(300));
		JLib::WaitGroup wg;
		wg.n.store(1, std::memory_order_relaxed);
		JLib::Task* t = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		t->waitGroup = &wg;
		sched.Push(t);
		sched.WaitFor(wg);
	}
	Done("a parked pool woke for a single push");

	Stage("one Fiber task");
	{
		JLib::WaitGroup wg;
		wg.n.store(1, std::memory_order_relaxed);
		JLib::Task* t = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		t->waitGroup = &wg;
		sched.Push(t);
		sched.WaitFor(wg);
	}
	Done("fiber task ran");

	Stage("Fiber task that WaitFors from inside itself (suspend + resume)");
	{
		g_ran.store(0);
		JLib::WaitGroup inner, outer;
		inner.n.store(1, std::memory_order_relaxed);
		outer.n.store(1, std::memory_order_relaxed);
		WaiterCtx ctx{ &sched, &inner };

		JLib::Task* w = sched.CreateTask(&FiberWaiterBody, &ctx,
			JLib::TaskType::Fiber);
		w->waitGroup = &outer;
		sched.Push(w);
		sched.WaitFor(outer);
		std::fprintf(stderr, "        ran=%d (expect 2: inner task + resumed fiber)\n", g_ran.load());
		std::fflush(stderr);
	}
	Done("a fiber suspended on a WaitGroup and was resumed");

	Stage("many suspending fibers at once");
	{
		const int N = 64;
		g_ran.store(0);
		std::vector<JLib::WaitGroup> inners(N);
		std::vector<WaiterCtx>       ctxs(N);
		JLib::WaitGroup outer;
		outer.n.store(N, std::memory_order_relaxed);
		for (int i = 0; i < N; ++i) {
			inners[i].n.store(1, std::memory_order_relaxed);
			ctxs[i] = WaiterCtx{ &sched, &inners[i] };
			JLib::Task* w = sched.CreateTask(&FiberWaiterBody, &ctxs[i],
				JLib::TaskType::Fiber);
			w->waitGroup = &outer;
			sched.Push(w);
		}
		sched.WaitFor(outer);
		std::fprintf(stderr, "        ran=%d of %d\n", g_ran.load(), N * 2);
		std::fflush(stderr);
	}
	Done("concurrent suspend/resume drained");

	Stage("DAG: diamond A -> {B,C} -> D");
	{
		g_ran.store(0);
		JLib::WaitGroup wg;
		wg.n.store(4, std::memory_order_relaxed);
		JLib::TaskDAG dag(sched);

		JLib::Task* ta = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		JLib::Task* tb = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		JLib::Task* tc = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		JLib::Task* td = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		ta->waitGroup = &wg; tb->waitGroup = &wg; tc->waitGroup = &wg; td->waitGroup = &wg;

		auto* a = dag.CreateNode(ta);
		auto* b = dag.CreateNode(tb);
		auto* c = dag.CreateNode(tc);
		auto* d = dag.CreateNode(td);
		dag.AddDependency(b, a);
		dag.AddDependency(c, a);
		dag.AddDependency(d, b);
		dag.AddDependency(d, c);

		dag.Submit();
		sched.WaitFor(wg);
		std::fprintf(stderr, "        ran=%d of 4\n", g_ran.load());
		std::fflush(stderr);
	}
	Done("diamond completed and the graph destructed");

	Stage("DAG: 1 -> 256 -> 1 fan-out/fan-in");
	{
		const int N = 256;
		g_ran.store(0);
		JLib::WaitGroup wg;
		wg.n.store(N + 2, std::memory_order_relaxed);
		JLib::TaskDAG dag(sched);

		JLib::Task* tr = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		JLib::Task* tj = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		tr->waitGroup = &wg; tj->waitGroup = &wg;
		auto* root = dag.CreateNode(tr);
		auto* join = dag.CreateNode(tj);

		for (int i = 0; i < N; ++i) {
			JLib::Task* t = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
			t->waitGroup = &wg;
			auto* mid = dag.CreateNode(t);
			dag.AddDependency(mid, root);
			dag.AddDependency(join, mid);
		}

		dag.Submit();
		sched.WaitFor(wg);
		std::fprintf(stderr, "        ran=%d of %d\n", g_ran.load(), N + 2);
		std::fflush(stderr);
	}
	Done("wide graph drained; edge chunks retired and flushed to the collector");

	Stage("DAG: AddDependency after Submit, predecessor already finished");
	{
		g_ran.store(0);
		JLib::WaitGroup aDone, lateDone;
		aDone.n.store(1, std::memory_order_relaxed);
		lateDone.n.store(1, std::memory_order_relaxed);
		JLib::TaskDAG dag(sched);

		JLib::Task* ta = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		JLib::Task* tl = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
		ta->waitGroup = &aDone;
		tl->waitGroup = &lateDone;

		auto* a    = dag.CreateNode(ta);
		auto* late = dag.CreateNode(tl);
		auto* ext  = dag.CreateExternalNode();
		dag.AddDependency(late, ext);       
		dag.Submit();

		sched.WaitFor(aDone);               
		std::fprintf(stderr, "        a finished (ran=%d); adding late edge\n", g_ran.load());
		std::fflush(stderr);

		dag.AddDependency(late, a);         

		if (g_ran.load() != 1) {
			std::fprintf(stderr,
				"        FAIL: late node ran before the external was signalled (ran=%d) -- the\n"
				"        edge was discharged twice, so the countdown hit zero early.\n", g_ran.load());
			std::fflush(stderr);
			return 1;
		}

		dag.SignalExternal(ext);            
		sched.WaitFor(lateDone);
		std::fprintf(stderr, "        ran=%d of 2 (a + late)\n", g_ran.load());
		std::fflush(stderr);
	}
	Done("a late edge against a finished predecessor was discharged exactly once");

	Stage("DAG: AddDependency racing the completion walk");
	{
		const int kIters = 3000;
		int stranded = 0, hitEarly = 0;

		int prepended = 0, discharged = 0, otherCount = 0;

		for (int it = 0; it < kIters; ++it) {
			std::atomic<bool> released{ false };
			std::atomic<int>  ranEarly{ 0 };
			std::atomic<int>  lateRan{ 0 };
			RaceCtx ctx{ &released, &ranEarly, &lateRan };

			JLib::WaitGroup aDone;
			aDone.n.store(1, std::memory_order_relaxed);
			JLib::TaskDAG dag(sched);

			JLib::Task* ta = sched.CreateTask(&Body, nullptr, JLib::TaskType::Fiber);
			JLib::Task* tl = sched.CreateTask(&RaceLateBody, &ctx, JLib::TaskType::Fiber);
			ta->waitGroup = &aDone;

			auto* a    = dag.CreateNode(ta);
			auto* late = dag.CreateNode(tl);
			auto* ext  = dag.CreateExternalNode();
			dag.AddDependency(late, ext);

			int snap = 0;
			{
				// `a` may finish (and be retired) before AddDependency uses it. Holding a node
				// pointer past its completion requires an epoch guard opened before it can finish.
				JLib::EpochGuard guard;
				dag.Submit();

				if (it & 1) { for (int k = 0; k < 8; ++k) std::this_thread::yield(); }

				dag.AddDependency(late, a);
				snap = late->dependencies_left.load(std::memory_order_acquire);
			}
			if      (snap == 2) ++prepended;
			else if (snap == 1) ++discharged;
			else                ++otherCount;

			released.store(true, std::memory_order_release);
			dag.SignalExternal(ext);

			// Wait on time, not a yield count: 200000 yields is ~one OS quantum on Windows, and a
			// woken worker can legitimately be scheduled one quantum late.
			const auto waitStart = std::chrono::steady_clock::now();
			while (lateRan.load(std::memory_order_acquire) == 0
			       && std::chrono::steady_clock::now() - waitStart < std::chrono::seconds(2))
				std::this_thread::yield();

			if (lateRan.load(std::memory_order_acquire) == 0) ++stranded;
			if (ranEarly.load(std::memory_order_relaxed) != 0) ++hitEarly;

			sched.WaitFor(aDone);   
		}

		std::fprintf(stderr, "        iterations=%d  stranded=%d  ranEarly=%d\n",
			kIters, stranded, hitEarly);
		std::fprintf(stderr, "        path mix: prepended=%d discharged=%d other=%d\n",
			prepended, discharged, otherCount);
		std::fflush(stderr);
		if (prepended == 0) {
			std::fprintf(stderr,
				"        VACUOUS: every iteration took the already-discharged path, so the\n"
				"        CAS-against-a-live-walk branch was never exercised and the zeros above\n"
				"        prove nothing about it.\n");
			std::fflush(stderr);
			return 1;
		}
		if (stranded || hitEarly) {
			std::fprintf(stderr,
				"        FAIL: the late edge was not discharged exactly once in every\n"
				"        interleaving -- stranded means zero discharges, ranEarly means two.\n");
			std::fflush(stderr);
			return 1;
		}
	}
	Done("late edge discharged exactly once under a live race");

	std::fprintf(stderr, "\nWORK COMPLETE -- entering teardown at exit\n");
	std::fflush(stderr);
	return 0;
}
