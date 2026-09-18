// A TaskDAG destroyed without Submit gives back every node and every task it built.
//
// Submit is what empties the graph's node list, so a graph that is built and then dropped used to
// leak all of it: the nodes, and the tasks (with their records) the nodes held. Measured as slab
// slots still live after the graph is gone and reclamation has run.
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <Reclaimer.h>
#include <cstdio>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

static void Nop(void*) {}

static long long Live() {
	const auto u = TaskScheduler::SlabUsage();
	return u.c64.live + u.c80.live + u.c128.live + u.c256.live + u.c512.live;
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	TaskScheduler::Config cfg;
	cfg.workers = 2;
	cfg.main    = MainMode::OutOfPool;
	TaskScheduler::Init(cfg);
	auto& s = TaskScheduler::Instance();

	for (int i = 0; i < 16; ++i) Reclaimer::Flush();
	const long long before = Live();

	constexpr int kGraphs = 50, kNodes = 40;
	for (int g = 0; g < kGraphs; ++g) {
		TaskDAG dag(s);
		TaskNode* prev = nullptr;
		for (int i = 0; i < kNodes; ++i) {
			TaskNode* n = dag.CreateNode(s.CreateTask(&Nop, nullptr));
			if (prev) dag.AddDependency(n, prev);
			prev = n;
		}
		// dropped here, never submitted
	}
	for (int i = 0; i < 64; ++i) Reclaimer::Flush();
	const long long after = Live();

	std::printf("  live slab slots: %lld before, %lld after %d unsubmitted graphs of %d nodes\n",
		before, after, kGraphs, kNodes);
	// A leak is a node + a task + a record per node: thousands of slots.
	Check(after - before < 64, "an unsubmitted graph frees its nodes and tasks");

	std::printf(g_fail ? "RESULT: %d FAILURE(S)\n" : "RESULT: all checks passed\n", g_fail);
	std::fflush(stdout);
	std::_Exit(g_fail ? 1 : 0);
}
