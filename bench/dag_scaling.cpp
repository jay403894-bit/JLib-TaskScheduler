// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <TaskScheduler.h>
#include <TaskDAG.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <vector>

using Clock = std::chrono::steady_clock;
static double MsSince(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

struct Shape {
    int nodes;
    int fanout;    
};

struct Result {
    double buildMs = 1e300;
    double execMs = 1e300;
    long long edges = 0;
};

static Result RunShape(JLib::TaskScheduler& sched, const Shape& s, int reps) {
    Result best;
    for (int r = 0; r < reps; ++r) {
        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        long long edges = 0;

        const auto tBuild = Clock::now();
        JLib::TaskDAG dag(sched);
        std::vector<JLib::TaskNode*> prevLayer, curLayer;
        int made = 0;

        while (made < s.nodes) {
            const int layerSize = (s.fanout <= 1)
                ? 1
                : ((s.nodes - made) < s.fanout ? (s.nodes - made) : s.fanout);
            curLayer.clear();
            for (int i = 0; i < layerSize && made < s.nodes; ++i, ++made) {
                auto* t = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
                if (!t) { std::printf("  CreateTask returned null -- slab too small\n"); return best; }
                t->waitGroup = &wg;
                wg.n.fetch_add(1, std::memory_order_relaxed);
                auto* n = dag.CreateNode(t);
                if (!n) { std::printf("  CreateNode returned null -- slab too small\n"); return best; }
                for (auto* p : prevLayer) { dag.AddDependency(n, p); ++edges; }
                curLayer.push_back(n);
            }
            prevLayer = curLayer;
        }
        const double buildMs = MsSince(tBuild);

        const auto tExec = Clock::now();
        if (!dag.Submit()) { std::printf("  Submit rejected the graph\n"); return best; }
        sched.WaitFor(wg);
        const double execMs = MsSince(tExec);

        if (ran.load() != s.nodes) std::printf("  WRONG: ran=%d expected=%d\n", ran.load(), s.nodes);
        if (buildMs < best.buildMs) best.buildMs = buildMs;
        if (execMs < best.execMs)  best.execMs = execMs;
        best.edges = edges;
    }
    return best;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    
    JLib::TaskScheduler::Config cfg;
    cfg.mode = JLib::Mode::Migrate;
    cfg.main = JLib::MainMode::OutOfPool;
    cfg.slab = { 1 << 20, 1 << 17, 1 << 17 };
    JLib::TaskScheduler::Init(cfg);
    auto& sched = JLib::TaskScheduler::Instance();

    std::printf("TaskDAG scaling -- workers=%zu\n", sched.GetWorkerCount());
    std::printf("empty payloads: this measures the DAG, not the work\n\n");

    RunShape(sched, Shape{ 64, 4 }, 1);

    const Shape shapes[] = {
        { 256, 1 }, { 256, 4 }, { 256, 16 }, { 256, 64 },
        { 2048, 1 }, { 2048, 4 }, { 2048, 16 }, { 2048, 64 },
        { 8192, 4 }, { 8192, 64 },
    };

    std::printf("%7s %7s %9s %12s %12s %11s %12s\n",
                "nodes", "fanout", "edges", "build ms", "build ns/edge", "exec ms", "exec us/node");
    std::printf("------------------------------------------------------------------------------------\n");
    for (const Shape& s : shapes) {
        const Result r = RunShape(sched, s, 3);
        if (r.buildMs > 1e200) continue;
        const double nsPerEdge = r.edges ? (r.buildMs * 1e6) / double(r.edges) : 0.0;
        std::printf("%7d %7d %9lld %12.3f %12.1f %11.3f %12.3f\n",
                    s.nodes, s.fanout, r.edges, r.buildMs, nsPerEdge,
                    r.execMs, (r.execMs * 1000.0) / double(s.nodes));
    }

    std::printf("\n");
    JLib::Stats::Print();   // needs -DJLIBSCHED_STATS=ON

    std::printf("  fixed-size slab users: Task %zu B, TaskNode %zu B, DagEdge %zu B  (slot is %zu B)\n",
                sizeof(JLib::Task), sizeof(JLib::TaskNode), sizeof(JLib::DagEdge),
                JLib::TaskAllocator::SLOT);
    
    {
        int cap0 = 0; double cap1 = 0; char capBig[64]{};
        auto lEmpty = []{};
        auto lOne   = [&cap0]{ (void)cap0; };
        auto lTwo   = [&cap0, &cap1]{ (void)cap0; (void)cap1; };
        auto lBig   = [capBig]{ (void)capBig[0]; };
        std::printf("  LambdaTask by capture: empty %zu B, 1 ref %zu B, 2 refs %zu B, 64 B capture %zu B\n",
                    sizeof(JLib::LambdaTask<decltype(lEmpty)>), sizeof(JLib::LambdaTask<decltype(lOne)>),
                    sizeof(JLib::LambdaTask<decltype(lTwo)>),   sizeof(JLib::LambdaTask<decltype(lBig)>));
    }

    std::printf("\nread DOWN each block: if exec us/node is flat as fanout grows, walking dependents\n");
    std::printf("is invisible next to dispatch and edge layout is not worth reopening.\n");

    JLib::detail::TeardownForTesting(sched);
    return 0;
}
