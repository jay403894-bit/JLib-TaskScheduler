
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <chrono>
#include <cstdio>
#include <vector>
#include <algorithm>

static void Body(void*) {}

static double TimeSubmit(JLib::TaskScheduler& sched, int N, int iters) {
    std::vector<double> us;
    us.reserve(iters);
    for (int it = 0; it < iters; ++it) {
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

        const auto t0 = std::chrono::steady_clock::now();
        dag.Submit();
        const auto t1 = std::chrono::steady_clock::now();
        us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());

        sched.WaitFor(wg);
    }
    std::sort(us.begin(), us.end());
    return us[us.size() / 2];   
}

static void Report(JLib::TaskScheduler& sched, int N, int iters) {
    std::vector<double> runs;
    for (int r = 0; r < 5; ++r) runs.push_back(TimeSubmit(sched, N, iters));
    std::sort(runs.begin(), runs.end());
    std::printf("%8d %8d   %8.2f  [%.2f .. %.2f]\n",
        N + 2, 2 * N, runs[2], runs.front(), runs.back());
    std::fflush(stdout);
}

int main() {
    JLib::TaskScheduler::Init(4);
    auto& sched = JLib::TaskScheduler::Instance();

    TimeSubmit(sched, 4096, 3);
    TimeSubmit(sched, 4096, 3);

    std::printf("%8s %8s   %8s  %s\n", "nodes", "edges", "median", "[min..max of 5 medians]");
    for (int N : { 64, 256, 1024, 4096 }) Report(sched, N, 51);
    return 0;
}
