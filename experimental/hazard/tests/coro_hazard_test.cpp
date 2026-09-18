// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "TaskScheduler.h"
#include "Coroutine.h"
#include "Hazard.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int  g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-70s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

namespace {

constexpr std::uint64_t kAlive = 0xA11FE0000A11FE00ull;
constexpr std::uint64_t kDead  = 0xDEADDEADDEADDEADull;

struct Node {
    std::uint64_t magic = kAlive;
    ~Node() { magic = kDead; }
};

std::atomic<int>   g_freed{ 0 };
std::atomic<Node*> g_head{ nullptr };
SchedulerMutex     g_gate;
std::atomic<bool>  g_published{ false };
std::atomic<bool>  g_resumed{ false };
std::atomic<bool>  g_sawAlive{ false };

void RetireNode(Node* n) {
    HazardDomain::Instance().Retire(n, [](void* p) {
        g_freed.fetch_add(1, std::memory_order_relaxed);
        delete static_cast<Node*>(p);
    });
}

Coro Reader() {
    HazardGuard g;
    Node* n = g.Protect(0, g_head);
    g_published.store(true, std::memory_order_release);

    co_await LockAsync(g_gate);
    g_gate.Unlock();

    g_sawAlive.store(n->magic == kAlive, std::memory_order_release);
    g_resumed.store(true, std::memory_order_release);
    co_return;
}

} 

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    TaskScheduler::EnableIoReactor(false);
    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();
    std::printf("coroutine hazard pointers -- workers=%zu\n\n", sched.GetWorkerCount());

    g_head.store(new Node{}, std::memory_order_release);
    g_gate.Lock();                       

    WaitGroup wg;
    Spawn(Reader(), &wg);

    for (int i = 0; i < 2000 && !g_published.load(std::memory_order_acquire); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    Check(g_published.load(std::memory_order_acquire),
          "the coroutine published a hazard before suspending");

    Node* victim = g_head.exchange(nullptr, std::memory_order_acq_rel);
    RetireNode(victim);
    HazardDomain::Instance().Scan();
    Check(g_freed.load() == 0,
          "a node named by a SUSPENDED coroutine frame survives a retire+scan");

    g_gate.Unlock();
    sched.WaitFor(wg);
    Check(g_resumed.load(std::memory_order_acquire) && g_sawAlive.load(std::memory_order_acquire),
          "the frame resumed and its node was still intact");

    HazardDomain::Instance().Scan();
    HazardDomain::Instance().Scan();
    Check(g_freed.load() == 1,
          "once the frame is destroyed the record is released and the node IS freed");

    {
        g_freed.store(0, std::memory_order_relaxed);
        g_published.store(false, std::memory_order_relaxed);
        g_head.store(new Node{}, std::memory_order_release);
        g_gate.Lock();                                  

        CancelScope scope;
        WaitGroup   wg2;
        Spawn(Reader(), &wg2, 0, CorePref::Default, scope.Token().Raw());

        for (int i = 0; i < 2000 && !g_published.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        Node* v2 = g_head.exchange(nullptr, std::memory_order_acq_rel);
        RetireNode(v2);
        HazardDomain::Instance().Scan();
        Check(g_freed.load() == 0, "still protected while the frame is suspended and not yet cancelled");

        scope.Cancel();
        g_gate.Unlock();
        sched.WaitFor(wg2);

        HazardDomain::Instance().Scan();
        HazardDomain::Instance().Scan();
        Check(g_freed.load() == 1,
              "a CANCELLED suspended frame released its record, and the node is freed");
        (void)0;
    }

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
