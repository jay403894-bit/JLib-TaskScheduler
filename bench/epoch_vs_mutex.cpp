// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "TaskScheduler.h"
#include "Epochs.h"
#include "Thread.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

namespace {

struct Node { Node* next; int payload; };

constexpr int kChain = 8;          

Node*      g_head = nullptr;       
std::mutex g_mutex;                

std::atomic<long long> g_reads{ 0 };
std::atomic<long long> g_retires{ 0 };
std::atomic<bool>      g_stop{ false };

void BuildChain() {
    for (int i = 0; i < kChain; ++i) g_head = new Node{ g_head, i };
}

inline int Walk() {
    int sum = 0;
    for (Node* n = g_head; n; n = n->next) sum += n->payload;
    return sum;
}

std::atomic<int> g_sink{ 0 };

void ReaderEBR() {
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        for (int i = 0; i < 32; ++i) { JLib::EpochGuard g; g_sink.store(Walk(), std::memory_order_relaxed); }
        n += 32;
    }
    g_reads.fetch_add(n, std::memory_order_relaxed);
}

void ReaderMutex() {
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        for (int i = 0; i < 32; ++i) {
            std::lock_guard<std::mutex> lk(g_mutex);
            g_sink.store(Walk(), std::memory_order_relaxed);
        }
        n += 32;
    }
    g_reads.fetch_add(n, std::memory_order_relaxed);
}

void WriterEBR() {
    auto& em = JLib::EpochManager::Instance();
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        auto* victim = new Node{ nullptr, 0 };
        em.RetirePtr(victim, em.CurrentEpoch(), [](void* p) { delete static_cast<Node*>(p); });
        
        if ((++n & 63) == 0) em.Tick();
    }
    g_retires.fetch_add(n, std::memory_order_relaxed);
}

void WriterMutex() {
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        auto* victim = new Node{ nullptr, 0 };
        {
            
            std::lock_guard<std::mutex> lk(g_mutex);
            delete victim;
        }
        ++n;
    }
    g_retires.fetch_add(n, std::memory_order_relaxed);
}

struct Result { double reads, retires; };

Result Run(void (*reader)(), void (*writer)(), int nreaders, int ms) {
    g_reads.store(0); g_retires.store(0); g_stop.store(false);
    auto& sched = JLib::TaskScheduler::Instance();
    for (int i = 0; i < nreaders; ++i)
        if (auto* t = sched.CreateTask([reader] { reader(); })) sched.Push(t);
    if (auto* t = sched.CreateTask([writer] { writer(); })) sched.Push(t);
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    g_stop.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(80));   
    const double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    return { g_reads.load() / s, g_retires.load() / s };
}

double Median(std::vector<double> v) { std::sort(v.begin(), v.end()); return v[v.size() / 2]; }

} 

int main(int argc, char** argv) {
    const int reps = (argc > 1) ? std::atoi(argv[1]) : 3;
    const int ms   = (argc > 2) ? std::atoi(argv[2]) : 150;

    JLib::TaskScheduler::Init(0);
    BuildChain();

    std::printf("epochs vs a plain mutex -- %d-node chain per read, %d ms, %d interleaved reps\n",
                kChain, ms, reps);
    std::printf("(the mutex arm needs NO deferred reclamation at all -- it frees under the lock)\n\n");
    std::printf("  readers        EBR reads/s      mutex reads/s   ratio      EBR retires/s   mutex retires/s\n");

    for (int r : { 1, 2, 4, 8, 16 }) {
        std::vector<double> er, mr, ew, mw;
        for (int i = 0; i < reps; ++i) {
            
            const Result e = Run(&ReaderEBR,   &WriterEBR,   r, ms);
            const Result m = Run(&ReaderMutex, &WriterMutex, r, ms);
            er.push_back(e.reads); ew.push_back(e.retires);
            mr.push_back(m.reads); mw.push_back(m.retires);
        }
        const double e = Median(er), m = Median(mr);
        std::printf("  %7d  %16.0f  %16.0f  %6.1fx  %14.0f  %14.0f\n",
                    r, e, m, (e > m ? e / m : -(m / e)), Median(ew), Median(mw));
    }

    std::printf("\n  A NEGATIVE ratio means the MUTEX won. Expect that at one reader: uncontended,\n"
                "  the lock is a couple of atomics and the epoch machinery is pure overhead.\n"
                "  The question is where it crosses, and whether the pool ever runs below it.\n");

    JLib::detail::TeardownForTesting(JLib::TaskScheduler::Instance());
    return 0;
}
