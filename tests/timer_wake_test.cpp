// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// The timer thread must wake for a new deadline that is earlier than what it sleeps toward, at any
// wheel level, and timers must work again after the pool is torn down and started again.
#include <TaskScheduler.h>
#include <Timer.h>
#include <CancelToken.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static constexpr int64_t kMs = 1'000'000;
static int g_fail = 0;
static void Check(bool ok, const char* what) {
	std::printf("  %s %s\n", ok ? "ok  " : "FAIL", what);
	if (!ok) ++g_fail;
}

struct Fired { std::atomic<long long> atNs{ 0 }; };
static void OnFire(void* ctx, CancelToken) { static_cast<Fired*>(ctx)->atNs.store(MonotonicNs()); }

// Waits up to limitMs for the timer; returns ms from `startNs` to its fire, or -1.
static double WaitFired(const Fired& f, long long startNs, int limitMs) {
	for (int i = 0; i < limitMs; ++i) {
		if (const long long t = f.atNs.load()) return (t - startNs) / 1e6;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return -1;
}

static void InitPool() {
	TaskScheduler::Config cfg;
	cfg.main    = MainMode::OutOfPool;
	cfg.workers = 4;
	cfg.timers  = true;
	TaskScheduler::Init(cfg);
}

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	InitPool();

	std::printf("[300 ms deadline armed while the timer thread waits on an empty wheel]\n");
	{
		// A short one first, so the thread has fired it and gone into its untimed wait.
		CancelScope s0; Fired f0;
		Deadline d0(1 * kMs, s0.Token(), &OnFire, &f0);
		Check(WaitFired(f0, MonotonicNs(), 1000) >= 0, "1 ms deadline fired");
		std::this_thread::sleep_for(std::chrono::milliseconds(20));

		CancelScope s; Fired f;
		const long long t0 = MonotonicNs();
		Deadline d(300 * kMs, s.Token(), &OnFire, &f);
		const double ms = WaitFired(f, t0, 2000);
		std::printf("  fired after %.1f ms\n", ms);
		Check(ms >= 299 && ms < 400, "fired on time (a level-1 deadline wakes the untimed wait)");
	}

	std::printf("[300 ms deadline armed while the thread sleeps toward a 1 s one]\n");
	{
		CancelScope sLong; Fired fLong;
		Deadline dLong(1000 * kMs, sLong.Token(), &OnFire, &fLong);
		std::this_thread::sleep_for(std::chrono::milliseconds(20));

		CancelScope s; Fired f;
		const long long t0 = MonotonicNs();
		Deadline d(300 * kMs, s.Token(), &OnFire, &f);
		const double ms = WaitFired(f, t0, 2000);
		std::printf("  fired after %.1f ms\n", ms);
		Check(ms >= 299 && ms < 400, "fired on time, not at the longer deadline's wake");
	}

	std::printf("[deadlines never fire early]\n");
	{
		double earliest = 1e9;
		for (int i = 0; i < 200; ++i) {
			CancelScope s; Fired f;
			const int delayUs = 300 + (i * 37) % 2700;           // 0.3-3 ms, off the tick grid
			const long long t0 = MonotonicNs();
			Deadline d(delayUs * 1000LL, s.Token(), &OnFire, &f);
			const double ms = WaitFired(f, t0, 1000);
			if (ms >= 0) earliest = std::min(earliest, ms - delayUs / 1000.0);
		}
		std::printf("  earliest fire relative to its deadline: %+.3f ms\n", earliest);
		Check(earliest >= 0, "no deadline fired before now + delay");
	}

	std::printf("[timers after the pool is torn down and started again]\n");
	{
		detail::DestroyForTesting();
		InitPool();
		CancelScope s; Fired f;
		const long long t0 = MonotonicNs();
		Deadline d(20 * kMs, s.Token(), &OnFire, &f);
		const double ms = WaitFired(f, t0, 2000);
		std::printf("  fired after %.1f ms\n", ms);
		Check(ms >= 19 && ms < 200, "a deadline fires after re-init");
	}

	detail::DestroyForTesting();
	std::printf(g_fail ? "timer_wake_test: %d FAILED\n" : "timer_wake_test: all passed\n", g_fail);
	return g_fail ? 1 : 0;
}
