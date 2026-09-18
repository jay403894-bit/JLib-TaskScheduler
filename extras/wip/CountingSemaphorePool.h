// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include "CspTask.h"    
#include "CspDeque.h"   
#include "SeatSemaphore.h"

#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

namespace JLib {

class CountingSemaphorePool {
public:
	
	CountingSemaphorePool();
	~CountingSemaphorePool();
	CountingSemaphorePool(const CountingSemaphorePool&) = delete;
	CountingSemaphorePool& operator=(const CountingSemaphorePool&) = delete;

	void Start(size_t workers = 0,
	           double permitFrac = 0.375,
	           std::chrono::nanoseconds stagger = std::chrono::microseconds(125));
	void Stop();

	void Push(csp::Task* task);

	void PushWide(csp::Task** tasks, size_t count);

	void ParallelFor(int begin, int end, int grain,
	                 void (*body)(void* ctx, int lo, int hi), void* ctx);

	size_t WorkerCount() const noexcept { return workers_.size(); }
	SeatSemaphore& Wait() noexcept { return wait_; }

	unsigned long long Executed()  const noexcept { return executed_.load(std::memory_order_relaxed); }
	unsigned long long Steals()    const noexcept { return steals_.load(std::memory_order_relaxed); }
	unsigned long long StealMiss() const noexcept { return stealMiss_.load(std::memory_order_relaxed); }
	unsigned long long Parks()     const noexcept { return parks_.load(std::memory_order_relaxed); }

private:
	struct Worker;
	void WorkerLoop(size_t index);
	void GrowWide(int extra);
	void HelpUntil(std::atomic<int>& live);

	SeatSemaphore                          wait_;
	std::vector<std::unique_ptr<Worker>> workers_;
	std::vector<std::thread>             threads_;
	std::atomic<bool>                    running_{ false };
	std::atomic<unsigned long long>      nextTarget_{ 0 };

	std::atomic<unsigned long long> executed_{ 0 };
	std::atomic<unsigned long long> steals_{ 0 };
	std::atomic<unsigned long long> stealMiss_{ 0 };
	std::atomic<unsigned long long> parks_{ 0 };
};

}   
