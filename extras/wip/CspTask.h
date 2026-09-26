// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include <atomic>

namespace JLib {
namespace csp {

struct Task {
	void (*fn)(void*) = nullptr;
	void*  ctx        = nullptr;
	
	std::atomic<Task*> next{ nullptr };

	Task() = default;
	Task(void (*f)(void*), void* c) noexcept : fn(f), ctx(c) {}

	Task(const Task&) = delete;
	Task& operator=(const Task&) = delete;

	inline void Execute() noexcept { fn(ctx); }
};

class Inbox {
public:
	Inbox() noexcept : head_(&stub_), tail_(&stub_) {}
	Inbox(const Inbox&) = delete;
	Inbox& operator=(const Inbox&) = delete;

	void push(Task* t) noexcept { append(t); }

	void push_batch(Task* headBatch, Task* tailBatch) noexcept {
		tailBatch->next.store(nullptr, std::memory_order_relaxed);
		Task* prev = head_.exchange(tailBatch, std::memory_order_acq_rel);
		prev->next.store(headBatch, std::memory_order_release);
	}

	bool pop(Task*& out) noexcept {
		Task* tail = tail_;
		Task* next = tail->next.load(std::memory_order_acquire);

		if (tail == &stub_) {
			if (!next) return false;
			tail_ = next;
			tail  = next;
			next  = next->next.load(std::memory_order_acquire);
		}
		if (next) {
			tail_ = next;
			out   = tail;
			return true;
		}
		if (tail != head_.load(std::memory_order_acquire)) return false;

		append(&stub_);
		next = tail->next.load(std::memory_order_acquire);
		if (next) {
			tail_ = next;
			out   = tail;
			return true;
		}
		return false;
	}

	bool empty() const noexcept {
		Task* tail = tail_;
		return (tail == &stub_) && (tail->next.load(std::memory_order_acquire) == nullptr);
	}

private:
	void append(Task* n) noexcept {
		n->next.store(nullptr, std::memory_order_relaxed);
		Task* prev = head_.exchange(n, std::memory_order_acq_rel);
		prev->next.store(n, std::memory_order_release);
	}

	std::atomic<Task*> head_;
	Task*              tail_;
	
	Task               stub_;
};

}   
}   
