// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <type_traits>
#include "Task.h"
#include "platform.h"   
#include "TaskAllocator.h"

namespace JLib {
	
    class alignas(platform::kCacheLine)TaskMPSCQueue {
        static_assert(std::is_pointer<Task*>::value, "MPSCQueue<T> expects a pointer type");

        std::atomic<Task*> head_;
        Task*              tail_;
        Task*              stub_;
        
        TaskAllocator*     alloc_;

        void append(Task* n) {
            n->next.store(nullptr, std::memory_order_relaxed);
            Task* prev = head_.exchange(n, std::memory_order_acq_rel);
            prev->next.store(n, std::memory_order_release);
        }

    public:
        
        TaskMPSCQueue() : head_(nullptr), tail_(nullptr), stub_(nullptr), alloc_(nullptr) {}

        ~TaskMPSCQueue() {
            if (!stub_) return;          
            clear();
            stub_->~Task();
            if (alloc_) alloc_->Free(stub_);
            stub_ = nullptr;
        }

        void init(TaskAllocator* allocator) {
            void* mem = allocator->Alloc();
            stub_ = ::new (mem) Task();
            stub_->next.store(nullptr, std::memory_order_relaxed);
            head_.store(stub_, std::memory_order_relaxed);
            tail_ = stub_;
            alloc_ = allocator;
        }
        void push(Task* task) { append(task); }

        void push_batch(Task*head_batch, Task*tail_batch) {
            tail_batch->next.store(nullptr, std::memory_order_relaxed);
            Task* prev = head_.exchange(tail_batch, std::memory_order_acq_rel);
            prev->next.store(head_batch, std::memory_order_release);
        }

        bool pop(Task*& out) {
            Task* tail = tail_;
            Task* next = tail->next.load(std::memory_order_acquire);

            if (tail == stub_) {
                if (!next) return false;            
                tail_ = next;                      
                tail = next;
                next = next->next.load(std::memory_order_acquire);
            }

            if (next) {                              
                tail_ = next;
                out = static_cast<Task*>(tail);
                return true;
            }

            if (tail != head_.load(std::memory_order_acquire)) {
                return false;
            }

            append(stub_);
            next = tail->next.load(std::memory_order_acquire);
            if (next) {
                tail_ = next;
                out = static_cast<Task*>(tail);
                return true;
            }
            return false;
        }

        void clear() {
            Task*tmp;
            while (pop(tmp)) { }
        }

        bool empty() const {
            Task* tail = tail_;
            Task* next = tail->next.load(std::memory_order_acquire);
            return (tail == stub_ && next == nullptr);
        }

        bool quiescent() const {
            Task* tail = tail_;
            if (tail != stub_) return false;                                   
            if (tail->next.load(std::memory_order_acquire)) return false;      
            return head_.load(std::memory_order_acquire) == tail;              
        }
    };
}
