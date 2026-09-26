// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#define NOMINMAX
#include <vector>
#include <atomic>
#include <iostream>

#include <limits>
#include <cstdint>
#include "Epochs.h"
#include "TaskAllocator.h"
#include "Thread.h"
#include "Fiber.h"

namespace JLib {
	struct LNodeBase; 

	struct LMarkableReference {
		LNodeBase* val_;
		bool marked_;

		LMarkableReference(LNodeBase* val = nullptr, bool mark = false)
			: val_(val), marked_(mark) {}
	};

	struct LMarkablePointer {
		std::atomic<uintptr_t> ref_{ 0 };

		static uintptr_t pack(LNodeBase* ptr, bool mark) {
			return reinterpret_cast<uintptr_t>(ptr) | (mark ? 1ULL : 0ULL);
		}

		static LNodeBase* unpackPtr(uintptr_t val) {
			return reinterpret_cast<LNodeBase*>(val & ~1ULL);
		}

		static bool unpackMark(uintptr_t val) {
			return (val & 1ULL) != 0;
		}

		LMarkablePointer(LNodeBase* val = nullptr, bool mark = false) {
			ref_.store(pack(val, mark), std::memory_order_release);
		}
		bool getMark() const {
			return (ref_.load(std::memory_order_acquire) & 1ULL) != 0;
		}
		void set(LNodeBase* val, bool mark) {
			
			ref_.store(pack(val, mark), std::memory_order_release);
		}
		
		bool attemptMark(LNodeBase* expectedPtr, bool newMark) {
			uintptr_t curr = ref_.load(std::memory_order_acquire);
			while (true) {
				LNodeBase* ptr = reinterpret_cast<LNodeBase*>(curr & ~1ULL);
				bool mark = (curr & 1ULL) != 0;

				if (ptr != expectedPtr) return false;
				if (mark == newMark) return true;

				uintptr_t desired = reinterpret_cast<uintptr_t>(ptr) | (newMark ? 1ULL : 0ULL);
				if (ref_.compare_exchange_weak(curr, desired, std::memory_order_acq_rel))
					return true;
			}
		}
		
		LNodeBase* get(bool& mark) const {
			uintptr_t val = ref_.load(std::memory_order_acquire);
			mark = unpackMark(val);
			return unpackPtr(val);
		}
		LNodeBase* getReference() const {
			
			uintptr_t val = ref_.load(std::memory_order_acquire);
			
			return reinterpret_cast<LNodeBase*>(val & ~1ULL);
		}
		
		bool compareAndSet(LNodeBase* expectedPtr, LNodeBase* newPtr, bool expectedMark, bool newMark) {
			uintptr_t expected = pack(expectedPtr, expectedMark);
			uintptr_t desired = pack(newPtr, newMark);
			return ref_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel);
		}
	};
	struct LNodeBase {
		LMarkablePointer next;   
		uint64_t key;           
		
		TaskAllocator* owner = nullptr;
	};
	template<typename T>
	struct LNode : LNodeBase {
		T data;  
		LNode(uint64_t k, T d) {  
			key = k;
			data = d;
		}
	};

	template <typename T>
	class LockFreeList {
		struct Window {
			LNodeBase* pred;
			LNodeBase* curr;
			Window(LNodeBase* myPred, LNodeBase* myCurr) {
				pred = myPred, curr = myCurr;
			}
			static Window find(LNodeBase* head, uint64_t key) {
				LNodeBase* pred = nullptr;
				LNodeBase* curr = nullptr;
				LNodeBase* succ = nullptr;
				bool marked = false;
				bool snip = false;
			RETRY:
				while (true) {
					pred = head;
					curr = pred->next.getReference();
					while (true) {
						succ = curr->next.get(marked);
						while (marked) {
							snip = pred->next.compareAndSet(curr, succ, false, false);
							if (!snip) goto RETRY;
							curr = succ;
							succ = curr->next.get(marked);
						}
						if (curr->key >= key)
							return Window(pred, curr);
						pred = curr;
						curr = succ;
					}
				}
			}
		};
		TaskAllocator& allocator;

		static void slabDeleter(void* ptr) {
			auto* node = static_cast<LNode<T>*>(ptr);

			node->data.~T();

			if (node->owner) node->owner->Free(node);
		}
		static void heapDeleter(void* ptr) {
			auto* node = static_cast<LNode<T>*>(ptr);
			
			node->data.~T();
			
			delete node;
		}
		LNodeBase* head;
		LNodeBase* tail;
	public:
		LockFreeList(TaskAllocator& alloc) : allocator(alloc) {
			void* mem = allocator.Alloc();
			void* mem2 = allocator.Alloc();
			
			if (!mem || !mem2) {
				if (mem)  allocator.Free(mem);
				if (mem2) allocator.Free(mem2);
				head = tail = nullptr;
				return;
			}
			head = new (mem) LNode<T>(0, T());
			tail = new (mem2) LNode<T>(UINT64_MAX, T());
			head->owner = &allocator;
			tail->owner = &allocator;
			head->next.set(tail, false);
		}

		bool ok() const { return head != nullptr; }
		~LockFreeList() {
			if (!head) return;   
			
			LNodeBase* curr = head->next.getReference();
			while (curr != tail) {
				LNodeBase* next = curr->next.getReference();
				LNode<T>* typed = static_cast<LNode<T>*>(curr);
				typed->data.~T();
				allocator.Free(curr);
				curr = next;
			}
			allocator.Free(head);
			allocator.Free(tail);
		}
		
		bool push(T item) {
			if (!head) return false;   
			EpochGuard guard;
			void* mem = allocator.Alloc();
			if (!mem) return false;
			LNode<T>* node = new (mem) LNode<T>(0, item);
			node->owner = &allocator;
			while (true) {
				LNodeBase* first = head->next.getReference();
				node->next.set(first, false);
				if (head->next.compareAndSet(first, node, false, false)) return true;
			}
		}

		bool add(uint64_t key, T item) {
			if (!head) return false;   
			EpochGuard guard;   
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);

				if (curr->key == key) {
					return false;
				}
				void* mem = allocator.Alloc();
				
				if (!mem) return false;
				LNode<T>* node = new (mem) LNode<T>(key, item);
				node->owner = &allocator;   
				node->next.set(curr, false);

				if (pred->next.compareAndSet(curr, node, false, false)) {
					return true;
				}
			}
		}
		bool remove(uint64_t key) {
			if (!head) return false;   
			EpochGuard guard;   
			bool snip = false;
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);
				if (curr->key != key) {
					return false;
				}
				else {
					LNode<T>* succ = static_cast<LNode<T>*>(curr->next.getReference());
					snip = curr->next.attemptMark(succ, true);
					if (!snip)
						continue;
					pred->next.compareAndSet(curr, succ, false, false);
					EpochManager::Instance().RetirePtr(
						curr,
						EpochManager::Instance().CurrentEpoch(),
						&LockFreeList<T>::slabDeleter
					);
					return true;
				}
			}
		}
		template <typename F>
		void for_each(F func) {
			if (!head) return;         

			EpochGuard guard;  
			
			LNodeBase* curr = head->next.getReference();

			while (curr != tail) {
				
				bool marked = curr->next.getMark();
				LNodeBase* succ = curr->next.getReference();

				if (!marked) {
					
					LNode<T>* typedNode = static_cast<LNode<T>*>(curr);
					func(typedNode->data);
				}
				curr = succ;
			}
		}
		bool contains(uint64_t key) {
			if (!head) return false;   
			EpochGuard guard;  
			LNodeBase* curr = head;

			while (curr != nullptr) {
				LNodeBase* succ = curr->next.getReference();
				bool marked = curr->next.getMark();

				if (curr->key >= key) {
					return (curr->key == key && !marked);
				}

				curr = succ;
			}
			return false;
		}
		T* get(uint64_t key) {
			EpochGuard guard;  
			bool marked = false;
			LNodeBase* curr = head;

			while (curr->key < key) {
				curr = curr->next.get(marked);
			}

			if (curr->key == key && !marked) {
				LNode<T>* typedNode = static_cast<LNode<T>*>(curr);
				return &typedNode->data;  
			}

			return nullptr;  
		}
	};

};
