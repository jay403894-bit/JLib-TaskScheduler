#pragma once
#include <atomic>
#include <cstddef>
#include "platform.h"   

namespace T_Threads {

    struct MPSCNode {
        std::atomic<MPSCNode*> next{ nullptr };
    };

    template <typename T>
    struct MPSCTraits {
        static MPSCNode* Hook(T* p) { return &p->mpscHook; }
        static T* Owner(MPSCNode* n) {
            return reinterpret_cast<T*>(reinterpret_cast<char*>(n) - offsetof(T, mpscHook));
        }
    };

    template<typename T, typename Traits = MPSCTraits<T>>
    class MPSCQueue {
        
        alignas(JLib::platform::kCacheLine) std::atomic<MPSCNode*> head_;
        alignas(JLib::platform::kCacheLine) MPSCNode* tail_;
        MPSCNode stub_;

        void append(MPSCNode* n) {
            n->next.store(nullptr, std::memory_order_relaxed);
            MPSCNode* prev = head_.exchange(n, std::memory_order_acq_rel);
            prev->next.store(n, std::memory_order_release);
        }

    public:
        
        MPSCQueue() : head_(&stub_), tail_(&stub_) {}
        MPSCQueue(const MPSCQueue&) = delete;
        MPSCQueue& operator=(const MPSCQueue&) = delete;

        void push(T* node) { append(Traits::Hook(node)); }

        bool pop(T*& out) {
            MPSCNode* tail = tail_;
            MPSCNode* next = tail->next.load(std::memory_order_acquire);

            if (tail == &stub_) {
                if (!next) return false;
                tail_ = next;
                tail = next;
                next = next->next.load(std::memory_order_acquire);
            }

            if (next) {
                tail_ = next;
                out = Traits::Owner(tail);
                return true;
            }

            if (tail != head_.load(std::memory_order_acquire)) return false;

            append(&stub_);
            next = tail->next.load(std::memory_order_acquire);
            if (next) {
                tail_ = next;
                out = Traits::Owner(tail);
                return true;
            }
            return false;
        }

        bool empty() const {
            return tail_ == &stub_ && tail_->next.load(std::memory_order_acquire) == nullptr;
        }
    };
}
