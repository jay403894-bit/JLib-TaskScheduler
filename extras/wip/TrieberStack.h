#pragma once
#include <atomic>

template <typename T>
class TreiberStack {
    struct Node {
        T data;
        Node* next;
    };

    std::atomic<Node*> head{ nullptr };

public:
    // Push: Insert at head
    void push(T value) {
        Node* newNode = new Node{ value, nullptr };
        Node* oldHead = head.load(std::memory_order_relaxed);

        do {
            newNode->next = oldHead;
        } while (!head.compare_exchange_weak(
            oldHead, newNode,
            std::memory_order_release,
            std::memory_order_relaxed
        ));
    }

    // Pop: Remove from head
    bool pop(T& value) {
        Node* oldHead = head.load(std::memory_order_relaxed);

        while (oldHead) {
            if (head.compare_exchange_weak(
                oldHead, oldHead->next,
                std::memory_order_acquire,
                std::memory_order_relaxed
            )) {
                value = oldHead->data;
                delete oldHead;
                return true;
            }
        }
        return false;
    }
};

