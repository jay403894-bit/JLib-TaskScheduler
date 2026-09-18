// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include "Task.h"
#include "TaskScheduler.h"
#include "TaskNode.h"
#include "Epochs.h"
#include "TaskAllocator.h"
#include "Thread.h"   

static constexpr uint8_t NONE = 255;

namespace JLib {

    class TaskDAG {
    public:
        TaskDAG(TaskScheduler& sched) : scheduler(sched) {};
        
        ~TaskDAG();
        TaskNode* CreateNode(Task* t, uint8_t priority = NONE, uint8_t cpu_id = NONE);
        
        TaskNode* CreateMainNode(Task* t, uint8_t priority = NONE);
        
        TaskNode* CreateGate(TaskNode::LogicType type);

        TaskNode* CreateExternalNode();
        void SignalExternal(TaskNode* node);
        // Legal after Submit. A node is retired when it finishes or is cancelled, so a caller that
        // may use a node pointer after that point must hold an EpochGuard opened before it could finish.
        void AddDependency(TaskNode* dependent, TaskNode* dependency);

         static void OnTaskFinishedWrapper(void* data) {
             auto* node = static_cast<TaskNode*>(data);
             TaskDAG* owner = node->owner;
             node->origFn(node->origData);   
             
             owner->OnTaskFinished(node, TaskNode::Outcome::Completed);
         }

         static void OnTaskDiscarded(Task* t) {
             if (!t || t->fn != &OnTaskFinishedWrapper || !t->data) return;
             auto* node = static_cast<TaskNode*>(t->data);
             node->owner->OnTaskFinished(node, TaskNode::Outcome::Cancelled);
         }
        
        bool HasCycle();

        bool Submit();

        void OnTaskFinished(TaskNode* node, TaskNode::Outcome outcome = TaskNode::Outcome::Completed);

        void Cancel();
        bool Cancelled() const { return cancelled.load(std::memory_order_acquire); }

        CancelToken Token() const { return scope.Token(); }

    private:
        TaskScheduler& scheduler;
        
        std::atomic<bool> cancelled{ false };
        
        CancelScope scope;
        
        void DisposeUnexecutedTask(TaskNode* node);
        
        std::vector<TaskNode*> nodes;

        std::atomic<bool> submitted_{ false };
        void RejectIfSubmitted(const char* what) const;

        // Bump arena for edges. Each block is one TaskAllocator slot; edges are handed out by
        // index and never freed individually -- the whole chain is retired in ~TaskDAG.
        static constexpr size_t kEdgesPerBlock =
            (TaskAllocator::SLOT - sizeof(void*) - sizeof(std::atomic<size_t>)) / sizeof(DagEdge);

        struct EdgeBlock {
            EdgeBlock*          prev = nullptr;
            std::atomic<size_t> used{ 0 };
            DagEdge             edges[kEdgesPerBlock];
        };
        static_assert(sizeof(EdgeBlock) <= TaskAllocator::SLOT, "EdgeBlock must fit one slot");

        std::atomic<EdgeBlock*> currentBlock{ nullptr };

        DagEdge* AllocEdge();

    public:
        
        template <typename F>
        void ForEachDependent(TaskNode* node, F fn) {
            EpochGuard guard;
            DagEdge* head = node->firstEdge.load(std::memory_order_acquire);
            for (DagEdge* e = EdgePtr(head); e; e = e->next) fn(e->dep);
        }

        template <typename F>
        void WalkAndSeal(TaskNode* node, bool cancelled, F fn) {
            EpochGuard guard;
            DagEdge* head = node->firstEdge.load(std::memory_order_acquire);
            DagEdge* done = nullptr;                      
            for (;;) {
                for (DagEdge* e = EdgePtr(head); e != done; e = e->next) fn(e->dep);
                done = EdgePtr(head);
                if (node->firstEdge.compare_exchange_weak(head, EdgeSeal(done, cancelled),
                        std::memory_order_acq_rel, std::memory_order_acquire))
                    return;
                
            }
        }

    private:
        void Fire(TaskNode* node, TaskNode::Outcome outcome = TaskNode::Outcome::Completed);
        static void NodeDeleter(void* p);   
        static void EdgeBlockDeleter(void* p);
    };

    void SignalExternalNode(TaskNode* node);
};
