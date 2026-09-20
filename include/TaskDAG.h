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
        TaskDAG(TaskScheduler& sched)
            : scheduler(sched), edgeSlots(sched.GetWorkerCount() + 1) {};
        
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

        // Bump arena for edges: edges are handed out by index and never freed individually -- the
        // whole chain is retired in ~TaskDAG. Blocks come from the heap, not the task slab: a slab
        // slot is 256 B, which is FIFTEEN edges, so a graph paid an allocator call and a chain CAS
        // every fifteen. 4 KiB holds ~255 and keeps DAG building off the task classes entirely.
        // A block is only taken when a thread actually adds an edge, so a thread that builds
        // nothing costs nothing.
        //
        // ONE CURRENT BLOCK PER THREAD. A single shared block put every concurrent AddDependency on
        // one cache line: with each thread building its own node pair (so no other contention),
        // aggregate throughput FELL from 18.9M edges/s on one thread to 9.7M on two and stayed
        // ~10M however many helped -- parallel graph build was slower than serial. A thread bumps
        // its own block; blocks are chained into `retireHead` only when installed, which is once
        // per kEdgesPerBlock edges, and the chain is what ~TaskDAG retires.
        // Block size is a guess pending a histogram of real graphs: big enough that the chain CAS
        // and the allocation amortise, small enough that a thread that adds a few edges does not
        // take a page. Sweepable: -DJLIBSCHED_DAG_EDGE_BLOCK=<bytes>.
#ifndef JLIBSCHED_DAG_EDGE_BLOCK
#define JLIBSCHED_DAG_EDGE_BLOCK 4096
#endif
        static constexpr size_t kEdgeBlockBytes = JLIBSCHED_DAG_EDGE_BLOCK;
        static constexpr size_t kEdgesPerBlock =
            (kEdgeBlockBytes - sizeof(void*) - sizeof(std::atomic<size_t>)) / sizeof(DagEdge);

        struct EdgeBlock {
            EdgeBlock*          prev = nullptr;   // next in the retire chain, not in use order
            std::atomic<size_t> used{ 0 };
            DagEdge             edges[kEdgesPerBlock];
        };
        static_assert(sizeof(EdgeBlock) <= kEdgeBlockBytes, "EdgeBlock must fit its block");

        // A worker's slot is its qIndex; every thread outside the pool shares the last one (they
        // contend with each other only, as they did before).
        struct alignas(platform::kCacheLine) BlockSlot {
            std::atomic<EdgeBlock*> cur{ nullptr };
        };
        std::vector<BlockSlot>  edgeSlots;
        std::atomic<EdgeBlock*> retireHead{ nullptr };

        BlockSlot& EdgeSlotForThisThread();
        DagEdge*   AllocEdge();

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
