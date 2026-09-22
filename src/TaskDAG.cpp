// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/TaskDAG.h"

#include <cstdio>    
#include <cstdlib>
#include <new>
#include "../include/Reclaimer.h"
#include "../include/Memory.h"   
using namespace JLib;

void TaskDAG::RejectIfSubmitted(const char* what) const {
    if (!submitted_.load(std::memory_order_acquire)) return;
    std::fprintf(stderr,
        "[JLib::Scheduler] FATAL: %s called on a TaskDAG that has already been submitted.\n"
        "  Submit collects the roots and clears the node list, so a node created afterwards is\n"
        "  never dispatched and never freed -- it waits forever and takes every dependent with it.\n"
        "  The node list is also unsynchronised, so this would race Submit's own reader.\n"
        "  ADDING AN EDGE IS FINE: AddDependency works on a running graph. CREATING A NODE IS NOT.\n"
        "  Build every node before Submit, or use a second graph.\n", what);
    std::fflush(stderr);
    std::abort();
}

TaskNode* TaskDAG::CreateNode(Task* t, uint8_t priority, uint8_t cpu_id) {
    RejectIfSubmitted("TaskDAG::CreateNode");
    
    if (t && t->cancelToken == CancelToken::kNone) t->cancelToken = scope.Token().Raw();

    void* mem = scheduler.GetAllocator()->AllocSized(sizeof(TaskNode));
    if (!mem) return nullptr;

    TaskNode* node = new (mem) TaskNode(t);

    node->isLocal = (priority == NONE);
    node->priority = (priority == NONE) ? 0 : priority;
    node->cpuID = cpu_id;

    nodes.push_back(node);   
    return node;
}

TaskNode* TaskDAG::CreateMainNode(Task* t, uint8_t priority) {
    // Fire() hands main nodes to PushMain, which starts them on main and keeps their type: a Native node
    // runs straight on main's stack, a Fiber node gets a fiber. A Fiber node that suspends resumes where
    // its wait's pin says (Pin::Main to finish on main). With main out of the pool it has no fibers, so a
    // main node must not suspend there (the suspend points fail loudly if it does).
    TaskNode* node = CreateNode(t, priority, NONE);
    if (node) node->isMain = true;
    return node;
}

TaskNode* TaskDAG::CreateGate(TaskNode::LogicType type) {
    RejectIfSubmitted("TaskDAG::CreateGate");

    void* mem = scheduler.GetAllocator()->AllocSized(sizeof(TaskNode));
    if (!mem) return nullptr;
    
    TaskNode* node = new (mem) TaskNode(nullptr);
    node->isGate = true;
    node->gateType = type;
    nodes.push_back(node);
    return node;
}

TaskNode* TaskDAG::CreateExternalNode() {
    RejectIfSubmitted("TaskDAG::CreateExternalNode");

    void* mem = scheduler.GetAllocator()->AllocSized(sizeof(TaskNode));
    if (!mem) return nullptr;
    
    TaskNode* node = new (mem) TaskNode(nullptr);
    node->isExternal = true;
    
    node->owner = this;
    nodes.push_back(node);
    return node;
}

void TaskDAG::SignalExternal(TaskNode* node) {
    if (!node) return;

    EpochGuard guard;
    const int prev = node->extBits.fetch_or(TaskNode::EXT_SIGNALLED, std::memory_order_acq_rel);

    if (prev & TaskNode::EXT_SIGNALLED) return;

    if (prev & TaskNode::EXT_ARMED) {
        OnTaskFinished(node);
    }
}

void JLib::SignalExternalNode(TaskNode* node) {
    
    if (node && node->owner) node->owner->SignalExternal(node);
}

bool TaskDAG::HasCycle() {
    
    const size_t n = nodes.size();
    for (size_t i = 0; i < n; ++i) nodes[i]->buildIndex = static_cast<uint32_t>(i);

    std::vector<int> indeg(n);
    for (size_t i = 0; i < n; ++i)
        indeg[i] = nodes[i]->dependencies_left.load(std::memory_order_relaxed);

    std::vector<TaskNode*> ready;
    ready.reserve(n);
    for (size_t i = 0; i < n; ++i)
        if (indeg[i] == 0) ready.push_back(nodes[i]);

    size_t processed = 0;
    while (!ready.empty()) {
        TaskNode* nd = ready.back(); ready.pop_back();
        ++processed;
        
    for (DagEdge* e = EdgePtr(nd->firstEdge.load(std::memory_order_relaxed)); e; e = e->next) {
            TaskNode* dep = e->dep;
            
            assert(dep->buildIndex < n && "AddDependency crossed DAGs: dependent is not in this graph");
            if (--indeg[dep->buildIndex] == 0) ready.push_back(dep);
        }
    }
    return processed != n;
}

bool TaskDAG::Submit() {
    const bool cyclic = HasCycle();

    submitted_.store(true, std::memory_order_release);

    if (cyclic) {
        // The tasks never ran, so they can go now. The nodes may still be named by a caller
        // (AddDependency is legal after Submit), so they go through the epoch like any other.
        EpochManager& em = EpochManager::Instance();
        for (auto* n : nodes) {
            DisposeUnexecutedTask(n);
            em.RetirePtr(n, em.CurrentEpoch(), &TaskDAG::NodeDeleter);
        }
        nodes.clear();
        return false;
    }

    std::vector<TaskNode*> roots;
    for (auto* n : nodes)
        if (n->dependencies_left.load(std::memory_order_acquire) == 0)
            roots.push_back(n);

    nodes.clear();
    for (auto* r : roots)
        Fire(r);
    return true;
}

void TaskDAG::AddDependency(TaskNode* dependent, TaskNode* dependency) {
    if (!dependent || !dependency) return;

    // After Submit either node may finish and be retired while we work on it.
    EpochGuard guard;

    DagEdge* e = AllocEdge();
    if (!e) {
        // Dropping the edge would let `dependent` run before `dependency`.
        std::fprintf(stderr, "[JLib::Scheduler] FATAL: TaskDAG::AddDependency is out of memory.\n");
        std::fflush(stderr);
        std::abort();
    }
    e->dep = dependent;

    dependent->dependencies_left.fetch_add(1, std::memory_order_relaxed);

    DagEdge* head = dependency->firstEdge.load(std::memory_order_acquire);
    for (;;) {
        if (EdgeSealed(head)) {
            
            const bool cancelled = EdgeCancelled(head);
            if (cancelled) { Fire(dependent, TaskNode::Outcome::Cancelled); return; }
            const bool ready = (dependent->gateType == TaskNode::OR)
                ? true
                : (dependent->dependencies_left.fetch_sub(1, std::memory_order_acq_rel) - 1 == 0);
            if (ready) Fire(dependent);
            return;
        }
        e->next = EdgePtr(head);
        if (dependency->firstEdge.compare_exchange_weak(head, e,
                std::memory_order_release, std::memory_order_acquire))
            return;   
    }
}

JLib::TaskDAG::BlockSlot& JLib::TaskDAG::EdgeSlotForThisThread() {
    Thread* self = Thread::GetCurrent();
    const size_t outside = edgeSlots.size() - 1;
    if (!self || !self->IsPoolWorker() || self->qIndex < 0) return edgeSlots[outside];
    const size_t q = (size_t)self->qIndex;
    return edgeSlots[q < outside ? q : outside];
}

JLib::DagEdge* JLib::TaskDAG::AllocEdge() {
    BlockSlot& slot = EdgeSlotForThisThread();
    for (;;) {
        // This thread's own block: the bump is on a line nobody else is writing (except among
        // threads sharing the outside slot).
        EdgeBlock* block = slot.cur.load(std::memory_order_acquire);
        if (block) {
            const size_t i = block->used.fetch_add(1, std::memory_order_acq_rel);
            if (i < kEdgesPerBlock) {
                DagEdge* e = &block->edges[i];
                e->dep = nullptr;
                e->next = nullptr;
                return e;
            }
        }

        // Full (or none yet): take a block and chain it for retirement before anyone can use it,
        // so ~TaskDAG frees it even if this thread never installs it below.
        // From the calling worker's own heap (Memory.h); any thread may free it later.
        // JLIB_DAG_CTL_EDGE_NEW: the previous general-heap new/delete, for A/B only.
#if defined(JLIB_DAG_CTL_EDGE_NEW)
        EdgeBlock* fresh = new (std::nothrow) EdgeBlock();
        if (!fresh) return nullptr;
#else
        void* mem = JLib::Alloc(sizeof(EdgeBlock));
        if (!mem) return nullptr;
        EdgeBlock* fresh = ::new (mem) EdgeBlock();
#endif
        fresh->used.store(1, std::memory_order_relaxed);   // edges[0] is ours

        EdgeBlock* head = retireHead.load(std::memory_order_relaxed);
        do {
            fresh->prev = head;
        } while (!retireHead.compare_exchange_weak(head, fresh,
                     std::memory_order_release, std::memory_order_relaxed));

        // Publish it as this slot's current block. A racing thread on the shared outside slot may
        // install first; ours is already on the retire chain, so retry in theirs and let it go.
        if (slot.cur.compare_exchange_strong(block, fresh,
                std::memory_order_acq_rel, std::memory_order_acquire))
            return &fresh->edges[0];
    }
}

JLib::TaskDAG::~TaskDAG() {
    EpochManager& em = EpochManager::Instance();
    const auto epoch = em.CurrentEpoch();

    // A graph destroyed without Submit still owns every node it built, and each node its task.
    // Submit empties `nodes` on both of its paths, so anything left here never ran: dispose it the
    // way the cyclic path does.
#if !defined(JLIB_DAG_CTL_KEEP_UNSUBMITTED)   // negative control: dag_unsubmitted_test must fail
    for (auto* n : nodes) {
        DisposeUnexecutedTask(n);
        em.RetirePtr(n, epoch, &TaskDAG::NodeDeleter);
    }
#endif
    nodes.clear();

    // Every block ever taken is on this chain, whichever thread's slot it ended up in.
    EdgeBlock* block = retireHead.load(std::memory_order_acquire);
    while (block) {
        EdgeBlock* prev = block->prev;
        em.RetirePtr(block, epoch, &TaskDAG::EdgeBlockDeleter);
        block = prev;
    }

    Reclaimer::Flush();
}

void TaskDAG::EdgeBlockDeleter(void* p) {
    // Heap, not the slab: safe after the pool is gone (Join moves live blocks to mimalloc's main heap).
#if defined(JLIB_DAG_CTL_EDGE_NEW)
    delete static_cast<EdgeBlock*>(p);
#else
    EdgeBlock* b = static_cast<EdgeBlock*>(p);
    b->~EdgeBlock();
    JLib::Free(b);
#endif
}

void TaskDAG::OnTaskFinished(TaskNode* node, TaskNode::Outcome outcome) {
#if defined(JLIBSCHED_DAG_TERMINAL_TRACE)
    
    if (node && node->terminalCount.fetch_add(1, std::memory_order_acq_rel) != 0) {
        static std::atomic<bool> told{ false };
        if (!told.exchange(true, std::memory_order_relaxed)) {
            std::fprintf(stderr,
                "[JLib::Scheduler] DAG DOUBLE TERMINAL: node %p reached OnTaskFinished more than "
                "once (outcome=%d). Dependents fire twice and the node is retired twice.\n",
                (void*)node, (int)outcome);
            std::fflush(stderr);
        }
    }
#endif
    
    WalkAndSeal(node, outcome == TaskNode::Outcome::Cancelled, [this, outcome](TaskNode* dep) {
        if (outcome == TaskNode::Outcome::Cancelled) {
            Fire(dep, TaskNode::Outcome::Cancelled);
            return;
        }
        bool ready = (dep->gateType == TaskNode::OR)
            ? true
            : (dep->dependencies_left.fetch_sub(1, std::memory_order_acq_rel) - 1 == 0);
        if (ready) Fire(dep);
        });
    
    EpochManager::Instance().RetirePtr(node, EpochManager::Instance().CurrentEpoch(), &TaskDAG::NodeDeleter);
}

void TaskDAG::NodeDeleter(void* p) {
    auto* n = static_cast<TaskNode*>(p);
    
    TaskAllocator* a = TaskScheduler::Instance().GetAllocator();
    n->~TaskNode();
    a->Free(n);   // Free, not FreeSized: FreeSized returns false for a heap fallback and leaks it
}

void TaskDAG::Cancel() {
    
    cancelled.store(true, std::memory_order_release);
    
    scope.Cancel();
}

void TaskDAG::DisposeUnexecutedTask(TaskNode* node) {
    Task* t = node->task;
    if (!t) return;                 
    node->task = nullptr;           

    if (t->waitGroup) {
        t->waitGroup->Done();
    }
    scheduler.FreeTask(t);
}

void TaskDAG::Fire(TaskNode* node, TaskNode::Outcome outcome) {

    if (node->submitted.exchange(true, std::memory_order_acq_rel)) {
        return; 
    }

    if (cancelled.load(std::memory_order_acquire)) {
        outcome = TaskNode::Outcome::Cancelled;
    }

    if (outcome == TaskNode::Outcome::Cancelled) {
        
        DisposeUnexecutedTask(node);
        OnTaskFinished(node, TaskNode::Outcome::Cancelled);
        return;
    }

    if (node->isGate) {
        
        OnTaskFinished(node);
        return;
    }

    if (node->isExternal) {
        
        const int prev = node->extBits.fetch_or(TaskNode::EXT_ARMED, std::memory_order_acq_rel);
        if (prev & TaskNode::EXT_SIGNALLED) {
            OnTaskFinished(node);   
        }
        return;
    }

    // A node with no task has no work: it completes like a gate.
    if (!node->task) {
        OnTaskFinished(node);
        return;
    }

    node->owner = this;
    node->origFn = node->task->fn;
    node->origData = node->task->data;
    node->task->fn = &OnTaskFinishedWrapper;
    node->task->data = node;

    bool queued;
    if (node->isMain) {
        queued = scheduler.PushMain(node->task);
    }
    else if ((node->isFork || node->isLocal) && node->cpuID != TaskNode::kNoAffinity) {
        queued = scheduler.PushTo(node->cpuID, node->task);
    }
    else {
        // Round-robin, not Push: a finishing node releases its dependents as a fan-out, and Push
        // on a worker would keep them all on that worker's own deque.
        queued = scheduler.PushTarget(node->task);
    }

    // Nobody will run it, so finish it as cancelled -- otherwise every dependent waits forever.
    if (!queued) {
        node->task->fn = node->origFn;
        node->task->data = node->origData;
        DisposeUnexecutedTask(node);
        OnTaskFinished(node, TaskNode::Outcome::Cancelled);
    }
}
