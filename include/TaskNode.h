// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Task.h"
#include <stdexcept>
#include <string>

namespace JLib {
    class TaskDAG; 

    struct TaskNode;

    struct DagEdge {
        TaskNode* dep  = nullptr;
        DagEdge*  next = nullptr;
    };

    static constexpr uintptr_t kEdgeSealed    = 1;
    static constexpr uintptr_t kEdgeCancelled = 2;
    static constexpr uintptr_t kEdgeTagMask   = 3;
    static_assert(alignof(DagEdge) > kEdgeTagMask,
        "TaskNode::firstEdge packs two bits into the low bits of a DagEdge*; DagEdge's alignment "
        "must leave them free.");

    inline DagEdge* EdgePtr(DagEdge* t) {
        return reinterpret_cast<DagEdge*>(reinterpret_cast<uintptr_t>(t) & ~kEdgeTagMask);
    }
    inline bool EdgeSealed(DagEdge* t) {
        return (reinterpret_cast<uintptr_t>(t) & kEdgeSealed) != 0;
    }
    inline bool EdgeCancelled(DagEdge* t) {
        return (reinterpret_cast<uintptr_t>(t) & kEdgeCancelled) != 0;
    }
    inline DagEdge* EdgeSeal(DagEdge* p, bool cancelled) {
        return reinterpret_cast<DagEdge*>(reinterpret_cast<uintptr_t>(p) | kEdgeSealed
                                          | (cancelled ? kEdgeCancelled : 0));
    }

    struct TaskNode {
        Task* task;                           

        TaskDAG* owner = nullptr;
        Task::Func origFn = nullptr;
        void* origData = nullptr;

        enum class Outcome : uint8_t { Completed, Cancelled };

        enum LogicType : uint8_t { AND, OR };
        LogicType gateType   : 1;

        uint8_t   isGate     : 1;

        uint8_t   isExternal : 1;

        enum : int { EXT_ARMED = 1, EXT_SIGNALLED = 2 };
        std::atomic<int> extBits{ 0 };

        std::atomic<DagEdge*> firstEdge{ nullptr };

        uint32_t buildIndex = 0;

        std::atomic<int> dependencies_left;
        std::atomic<bool> submitted{ false };
#if defined(JLIBSCHED_DAG_TERMINAL_TRACE)
        
        std::atomic<int> terminalCount{ 0 };
#endif
        
        static constexpr uint8_t kNoAffinity = 0xFF;
        uint8_t cpuID    : 8;
        uint8_t priority : 8;
        uint8_t isLocal  : 1;
        uint8_t isFork   : 1;
        
        uint8_t isMain   : 1;

        explicit TaskNode(Task* t)
            : task(t)
            , gateType(AND), isGate(0), isExternal(0)
            , dependencies_left(0)
            , cpuID(kNoAffinity), priority(0), isLocal(1), isFork(0), isMain(0) {}

    };

    static_assert(sizeof(TaskNode) <= 64,
        "TaskNode must fit the 64-byte slab class -- see the packing note above");
}
