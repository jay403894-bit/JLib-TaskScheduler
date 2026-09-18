// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../../include/Topology.h"
#include <vector>
#include <cstddef>

namespace JLib { namespace topology {

static bool GetGroupMasksForRelation(LOGICAL_PROCESSOR_RELATIONSHIP relation,
                                     std::vector<CpuMask>& outMasks) {
    DWORD len = 0;
    GetLogicalProcessorInformationEx(relation, nullptr, &len);
    if (len == 0) return false;

    std::vector<std::byte> buffer(len);
    auto* base = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buffer.data());
    if (!GetLogicalProcessorInformationEx(relation, base, &len)) return false;

    DWORD offset = 0;
    while (offset < len) {
        auto* info = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buffer.data() + offset);
        if (info->Relationship == relation) {
            
            auto widen = [&](const GROUP_AFFINITY& ga) {
                CpuMask m;
                for (unsigned bit = 0; bit < 64; ++bit)
                    if ((uint64_t)ga.Mask & (uint64_t(1) << bit))
                        m.Set(CpuMask::Make(ga.Group, bit));
                if (m.Any()) outMasks.push_back(m);
            };

            if (relation == RelationCache) {
                
                widen(info->Cache.GroupMask);
            } else {
                const PROCESSOR_RELATIONSHIP& proc = info->Processor;
                for (WORD g = 0; g < proc.GroupCount; ++g) widen(proc.GroupMask[g]);
            }
        }
        offset += info->Size;
    }
    return true;
}

void Query(Info& out) {
    out.haveCores = GetGroupMasksForRelation(RelationProcessorCore, out.coreMasks);
    out.haveCache = GetGroupMasksForRelation(RelationCache,         out.cacheMasks);

    out.groupCount   = (unsigned)GetActiveProcessorGroupCount();
    out.logicalCount = (unsigned)GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);

    DWORD len = 0;
    GetLogicalProcessorInformationEx(RelationProcessorCore, nullptr, &len);
    if (len == 0) return;

    std::vector<std::byte> buf(len);
    auto* base = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buf.data());
    if (!GetLogicalProcessorInformationEx(RelationProcessorCore, base, &len)) return;

    DWORD off = 0;
    while (off < len) {
        auto* info = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buf.data() + off);
        if (info->Relationship == RelationProcessorCore) {
            const PROCESSOR_RELATIONSHIP& proc = info->Processor;
            const int eff = (int)proc.EfficiencyClass;
            if (eff > out.maxClass) out.maxClass = eff;
            for (WORD g = 0; g < proc.GroupCount; ++g) {
                const GROUP_AFFINITY& ga = proc.GroupMask[g];
                for (unsigned bit = 0; bit < 64; ++bit)
                    if ((uint64_t)ga.Mask & (uint64_t(1) << bit))
                        out.efficiencyClass[CpuMask::Make(ga.Group, bit)] = eff;
            }
        }
        off += info->Size;
    }
}

}} 
