// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../../include/Topology.h"
#include <sys/sysctl.h>
#include <cstdint>

namespace {

long long SysctlInt(const char* name)
{
    std::int64_t value = 0;
    size_t size = sizeof(value);
    if (::sysctlbyname(name, &value, &size, nullptr, 0) != 0) return -1;
    if (size == sizeof(std::int32_t)) return (long long)*(std::int32_t*)&value;
    return (long long)value;
}

} 

void JLib::topology::Query(Info& out)
{
    const long long logical  = SysctlInt("hw.logicalcpu");
    const long long physical = SysctlInt("hw.physicalcpu");
    if (logical <= 0) return;                 

    const int n = (int)(logical > (long long)CpuMask::kMaxCpus ? (long long)CpuMask::kMaxCpus : logical);
    out.logicalCount = (unsigned)n;
    out.groupCount   = 1;

    if (physical > 0 && physical == logical) {
        out.coreMasks.reserve((size_t)n);
        for (int c = 0; c < n; ++c) {
            CpuMask m;
            m.Set((CpuId)c);
            out.coreMasks.push_back(m);
        }
        out.haveCores = true;
    }
    
}
