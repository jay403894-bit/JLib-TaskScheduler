// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../../include/Topology.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>

namespace JLib { namespace topology {

static bool ReadFileText(const char* path, std::string& out) {
    FILE* f = std::fopen(path, "rb");
    if (!f) return false;
    char buf[512];
    size_t n = std::fread(buf, 1, sizeof(buf) - 1, f);
    std::fclose(f);
    buf[n] = '\0';
    out.assign(buf, n);
    return true;
}

namespace detail {
CpuMask ParseCpuList(const std::string& s) {
    CpuMask mask;
    const char* p = s.c_str();
    while (*p) {
        while (*p == ',' || *p == ' ' || *p == '\n') ++p;
        if (!*p) break;
        char* end = nullptr;
        long lo = std::strtol(p, &end, 10);
        if (end == p) break;                       
        p = end;
        long hi = lo;
        if (*p == '-') {
            ++p;
            hi = std::strtol(p, &end, 10);
            if (end == p) break;
            p = end;
        }
        for (long c = lo; c <= hi && c < (long)CpuMask::kMaxCpus; ++c)
            if (c >= 0) mask.Set((CpuId)c);
    }
    return mask;
}
}   
using detail::ParseCpuList;

static int OnlineCpuCount() {
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) n = 1;
    return (int)std::min<long>(n, (long)CpuMask::kMaxCpus);
}

void Query(Info& out) {
    const int nCpu = OnlineCpuCount();
    
    out.logicalCount = (unsigned)nCpu;
    out.groupCount   = 1;
    char path[256];
    std::string text;

    for (int cpu = 0; cpu < nCpu; ++cpu) {
        std::snprintf(path, sizeof(path),
                      "/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list", cpu);
        if (!ReadFileText(path, text)) continue;
        const CpuMask m = ParseCpuList(text);
        if (m.Any() && std::find(out.coreMasks.begin(), out.coreMasks.end(), m) == out.coreMasks.end())
            out.coreMasks.push_back(m);
    }
    out.haveCores = !out.coreMasks.empty();

    for (int cpu = 0; cpu < nCpu; ++cpu) {
        for (int idx = 0; idx < 8; ++idx) {
            std::snprintf(path, sizeof(path),
                          "/sys/devices/system/cpu/cpu%d/cache/index%d/shared_cpu_list", cpu, idx);
            if (!ReadFileText(path, text)) continue;
            const CpuMask m = ParseCpuList(text);
            if (m.Any() && std::find(out.cacheMasks.begin(), out.cacheMasks.end(), m) == out.cacheMasks.end())
                out.cacheMasks.push_back(m);
        }
    }
    out.haveCache = !out.cacheMasks.empty();

    // Core classes, same convention as Windows' EfficiencyClass: higher is faster, P = maxClass.
    // Intel hybrid: the cpu_core (P) and cpu_atom (E) PMU devices list their CPUs.
    // ARM big.LITTLE: cpu_capacity per CPU; each distinct capacity is a class, ranked ascending.
    // Neither present (one core class, or a VM such as WSL): every CPU stays -1, all are P.
    std::string pText, eText;
    if (ReadFileText("/sys/devices/cpu_core/cpus", pText) && ReadFileText("/sys/devices/cpu_atom/cpus", eText)) {
        const CpuMask p = ParseCpuList(pText), e = ParseCpuList(eText);
        if (p.Any() && e.Any()) {
            for (int cpu = 0; cpu < nCpu; ++cpu) {
                if (p.Test((CpuId)cpu))      out.efficiencyClass[cpu] = 1;
                else if (e.Test((CpuId)cpu)) out.efficiencyClass[cpu] = 0;
            }
            out.maxClass = 1;
        }
    } else {
        std::vector<long> cap((size_t)nCpu, -1), distinct;
        for (int cpu = 0; cpu < nCpu; ++cpu) {
            std::snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%d/cpu_capacity", cpu);
            if (!ReadFileText(path, text)) continue;
            cap[(size_t)cpu] = std::strtol(text.c_str(), nullptr, 10);
            if (std::find(distinct.begin(), distinct.end(), cap[(size_t)cpu]) == distinct.end())
                distinct.push_back(cap[(size_t)cpu]);
        }
        if (distinct.size() >= 2) {
            std::sort(distinct.begin(), distinct.end());
            for (int cpu = 0; cpu < nCpu; ++cpu)
                if (cap[(size_t)cpu] >= 0)
                    out.efficiencyClass[cpu] = (int)(std::find(distinct.begin(), distinct.end(), cap[(size_t)cpu]) - distinct.begin());
            out.maxClass = (int)distinct.size() - 1;
        }
    }
}

}} 
