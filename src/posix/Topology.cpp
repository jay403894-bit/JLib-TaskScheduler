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

}

}} 
