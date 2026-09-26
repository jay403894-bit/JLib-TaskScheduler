// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"
#include <vector>
#include <cstdint>
#if JLIB_PLATFORM_LINUX
#include <string>   
#endif

namespace JLib { namespace topology {

using CpuId = std::uint32_t;

struct CpuMask {
    static constexpr unsigned kWords   = 4;
    static constexpr unsigned kMaxCpus = kWords * 64;   

    std::uint64_t w[kWords] = {};

    static constexpr unsigned GroupOf(CpuId c) { return c >> 6; }
    static constexpr unsigned BitOf(CpuId c)   { return c & 63u; }
    static constexpr CpuId    Make(unsigned group, unsigned bit) { return CpuId(group) * 64u + bit; }

    void Set(CpuId c) {
        if (c < kMaxCpus) w[c >> 6] |= (std::uint64_t(1) << (c & 63u));
    }
    bool Test(CpuId c) const {
        return c < kMaxCpus && ((w[c >> 6] >> (c & 63u)) & 1u) != 0;
    }
    bool Any() const {
        for (unsigned i = 0; i < kWords; ++i) if (w[i]) return true;
        return false;
    }
    
    unsigned Count() const {
        unsigned n = 0;
        for (unsigned i = 0; i < kWords; ++i)
            for (std::uint64_t x = w[i]; x; x &= (x - 1)) ++n;
        return n;
    }
    bool operator==(const CpuMask& o) const {
        for (unsigned i = 0; i < kWords; ++i) if (w[i] != o.w[i]) return false;
        return true;
    }
    bool operator!=(const CpuMask& o) const { return !(*this == o); }
};

struct Info {
    
    std::vector<CpuMask> coreMasks;
    
    std::vector<CpuMask> cacheMasks;

    bool haveCores = false;   
    bool haveCache = false;   

    unsigned logicalCount = 0;
    unsigned groupCount   = 0;

    int  efficiencyClass[CpuMask::kMaxCpus];
    int  maxClass = -1;

    Info() { for (unsigned i = 0; i < CpuMask::kMaxCpus; ++i) efficiencyClass[i] = -1; }
};

void Query(Info& out);

#if JLIB_PLATFORM_LINUX
namespace detail {

CpuMask ParseCpuList(const std::string& s);
}
#endif

}} 
