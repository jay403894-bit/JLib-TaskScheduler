// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

#if defined(__linux__)

#include <atomic>
#include <cstdint>
#include <linux/io_uring.h>

namespace JLib { namespace uring {

struct Ring {
    int fd = -1;

    void*    sqRing     = nullptr;
    std::size_t sqRingSz = 0;
    void*    cqRing     = nullptr;      
    std::size_t cqRingSz = 0;
    io_uring_sqe* sqes  = nullptr;
    std::size_t sqesSz  = 0;

    unsigned* sqHead    = nullptr;
    unsigned* sqTail    = nullptr;
    unsigned* sqMask    = nullptr;
    unsigned* sqArray   = nullptr;

    unsigned* cqHead    = nullptr;
    unsigned* cqTail    = nullptr;
    unsigned* cqMask    = nullptr;
    io_uring_cqe* cqes  = nullptr;

    unsigned  entries   = 0;
    unsigned  features  = 0;
};

enum class InitResult : std::uint8_t {
    Ok,
    Unsupported,   
    Denied,        
    Failed,        
};

InitResult Init(Ring& out, unsigned entries) noexcept;
void       Shutdown(Ring& r) noexcept;

InitResult Probe() noexcept;

io_uring_sqe* GetSqe(Ring& r) noexcept;

int Submit(Ring& r, unsigned waitFor) noexcept;

int WaitCq(Ring& r, unsigned minComplete) noexcept;

unsigned Reap(Ring& r, io_uring_cqe* out, unsigned max) noexcept;

bool PostWake(Ring& r, std::uint64_t sentinel) noexcept;

}} 

#endif 
