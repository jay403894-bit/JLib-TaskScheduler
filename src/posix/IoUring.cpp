// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "IoUring.h"

#if defined(__linux__)

#include <cerrno>
#include <csignal>          
#include <cstring>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#ifndef JLIB_SIGSET_BYTES
  #define JLIB_SIGSET_BYTES (sizeof(sigset_t))
#endif

namespace JLib { namespace uring {
namespace {

int sys_io_uring_setup(unsigned entries, io_uring_params* p) noexcept {
    return (int)syscall(__NR_io_uring_setup, entries, p);
}
int sys_io_uring_enter(int fd, unsigned toSubmit, unsigned minComplete,
                       unsigned flags, sigset_t* sig) noexcept {
    return (int)syscall(__NR_io_uring_enter, fd, toSubmit, minComplete, flags, sig,
                        (std::size_t)JLIB_SIGSET_BYTES);
}

inline std::atomic<unsigned>& Atomic(unsigned* p) noexcept {
    return *reinterpret_cast<std::atomic<unsigned>*>(p);
}

InitResult Classify(int err) noexcept {
    switch (err) {
        case ENOSYS: return InitResult::Unsupported;
        case EPERM:
        case EACCES: return InitResult::Denied;
        default:     return InitResult::Failed;
    }
}

} 

InitResult Init(Ring& out, unsigned entries) noexcept {
    Ring r;
    io_uring_params p{};

    const int fd = sys_io_uring_setup(entries, &p);
    if (fd < 0) return Classify(-fd == 0 ? errno : -fd);

    r.fd       = fd;
    r.entries  = p.sq_entries;
    r.features = p.features;

    r.sqRingSz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    r.cqRingSz = p.cq_off.cqes  + p.cq_entries * sizeof(io_uring_cqe);

    const bool single = (p.features & IORING_FEAT_SINGLE_MMAP) != 0;
    if (single) {
        if (r.cqRingSz > r.sqRingSz) r.sqRingSz = r.cqRingSz;
        r.cqRingSz = r.sqRingSz;
    }

    r.sqRing = mmap(nullptr, r.sqRingSz, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);
    if (r.sqRing == MAP_FAILED) { r.sqRing = nullptr; Shutdown(r); return InitResult::Failed; }

    if (single) {
        r.cqRing = r.sqRing;
    } else {
        r.cqRing = mmap(nullptr, r.cqRingSz, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
        if (r.cqRing == MAP_FAILED) { r.cqRing = nullptr; Shutdown(r); return InitResult::Failed; }
    }

    r.sqesSz = p.sq_entries * sizeof(io_uring_sqe);
    r.sqes = (io_uring_sqe*)mmap(nullptr, r.sqesSz, PROT_READ | PROT_WRITE,
                                 MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES);
    if (r.sqes == MAP_FAILED) { r.sqes = nullptr; Shutdown(r); return InitResult::Failed; }

    auto* sq = (unsigned char*)r.sqRing;
    auto* cq = (unsigned char*)r.cqRing;
    r.sqHead  = (unsigned*)(sq + p.sq_off.head);
    r.sqTail  = (unsigned*)(sq + p.sq_off.tail);
    r.sqMask  = (unsigned*)(sq + p.sq_off.ring_mask);
    r.sqArray = (unsigned*)(sq + p.sq_off.array);
    r.cqHead  = (unsigned*)(cq + p.cq_off.head);
    r.cqTail  = (unsigned*)(cq + p.cq_off.tail);
    r.cqMask  = (unsigned*)(cq + p.cq_off.ring_mask);
    r.cqes    = (io_uring_cqe*)(cq + p.cq_off.cqes);

    for (unsigned i = 0; i < p.sq_entries; ++i) r.sqArray[i] = i;

    out = r;
    return InitResult::Ok;
}

void Shutdown(Ring& r) noexcept {
    if (r.sqes)   { munmap(r.sqes, r.sqesSz); r.sqes = nullptr; }
    
    if (r.cqRing && r.cqRing != r.sqRing) { munmap(r.cqRing, r.cqRingSz); }
    r.cqRing = nullptr;
    if (r.sqRing) { munmap(r.sqRing, r.sqRingSz); r.sqRing = nullptr; }
    if (r.fd >= 0) { close(r.fd); r.fd = -1; }
    r.sqHead = r.sqTail = r.sqMask = r.sqArray = nullptr;
    r.cqHead = r.cqTail = r.cqMask = nullptr;
    r.cqes = nullptr;
    r.entries = 0;
}

InitResult Probe() noexcept {
    Ring r;
    const InitResult res = Init(r, 2);      
    if (res == InitResult::Ok) Shutdown(r);
    return res;
}

io_uring_sqe* GetSqe(Ring& r) noexcept {
    
    const unsigned tail = Atomic(r.sqTail).load(std::memory_order_relaxed);
    const unsigned head = Atomic(r.sqHead).load(std::memory_order_acquire);
    if ((tail - head) >= r.entries) return nullptr;      
    return &r.sqes[tail & *r.sqMask];
}

int Submit(Ring& r, unsigned waitFor) noexcept {
    
    const unsigned tail = Atomic(r.sqTail).load(std::memory_order_relaxed);

    Atomic(r.sqTail).store(tail + 1, std::memory_order_release);

    unsigned flags = 0;
    if (waitFor > 0) flags |= IORING_ENTER_GETEVENTS;
    const int n = sys_io_uring_enter(r.fd, 1, waitFor, flags, nullptr);
    return (n < 0) ? -errno : n;
}

int WaitCq(Ring& r, unsigned minComplete) noexcept {
    
    const int n = sys_io_uring_enter(r.fd, 0, minComplete, IORING_ENTER_GETEVENTS, nullptr);
    return (n < 0) ? -errno : 0;
}

unsigned Reap(Ring& r, io_uring_cqe* out, unsigned max) noexcept {
    const unsigned head = Atomic(r.cqHead).load(std::memory_order_relaxed);
    
    const unsigned tail = Atomic(r.cqTail).load(std::memory_order_acquire);

    unsigned n = tail - head;
    if (n > max) n = max;
    for (unsigned i = 0; i < n; ++i)
        out[i] = r.cqes[(head + i) & *r.cqMask];

    if (n) Atomic(r.cqHead).store(head + n, std::memory_order_release);
    return n;
}

bool PostWake(Ring& r, std::uint64_t sentinel) noexcept {
    io_uring_sqe* sqe = GetSqe(r);
    if (!sqe) return false;
    std::memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_NOP;
    sqe->user_data = sentinel;
    return Submit(r, 0) >= 0;
}

}} 

#endif 
