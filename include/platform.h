// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once

#include <cstddef>   
#include <cstdint>

#if defined(_WIN32)
    #define JLIB_PLATFORM_WINDOWS 1
#elif defined(__linux__)
    #define JLIB_PLATFORM_POSIX 1
    #define JLIB_PLATFORM_LINUX 1
#else
    #error "JLib::Scheduler supports Windows and Linux (x86-64 and AArch64)."
#endif

// Thread-local state read by code that may run on a fiber must go through a JLIB_NOINLINE
// accessor. A fiber can resume on another thread, and a compiler may keep the TLS address it
// looked up before the switch; the caller of a never-inlined accessor never sees the TLS access,
// so every call looks it up again. No thread_local may be read directly in an inline function.
#if defined(_MSC_VER)
    #define JLIB_NOINLINE __declspec(noinline)
#else
    #define JLIB_NOINLINE __attribute__((noinline))
#endif

#if defined(__x86_64__) || defined(_M_X64)
    #define JLIB_ARCH_X86_64 1
#elif defined(__aarch64__) || defined(_M_ARM64)
    #define JLIB_ARCH_AARCH64 1
#else
    
    #error "JLib::Scheduler supports x86-64 and AArch64: the fiber context switch is hand-written assembly."
#endif

#if JLIB_PLATFORM_WINDOWS

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#define NOGDI              
#define NOUSER             
#include <windows.h>
#include <intrin.h>
#undef NOGDI
#undef NOUSER

using affinity_mask_t = DWORD_PTR;
using native_handle_t = HANDLE;

#else  

#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>
#if JLIB_ARCH_X86_64
#include <xmmintrin.h>
#include <x86intrin.h>
#endif

using affinity_mask_t = cpu_set_t;
using native_handle_t = pthread_t;

#ifndef MAP_NORESERVE
#define MAP_NORESERVE 0
#endif

#endif

namespace JLib { namespace platform {

inline constexpr std::size_t kCacheLine = 128;

inline void* ReserveNoAccess(std::size_t bytes) {
#if JLIB_PLATFORM_WINDOWS
    return ::VirtualAlloc(nullptr, bytes, MEM_RESERVE, PAGE_NOACCESS);
#else
    
    void* p = ::mmap(nullptr, bytes, PROT_NONE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    return (p == MAP_FAILED) ? nullptr : p;
#endif
}

inline bool CommitReadWrite(void* addr, std::size_t bytes) {
#if JLIB_PLATFORM_WINDOWS
    return ::VirtualAlloc(addr, bytes, MEM_COMMIT, PAGE_READWRITE) != nullptr;
#else
    return ::mprotect(addr, bytes, PROT_READ | PROT_WRITE) == 0;
#endif
}

inline void ReleaseReservation(void* addr, std::size_t bytes) {
#if JLIB_PLATFORM_WINDOWS
    (void)bytes;                       
    ::VirtualFree(addr, 0, MEM_RELEASE);
#else
    ::munmap(addr, bytes);             
#endif
}

inline void CpuRelax() {
#if JLIB_ARCH_X86_64
    _mm_pause();
#elif defined(_MSC_VER)
    
    __yield();
#elif defined(JLIB_SPIN_HINT_YIELD)
    
    __asm__ __volatile__("yield" ::: "memory");
#elif defined(JLIB_SPIN_HINT_NOP)
    
    __asm__ __volatile__("nop; nop; nop; nop; nop; nop; nop; nop" ::: "memory");
#else
    
    __asm__ __volatile__("isb" ::: "memory");
#endif
}

inline const char* SpinHintName() {
#if JLIB_ARCH_X86_64
    return "pause";
#elif defined(JLIB_SPIN_HINT_ISB)
    return "isb";
#elif defined(JLIB_SPIN_HINT_NOP)
    return "nop8";
#else
    return "yield";
#endif
}

inline unsigned CurrentCpu() {
#if JLIB_PLATFORM_WINDOWS
    return ::GetCurrentProcessorNumber();
#else
    const int c = ::sched_getcpu();      
    return (c < 0) ? 0u : (unsigned)c;
#endif
}

inline std::size_t PageSize() {
#if JLIB_PLATFORM_WINDOWS
    return 4096;                       
#else
    return (std::size_t)::sysconf(_SC_PAGESIZE);
#endif
}

inline unsigned PopCount64(std::uint64_t x) {
#if defined(_MSC_VER)
    return (unsigned)__popcnt64(x);
#else
    return (unsigned)__builtin_popcountll(x);
#endif
}

inline unsigned CountTrailingZeros64(std::uint64_t x) {
#if defined(_MSC_VER)
    unsigned long i;
    _BitScanForward64(&i, x);
    return (unsigned)i;
#else
    return (unsigned)__builtin_ctzll(x);
#endif
}

// x must be non-zero.
inline unsigned CountLeadingZeros64(std::uint64_t x) {
#if defined(_MSC_VER)
    unsigned long i;
    _BitScanReverse64(&i, x);
    return 63u - (unsigned)i;
#else
    return (unsigned)__builtin_clzll(x);
#endif
}

#if JLIB_ARCH_X86_64
inline std::uint64_t ReadTsc() noexcept { return (std::uint64_t)__rdtsc(); }
#endif
}}
