// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <intrin.h>
#include <cstdio>
#include <cstdint>
#include <cstring>

extern "C" void     ContextSwitch(void** from, void** to);
extern "C" void     FiberTrampoline();
extern "C" void     FiberEntryAsm();
extern "C" uint64_t AbiProbe(void** from, void** to);

struct FiberCtx {
    void*    fibSp;    
    void*    mainSp;   
    uint64_t hits;     
};
static FiberCtx g_ctx{};

static const size_t kFrameBytes = 176;
static const int    kSlotX19    = 10;   
static const int    kSlotX20    = 11;   
static const int    kSlotX30    = 21;   

static void* MakeFiber(void* stackBase, size_t stackSize, void (*entry)(), void* ctx) {
    uintptr_t top = (uintptr_t)((char*)stackBase + stackSize) & ~(uintptr_t)0xF;
    uint64_t* f = (uint64_t*)(top - kFrameBytes);
    memset(f, 0, kFrameBytes);
    
    f[kSlotX19] = (uint64_t)entry;              
    f[kSlotX20] = (uint64_t)ctx;                
                                                
    f[kSlotX30] = (uint64_t)&FiberTrampoline;   
                                                
    return (void*)f;
}

static inline uint64_t GetFpcr() { return (uint64_t)_ReadStatusReg(ARM64_FPCR); }
static const uint64_t kFpcrFZ = (uint64_t)1 << 24;   

static bool GuardPageFaults(void* addr) {
    __try {
        *(volatile char*)addr = 1;
        return false;                 
    }
    __except (GetExceptionCode() == EXCEPTION_ACCESS_VIOLATION
                  ? EXCEPTION_EXECUTE_HANDLER : EXCEPTION_CONTINUE_SEARCH) {
        return true;
    }
}

static const char* kRegNames[18] = {
    "x19","x20","x21","x22","x23","x24","x25","x26","x27","x28",
    "d8","d9","d10","d11","d12","d13","d14","d15"
};

int main() {
    const size_t kStack = 64 * 1024;
    SYSTEM_INFO si{};
    GetSystemInfo(&si);
    const size_t kPage = si.dwPageSize;

    void* region = VirtualAlloc(nullptr, kStack, MEM_RESERVE, PAGE_NOACCESS);
    if (!region) { printf("VirtualAlloc reserve failed: %lu\n", GetLastError()); return 1; }
    if (!VirtualAlloc((char*)region + kPage, kStack - kPage, MEM_COMMIT, PAGE_READWRITE)) {
        printf("VirtualAlloc commit failed: %lu\n", GetLastError()); return 1;
    }

    g_ctx.fibSp = MakeFiber(region, kStack, FiberEntryAsm, &g_ctx);

    printf("frame %zu bytes, x19 slot +%d, x20 slot +%d, x30 slot +%d, page %zu\n",
           kFrameBytes, kSlotX19 * 8, kSlotX20 * 8, kSlotX30 * 8, kPage);

    const uint64_t mainFpcr = GetFpcr();

    ContextSwitch(&g_ctx.mainSp, &g_ctx.fibSp);
    ContextSwitch(&g_ctx.mainSp, &g_ctx.fibSp);
    const bool tripsOk = g_ctx.hits >= 2;
    printf("round trips        : %s (%llu so far)\n",
           tripsOk ? "ok" : "NO RETURN", (unsigned long long)g_ctx.hits);

    const uint64_t mask = AbiProbe(&g_ctx.mainSp, &g_ctx.fibSp);
    const bool gprOk = (mask & 0x3FF) == 0;          
    const bool fpOk  = (mask & 0x3FC00) == 0;        
    if (mask) {
        printf("  clobber mask %05llx ->", (unsigned long long)mask);
        for (int i = 0; i < 18; ++i) if (mask & (1ull << i)) printf(" %s", kRegNames[i]);
        printf("\n");
    }
    printf("callee-saved GPRs  : %s\n", gprOk ? "preserved" : "CLOBBERED (x19-x28)");
    printf("callee-saved d8-d15: %s\n", fpOk ? "preserved" : "CLOBBERED");

    const uint64_t nowFpcr = GetFpcr();
    const bool fpcrOk = (nowFpcr == mainFpcr);
    printf("FPCR isolation     : %s (main %016llx, now %016llx)\n",
           fpcrOk ? "preserved" : "LEAKED",
           (unsigned long long)mainFpcr, (unsigned long long)nowFpcr);
    if (!fpcrOk && ((nowFpcr ^ mainFpcr) & kFpcrFZ))
        printf("  -> FZ leaked out of the fiber: the FPCR slot is not being restored\n");

    const bool faulted = GuardPageFaults(region);
    printf("guard page         : %s\n",
           faulted ? "faults as expected (ACCESS_VIOLATION)" : "*** WRITABLE ***");

    const bool pass = tripsOk && gprOk && fpOk && fpcrOk && faulted;
    printf("\n%s\n", pass ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return pass ? 0 : 1;
}
