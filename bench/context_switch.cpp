// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <windows.h>

#include "Fiber.h"
#include "Context.h"
#include "TaskScheduler.h"   
#include "../tests/fiber_body.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#if defined(_MSC_VER)
  #include <intrin.h>
#endif

using CsFn  = void (*)(Context*, Context*);
using PreFn = void (*)();

extern "C" {
    
    void CsSse(Context*, Context*);
    void CsSseVzu(Context*, Context*);
    void CsAvx(Context*, Context*);

    void BenchDirtyUpper();
    void BenchCleanUpper();

    void BenchClobberXmm();
    void BenchXmmRoundTrip(Context* from, Context* to, CsFn fn, void* out160);
}

namespace {

Context      g_main;
JLib::Fiber  g_fiber;
CsFn volatile  g_fn  = nullptr;
PreFn volatile g_pre = nullptr;

void FiberLoop() {
    for (;;) {
        g_pre();
        g_fn(&g_fiber.ctx, &g_main);
    }
}

void StartFiber() {
    const size_t kStack = 64 * 1024;
    void* mem = ::VirtualAlloc(nullptr, kStack, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    if (!mem) { std::fprintf(stderr, "VirtualAlloc failed\n"); std::exit(2); }
    g_fiber.stackBase = mem;
    g_fiber.stackSize = kStack;
    
    g_fiber.Init(&FiberLoop);
}

bool VerifyVariant(CsFn fn, const char* label) {
    g_fn  = fn;
    g_pre = &BenchClobberXmm;          

    alignas(16) unsigned char out[160];
    std::memset(out, 0, sizeof out);
    BenchXmmRoundTrip(&g_main, &g_fiber.ctx, fn, out);

    int bad = -1;
    for (int r = 0; r < 10 && bad < 0; ++r) {
        const unsigned char want = static_cast<unsigned char>(0xA6 + r);
        for (int b = 0; b < 16; ++b)
            if (out[r * 16 + b] != want) { bad = r; break; }
    }
    if (bad < 0) {
        std::printf("  %-34s XMM6-15 survived a clobbered round trip   ok\n", label);
        return true;
    }
    std::printf("  %-34s XMM%d CAME BACK AS 0x%02X            FAILED\n",
                label, 6 + bad, out[bad * 16]);
    return false;
}

struct Row {
    const char* label;
    CsFn        fn;
    PreFn       pre;
    const char* note;
};

double g_qpcFreq = 0.0;

double TimeRow(const Row& row, long long iters) {
    g_fn  = row.fn;
    g_pre = row.pre;

    LARGE_INTEGER a, b;
    ::QueryPerformanceCounter(&a);
    for (long long i = 0; i < iters; ++i) {
        g_pre();
        g_fn(&g_main, &g_fiber.ctx);
    }
    ::QueryPerformanceCounter(&b);

    const double secs = double(b.QuadPart - a.QuadPart) / g_qpcFreq;
    return secs * 1e9 / double(iters * 2);
}

double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return n & 1 ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

extern "C" unsigned char JLibCtxHasAvx;   

std::atomic<long long> g_wakes{ 0 };
PreFn volatile         g_schedPre = nullptr;

struct ParkCtx { JLib::TaskScheduler* sched; JLib::Event* ev; long long iters; };
static void ParkLoopBody(void* p) {
    auto& c = *static_cast<ParkCtx*>(p);
    for (long long i = 0; i < c.iters; ++i) {
        g_schedPre();                 
        c.sched->WaitOnEvent(*c.ev);
        g_wakes.fetch_add(1, std::memory_order_release);
    }
}

double TimeSuspendResume(bool gateOn, PreFn pre, long long iters) {
    JLibCtxHasAvx = gateOn ? 1u : 0u;
    g_schedPre    = pre;
    g_wakes.store(0, std::memory_order_relaxed);

    auto& sched = JLib::TaskScheduler::Instance();
    auto& ev    = sched.GetEvent("cs-bench-suspend-resume");

    JLib::WaitGroup wg;
    wg.n.fetch_add(1, std::memory_order_relaxed);

    ParkCtx pctx{ &sched, &ev, iters };
    auto* t = JLibTest::MakeCtxTask(sched, &ParkLoopBody, &pctx);
    if (!t) { std::fprintf(stderr, "CreateTask returned null\n"); std::exit(2); }
    t->waitGroup = &wg;
    sched.Push(t);

    while (g_wakes.load(std::memory_order_acquire) == 0) ev.SignalAll();

    LARGE_INTEGER a, b;
    ::QueryPerformanceCounter(&a);
    long long seen = 1;
    while (seen < iters) {
        ev.SignalAll();
        
        seen = g_wakes.load(std::memory_order_acquire);
    }
    ::QueryPerformanceCounter(&b);

    sched.WaitFor(wg);
    const double secs = double(b.QuadPart - a.QuadPart) / g_qpcFreq;
    return secs * 1e9 / double(iters - 1);
}

void PrintCpuBrand() {
#if defined(_MSC_VER)
    int r[4]{};
    char brand[49]{};
    __cpuid(r, 0x80000000);
    if (unsigned(r[0]) >= 0x80000004u) {
        for (int i = 0; i < 3; ++i) {
            __cpuid(r, 0x80000002 + i);
            std::memcpy(brand + i * 16, r, 16);
        }
        std::printf("cpu: %s\n", brand);
    }
#endif
}

} 

int main(int argc, char** argv) {
    long long iters = 50000;   
    int       reps  = 25;
    int       core  = 0;
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--iters") && i + 1 < argc) iters = std::atoll(argv[++i]);
        else if (!std::strcmp(argv[i], "--reps") && i + 1 < argc) reps = std::atoi(argv[++i]);
        else if (!std::strcmp(argv[i], "--core") && i + 1 < argc) core = std::atoi(argv[++i]);
        else { std::printf("usage: %s [--iters N] [--reps N] [--core N]\n", argv[0]); return 1; }
    }

    {
        PROCESS_POWER_THROTTLING_STATE pt{};
        pt.Version = PROCESS_POWER_THROTTLING_CURRENT_VERSION;
        const BOOL  gotQos = ::GetProcessInformation(::GetCurrentProcess(), ProcessPowerThrottling,
                                                     &pt, sizeof pt);
        const DWORD pc  = ::GetPriorityClass(::GetCurrentProcess());
        const bool  eco = gotQos && (pt.ControlMask & PROCESS_POWER_THROTTLING_EXECUTION_SPEED)
                                 && (pt.StateMask   & PROCESS_POWER_THROTTLING_EXECUTION_SPEED);
        std::printf("conditions: priority class 0x%lX%s, EcoQoS %s\n",
                    (unsigned long)pc,
                    pc == NORMAL_PRIORITY_CLASS ? " (NORMAL)"
                      : pc == IDLE_PRIORITY_CLASS ? " (IDLE -- THROTTLED, do not report this run)"
                      : pc == HIGH_PRIORITY_CLASS ? " (HIGH)" : "",
                    eco ? "ON -- THROTTLED, do not report this run" : (gotQos ? "off" : "unknown"));
    }
    PrintCpuBrand();

#if defined(_MSC_VER)
    if (!::IsProcessorFeaturePresent(PF_AVX_INSTRUCTIONS_AVAILABLE)) {
        std::printf("this CPU has no AVX -- there is no SSE/AVX transition to measure here.\n");
        return 0;
    }
#endif

    const DWORD_PTR prev = ::SetThreadAffinityMask(::GetCurrentThread(), (DWORD_PTR)1 << core);
    ::SetThreadPriority(::GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
    std::printf("pinned to cpu %d (%s), thread priority %d, running on cpu %u\n",
                core, prev ? "ok" : "AFFINITY FAILED",
                ::GetThreadPriority(::GetCurrentThread()), ::GetCurrentProcessorNumber());

    LARGE_INTEGER f;
    ::QueryPerformanceFrequency(&f);
    g_qpcFreq = double(f.QuadPart);

    StartFiber();

    std::printf("\ncorrectness gate (each variant round-trips XMM6-15 past a deliberate clobber)\n");
    bool ok = true;
    ok &= VerifyVariant(&CsSse,        "movdqa (bench copy)");
    ok &= VerifyVariant(&ContextSwitch,"movdqa (shipped library symbol)");
    ok &= VerifyVariant(&CsSseVzu,     "vzeroupper + movdqa");
    ok &= VerifyVariant(&CsAvx,        "vmovdqa (VEX)");
    if (!ok) {
        std::printf("\nA VARIANT DOES NOT PRESERVE THE ABI. No timings printed -- they would be\n"
                    "measuring a routine that is not doing the job.\n");
        return 1;
    }

    const Row rows[] = {
        { "movdqa            dirty upper", &CsSse,         &BenchDirtyUpper, "the OLD shipped behaviour" },
        { "movdqa            dirty upper", &CsSse,         &BenchDirtyUpper, "SAME-VS-SAME CONTROL -- must read 1.00x" },
        { "SHIPPED [library] dirty upper", &ContextSwitch, &BenchDirtyUpper, "what ships NOW: CPUID-gated vzeroupper" },
        { "vzeroupper+movdqa dirty upper", &CsSseVzu,      &BenchDirtyUpper, "ungated: row 3 minus this is the gate's cost" },
        { "vmovdqa (VEX)     dirty upper", &CsAvx,         &BenchDirtyUpper, "the alternative that was not taken" },
        { "movdqa            CLEAN upper", &CsSse,         &BenchCleanUpper, "the floor -- no dirty state to transition out of" },
        
        { "SHIPPED [library] CLEAN upper", &ContextSwitch, &BenchCleanUpper, "what the fix COSTS a non-AVX workload" },
    };
    const int kRows = int(sizeof rows / sizeof rows[0]);

    std::printf("\n%lld iterations/rep (%lld switches), %d reps, interleaved\n",
                iters, iters * 2, reps);

    for (int r = 0; r < kRows; ++r) (void)TimeRow(rows[r], iters / 10 + 1);

    std::vector<std::vector<double>> samples(kRows);
    int foregroundHits = 0, foregroundSamples = 0;

    const HWND self         = ::GetConsoleWindow();
    const bool focusKnowable = self && ::IsWindowVisible(self);

    for (int rep = 0; rep < reps; ++rep) {
        for (int r = 0; r < kRows; ++r)
            samples[r].push_back(TimeRow(rows[r], iters));
        ++foregroundSamples;
        if (focusKnowable && ::GetForegroundWindow() == self) ++foregroundHits;
    }

    const double base = Median(samples[0]);
    std::printf("\n%-32s %10s %10s %8s   %s\n", "", "median ns", "min ns", "ratio", "");
    for (int r = 0; r < kRows; ++r) {
        const double med = Median(samples[r]);
        const double mn  = *std::min_element(samples[r].begin(), samples[r].end());
        std::printf("%-32s %10.2f %10.2f %7.3fx   %s\n",
                    rows[r].label, med, mn, med / base, rows[r].note);
    }

    if (!focusKnowable) {
        std::printf("\n==> FOREGROUND unknown -- running under a pseudo-console (Windows Terminal),\n"
                    "    where this process owns no visible window to compare against.\n");
    } else {
        const double fgPct = foregroundSamples ? 100.0 * foregroundHits / foregroundSamples : 0.0;
        std::printf("\n==> FOREGROUND %.1f%% of the run\n", fgPct);
        if (fgPct > 2.0 && fgPct < 98.0)
            std::printf("    focus changed mid-run -- this run is uninterpretable, re-run it.\n");
    }

    const double ctrl = Median(samples[1]) / base;
    if (ctrl < 0.98 || ctrl > 1.02)
        std::printf("    SAME-VS-SAME CONTROL READS %.3fx -- the harness is not resolving this\n"
                    "    difference. Do not read anything into the other rows.\n", ctrl);

    ::SetThreadAffinityMask(::GetCurrentThread(), prev ? prev : ~(DWORD_PTR)0);
    ::SetThreadPriority(::GetCurrentThread(), THREAD_PRIORITY_NORMAL);

    JLib::TaskScheduler::Init(0);   

    struct SchedRow { const char* label; bool gate; PreFn pre; const char* note; };
    const SchedRow srows[] = {
        { "gate OFF (old)  dirty upper", false, &BenchDirtyUpper, "an AVX fiber BEFORE the change" },
        { "gate ON  (new)  dirty upper", true,  &BenchDirtyUpper, "the same fiber AFTER it" },
        { "gate OFF (old)  CLEAN upper", false, &BenchCleanUpper, "a non-AVX fiber before" },
        { "gate ON  (new)  CLEAN upper", true,  &BenchCleanUpper, "...and after -- the cost, if any" },
        { "gate OFF (old)  dirty upper", false, &BenchDirtyUpper, "SAME-VS-SAME CONTROL -- must match row 1" },
    };
    const int kSRows = int(sizeof srows / sizeof srows[0]);

    const long long srIters = 20000;
    const int       srReps  = 21;
    std::printf("\n\nEvent round trip through the pool (client path) -- %u workers, %lld round trips/rep, %d reps\n",
                (unsigned)JLib::TaskScheduler::Instance().GetWorkerCount(), srIters, srReps);
    std::printf("the gate is toggled at RUNTIME, so old and new are the same binary, interleaved\n");

    for (int r = 0; r < kSRows; ++r) (void)TimeSuspendResume(srows[r].gate, srows[r].pre, 2000);

    std::vector<std::vector<double>> ss(kSRows);
    for (int rep = 0; rep < srReps; ++rep)
        for (int r = 0; r < kSRows; ++r)
            ss[r].push_back(TimeSuspendResume(srows[r].gate, srows[r].pre, srIters));

    const double sbase = Median(ss[0]);
    std::printf("\n%-30s %10s %10s %8s   %s\n", "", "median ns", "min ns", "ratio", "");
    for (int r = 0; r < kSRows; ++r) {
        const double med = Median(ss[r]);
        const double mn  = *std::min_element(ss[r].begin(), ss[r].end());
        std::printf("%-30s %10.1f %10.1f %7.3fx   %s\n",
                    srows[r].label, med, mn, med / sbase, srows[r].note);
    }

    std::printf("\n    saves %+.0f ns per Event round trip on an AVX fiber    (row 1 - row 2)\n"
                "    costs %+.0f ns per Event round trip on a non-AVX one  (row 4 - row 3)\n"
                "    two switches' worth either way; compare against %.0f ns of raw switch delta.\n",
                Median(ss[0]) - Median(ss[1]),
                Median(ss[3]) - Median(ss[2]),
                2.0 * (Median(samples[0]) - Median(samples[2])));

    const double sctrl = Median(ss[4]) / sbase;
    if (sctrl < 0.95 || sctrl > 1.05)
        std::printf("\n    SAME-VS-SAME CONTROL READS %.3fx -- this section is not resolving its\n"
                    "    own difference. Do not read anything into its other rows.\n", sctrl);

    JLibCtxHasAvx = 1;   
    return 0;
}
