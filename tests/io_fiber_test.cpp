// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// The raw reactor, driven with fiber tasks. The reactor's `resume` is any task, run on completion:
//   1. continuation -- the resume is a fresh fiber task;
//   2. wait         -- a fiber submits with a continuation that finishes a group, then WaitFor()s it;
//   3. cancel       -- a read that never completes is cancelled and its continuation sees it.
// This is the PORTABLE I/O test: io_async_test is Windows-only, so on Linux this is what covers
// the io_uring pump.
// Modes (argv[1]): p = Mode::Pinned, k = one K. Combine: "pk".

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "Thread.h"
#include "WaitGroup.h"
#include "platform.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

#if !defined(_WIN32)
  #include <fcntl.h>
  #include <unistd.h>
#endif

// ---- the few platform calls a test needs: a temp file, an open for async reads, a pipe ----
#if defined(_WIN32)
static void TempPath(char* out, std::size_t n) {
    char dir[MAX_PATH];
    ::GetTempPathA(MAX_PATH, dir);
    std::snprintf(out, n, "%sjlib_io_fiber_test.bin", dir);
}
static void* OpenRead(const char* path) {
    HANDLE h = ::CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, nullptr,
                             OPEN_EXISTING, FILE_FLAG_OVERLAPPED, nullptr);
    return h == INVALID_HANDLE_VALUE ? nullptr : h;
}
static void CloseH(void* h) { ::CloseHandle(static_cast<HANDLE>(h)); }
static void RemoveFile(const char* path) { ::DeleteFileA(path); }
// Returns the end to read from; `other` is the end nobody writes to.
static void* OpenPipe(void** other) {
    const char* name = "\\\\.\\pipe\\jlib_io_fiber_test";
    HANDLE server = ::CreateNamedPipeA(name, PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
                                       PIPE_TYPE_BYTE | PIPE_WAIT, 1, 4096, 4096, 0, nullptr);
    HANDLE client = ::CreateFileA(name, GENERIC_READ | GENERIC_WRITE, 0, nullptr,
                                  OPEN_EXISTING, 0, nullptr);
    ::ConnectNamedPipe(server, nullptr);
    if (server == INVALID_HANDLE_VALUE || client == INVALID_HANDLE_VALUE) return nullptr;
    *other = client;
    return server;
}
#else
static void* FdHandle(int fd) { return reinterpret_cast<void*>(static_cast<std::intptr_t>(fd)); }
static void TempPath(char* out, std::size_t n) { std::snprintf(out, n, "/tmp/jlib_io_fiber_test.bin"); }
static void* OpenRead(const char* path) {
    const int fd = ::open(path, O_RDONLY);
    return fd < 0 ? nullptr : FdHandle(fd);
}
static void CloseH(void* h) { ::close(static_cast<int>(reinterpret_cast<std::intptr_t>(h))); }
static void RemoveFile(const char* path) { ::unlink(path); }
static void* OpenPipe(void** other) {
    int fds[2];
    if (::pipe(fds) != 0) return nullptr;
    *other = FdHandle(fds[1]);
    return FdHandle(fds[0]);
}
#endif

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-64s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

template <typename F>
static bool WaitUntil(F pred, int budgetMs = 5000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budgetMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

// One read. The file holds byte i at offset i (mod 256), so a read is checkable from its offset.
struct ReadOp {
    JLib::IoRequest req;
    JLib::IoResult  res;
    std::uint64_t   off = 0;
    char            buf[64] = {};
};

static bool ReadIsRight(const ReadOp& op) {
    if (!op.res.Ok() || op.res.bytes != sizeof op.buf) return false;
    for (std::size_t i = 0; i < sizeof op.buf; ++i)
        if ((unsigned char)op.buf[i] != (unsigned char)((op.off + i) & 0xFF)) return false;
    return true;
}

// ---- 1. continuation ----
static std::atomic<int> g_contRan{ 0 }, g_contBad{ 0 };
static void OnRead(void* p) {
    if (!ReadIsRight(*static_cast<ReadOp*>(p))) g_contBad.fetch_add(1, std::memory_order_relaxed);
    g_contRan.fetch_add(1, std::memory_order_relaxed);
}

// ---- 2. wait ----
struct WaiterCtx { void* h; int idx; };
static constexpr int kWaiters = 64, kWaitTimes = 16;
static std::atomic<int> g_waitReads{ 0 }, g_waitBad{ 0 }, g_waitMoved{ 0 };
static void Nothing(void*) {}   // the continuation: its end finishes the waiter's group

static void Waiter(void* p) {
    const WaiterCtx& c = *static_cast<WaiterCtx*>(p);
    auto& s  = JLib::TaskScheduler::Instance();
    auto& io = JLib::IoReactor::Instance();
    for (int i = 0; i < kWaitTimes; ++i) {
        ReadOp op;
        op.off = (std::uint64_t)((c.idx * kWaitTimes + i) * 7 % 4000);
        JLib::WaitGroup done;
        done.n.store(1, std::memory_order_relaxed);
        JLib::Task* cont = s.CreateTask(&Nothing, nullptr);
        cont->waitGroup = &done;

        JLib::Thread* at = JLib::Thread::GetCurrent();
        // true = finished without going in flight: no completion will come, so run it ourselves.
        if (io.SubmitRead(c.h, op.buf, sizeof op.buf, op.off, &op.req, &op.res, cont, JLib::CancelToken{}))
            s.Push(cont);
        s.WaitFor(done);

        if (ReadIsRight(op)) g_waitReads.fetch_add(1, std::memory_order_relaxed);
        else                 g_waitBad.fetch_add(1, std::memory_order_relaxed);
        if (JLib::Thread::GetCurrent() != at) g_waitMoved.fetch_add(1, std::memory_order_relaxed);
    }
}

// ---- 3. cancel ----
static std::atomic<int> g_cancelStatus{ -1 };
static void OnCancelled(void* p) {
    g_cancelStatus.store((int)static_cast<ReadOp*>(p)->res.status, std::memory_order_release);
}

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    const bool pin   = argc > 1 && std::strchr(argv[1], 'p');
    const bool withK = argc > 1 && std::strchr(argv[1], 'k');

    JLib::TaskScheduler::Config cfg;
    cfg.mode = pin ? JLib::Mode::Pinned : JLib::Mode::Migrate;
    cfg.main = JLib::MainMode::OutOfPool;
    if (withK) cfg.hotWorkers = 1;
    auto& io    = JLib::IoReactor::Instance();
    io.Start();   // the opt-in, BEFORE Init on purpose: it leaves a start hook and Init starts the
                  // pump (io_async_test covers Start after Init). Join stops it.
    JLib::TaskScheduler::Init(cfg);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("IoReactor from fibers -- workers=%zu K=%zu %s\n\n", sched.GetWorkerCount(),
                JLib::TaskScheduler::GetHotWorkers(), pin ? "Pinned" : "Migrate");

    char path[512];
    TempPath(path, sizeof path);
    {
        FILE* f = std::fopen(path, "wb");
        for (int i = 0; i < 4096; ++i) std::fputc(i & 0xFF, f);
        std::fclose(f);
    }
    void* h = OpenRead(path);
    Check(h != nullptr, "opened the file for async reads");
    Check(io.Register(h), "registered the handle");

    std::printf("continuation tasks (the resume is a fresh fiber)\n");
    {
        constexpr int kOps = 512;
        static ReadOp ops[kOps];
        JLib::WaitGroup wg;
        wg.n.store(kOps, std::memory_order_relaxed);
        for (int i = 0; i < kOps; ++i) {
            ops[i].off = (std::uint64_t)(i * 7 % 4000);
            JLib::Task* t = sched.CreateTask(&OnRead, &ops[i], JLib::Lane::Normal, JLib::TaskType::Fiber);
            t->waitGroup = &wg;
            const bool now = io.SubmitRead(h, ops[i].buf, sizeof ops[i].buf, ops[i].off,
                                           &ops[i].req, &ops[i].res, t, JLib::CancelToken{});
            if (now) sched.Push(t);
        }
        sched.WaitFor(wg);
        Check(g_contRan.load() == kOps, "every continuation ran, once");
        Check(g_contBad.load() == 0, "each saw its own read, with the right bytes");
        Check(WaitUntil([&] { return io.InFlight() == 0; }), "nothing left in flight");
    }

    std::printf("fibers that wait for their reads (%d x %d)\n", kWaiters, kWaitTimes);
    {
        static WaiterCtx ctx[kWaiters];
        JLib::WaitGroup wg;
        wg.n.store(kWaiters, std::memory_order_relaxed);
        for (int i = 0; i < kWaiters; ++i) {
            ctx[i] = WaiterCtx{ h, i };
            JLib::Task* t = sched.CreateTask(&Waiter, &ctx[i], JLib::Lane::Normal, JLib::TaskType::Fiber);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        Check(g_waitReads.load() == kWaiters * kWaitTimes, "every wait returned with its read done");
        Check(g_waitBad.load() == 0, "and the right bytes");
        if (pin) {
            Check(g_waitMoved.load() == 0, "Pinned: no waiter came back on another worker");
            if (g_waitMoved.load()) std::printf("      moved %d times\n", g_waitMoved.load());
        }
    }
    CloseH(h);

    std::printf("a read that never completes is cancelled\n");
    {
        void* client = nullptr;
        void* server = OpenPipe(&client);
        Check(server != nullptr, "pipe pair open");
        Check(io.Register(server), "registered the pipe");

        static ReadOp op;
        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        JLib::Task* t = sched.CreateTask(&OnCancelled, &op, JLib::Lane::Normal, JLib::TaskType::Fiber);
        t->waitGroup = &wg;
        const bool now = io.SubmitRead(server, op.buf, sizeof op.buf, 0, &op.req, &op.res, t,
                                       JLib::CancelToken{});
        Check(!now, "the read went in flight");
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        Check(g_cancelStatus.load() == -1, "and has not completed -- nothing was written");
        Check(io.RequestCancel(JLib::CancelToken{}) == 1, "one operation asked to cancel");
        sched.WaitFor(wg);
        Check(g_cancelStatus.load() == (int)JLib::IoStatus::Cancelled, "its continuation saw Cancelled");
        Check(WaitUntil([&] { return io.InFlight() == 0; }), "nothing left in flight");
        CloseH(client);
        CloseH(server);
    }

    RemoveFile(path);
    JLib::detail::TeardownForTesting(sched);   // Join: the reactor Stop runs with the pool still up
    std::printf("\n%s\n", g_fail ? "SOME CHECKS FAILED" : "ALL CHECKS PASSED");
    return g_fail ? 1 : 0;
}
