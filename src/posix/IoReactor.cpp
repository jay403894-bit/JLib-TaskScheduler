// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#if defined(__linux__)

#include "../../include/IoReactor.h"
#include "../../include/TaskScheduler.h"
#include "../IoPlatform.h"
#include "IoUring.h"
#include "../../include/platform.h"
#include "../../include/Timer.h"

#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>
#include <chrono>

namespace JLib {

static_assert(sizeof(struct msghdr) + IoRequest::kMaxVectors * sizeof(struct iovec)
                  <= IoRequest::kNativeBytes,
              "IoRequest::kNativeBytes cannot hold msghdr plus kMaxVectors iovecs");

static struct iovec* Iov(IoRequest* r) {
    return reinterpret_cast<struct iovec*>(r->native + sizeof(struct msghdr));
}

namespace ioplat {
    
    bool FillBufs(IoRequest* r, const IoBuffer* bufs, std::uint32_t count) noexcept {
        if (count == 0 || count > IoRequest::kMaxVectors) return false;
        struct iovec* v = Iov(r);
        for (std::uint32_t i = 0; i < count; ++i) {
            v[i].iov_base = bufs[i].data;
            v[i].iov_len  = static_cast<std::size_t>(bufs[i].len);
        }
        return true;
    }

    std::uint32_t AdvanceBufs(IoRequest* r, std::uint32_t consumed) noexcept {
        struct iovec* v = Iov(r);
        std::uint32_t n = r->bufCount;
        std::uint32_t i = 0;
        while (i < n && consumed > 0) {
            if (consumed >= v[i].iov_len) { consumed -= (std::uint32_t)v[i].iov_len; ++i; continue; }
            v[i].iov_base = static_cast<unsigned char*>(v[i].iov_base) + consumed;
            v[i].iov_len -= consumed;
            consumed = 0;
        }
        if (i == 0) return n;
        const std::uint32_t left = n - i;
        for (std::uint32_t k = 0; k < left; ++k) v[k] = v[i + k];
        return left;
    }

    std::size_t BufsRemaining(IoRequest* r) noexcept {
        struct iovec* v = Iov(r);
        std::size_t t = 0;
        for (std::uint32_t i = 0; i < r->bufCount; ++i) t += v[i].iov_len;
        return t;
    }
}

namespace {

    constexpr std::size_t kShards = 16;

    // A NOP posted to end K's ring wait (Thread::Wake of the ring waiter).
    constexpr std::uint64_t kWakeSentinel   = 1;
    // The completion of an ASYNC_CANCEL request: nothing to do.
    constexpr std::uint64_t kCancelSentinel = 2;

    const bool ioTrace = [] {
        const char* v = std::getenv("JLIB_IO_TRACE");
        return v && *v && *v != '0';
    }();
}

struct IoReactor::Impl {
    uring::Ring ring;
    bool        ringUp = false;

    // The SQ has one producer at a time: any submitting thread, under this lock.
    std::mutex submitMx;

    // The CQ has exactly one consumer, and it is the pump -- io_uring's CQ is single-consumer by
    // design, and nothing else reaps. That is also why no claim is needed around it.
    std::thread pump;

    struct Shard {
        mutable std::mutex m;
        IoRequest* head = nullptr;
        char pad[platform::kCacheLine];
    };
    Shard shards[kShards];

    Shard& ShardFor(const IoRequest* r) noexcept {
        const std::uintptr_t x = reinterpret_cast<std::uintptr_t>(r) >> 4;
        return shards[(x * 0x9E3779B97F4A7C15ull) >> 60 & (kShards - 1)];
    }

    void Link(IoRequest* r) noexcept {
        Shard& s = ShardFor(r);
        std::lock_guard<std::mutex> lk(s.m);
        r->prev = nullptr;
        r->next = s.head;
        if (s.head) s.head->prev = r;
        s.head = r;
        detail::g_ioOutstanding.fetch_add(1, std::memory_order_relaxed);
    }

    void UnlinkLocked(IoRequest* r) noexcept {
        Shard& s = ShardFor(r);
        if (r->prev) r->prev->next = r->next;
        else if (s.head == r) s.head = r->next;
        if (r->next) r->next->prev = r->prev;
        r->prev = r->next = nullptr;
        detail::g_ioOutstanding.fetch_sub(1, std::memory_order_relaxed);
    }

    void UnlinkUnlocked(IoRequest* r) noexcept {
        std::lock_guard<std::mutex> lk(ShardFor(r).m);
        UnlinkLocked(r);
    }

    std::mutex               life;
    std::atomic<bool>        stopping{ true };

    // The pump: a plain thread, not a pool worker, and the CQ's only reader. It sleeps in the ring
    // wait, moves every completion into the injector (no wake -- every worker takes from it each
    // pass), and goes straight back. It runs nothing itself. Workers never touch the CQ: reaping is
    // a shared-memory read, but its tail is one cache line every worker would hit on every pass.
    // Same design as the Windows pump; see tests/verify/kport_model.c.
    static constexpr unsigned     kBatch     = 64;

    // Stop() posts one of these to end the pump; nothing else does.
    void PostWake() noexcept {
        std::lock_guard<std::mutex> lk(submitMx);
        uring::PostWake(ring, kWakeSentinel);
    }

    // One CQE: publish the result and return the resume to run, or null if there is none, it was
    // handed on (a pinned resume goes where its pin says), or the op was resubmitted.
    Task* Complete(const io_uring_cqe& c);

    // A batch, ALL of it to the injector in one bulk push (no wake: every worker reads the
    // injector each pass). Pinned resumes never get here -- Complete() routes them through
    // WakeTask. Returns the number of stop markers seen.
    unsigned Dispatch(const io_uring_cqe* cs, unsigned n) {
        Task* ts[kBatch];
        std::size_t nt = 0;
        unsigned markers = 0;
        for (unsigned i = 0; i < n; ++i) {
            if (cs[i].user_data == kWakeSentinel)   { ++markers; continue; }
            if (cs[i].user_data == kCancelSentinel) continue;
            if (Task* t = Complete(cs[i])) ts[nt++] = t;
        }
        Inject(ts, nt);
        return markers;
    }

    static void Inject(Task** ts, std::size_t n) {
        if (!n || !TaskScheduler::IsInitialized()) return;
        if (TaskScheduler::PushInjector(ts, n)) {
            detail::g_ioToLane.fetch_add(n, std::memory_order_relaxed);
            return;
        }
        // Injector refused (pool stopping): each to a compute worker, as before the injector.
        detail::g_ioFloorFallback.fetch_add(n, std::memory_order_relaxed);
        auto& s = TaskScheduler::Instance();
        for (std::size_t i = 0; i < n; ++i) s.PushTo(ts[i], CorePref::Any, true);
    }

    // The pump's whole life: sleep in the ring wait, inject, repeat -- until Stop() posts a marker.
    // Above the compute workers, as the clock is (best effort: a negative nice needs privileges).
    void Pump() {
        (void)::syscall(SYS_setpriority, PRIO_PROCESS, (int)::syscall(SYS_gettid), -5);
        for (;;) {
            // Returns at once if the CQ already holds anything; otherwise sleeps until it does.
            uring::WaitCq(ring, 1);
            io_uring_cqe cs[kBatch];
            const unsigned n = uring::Reap(ring, cs, kBatch);
            if (n == 0) {
                if (stopping.load(std::memory_order_acquire)) return;   // never spin on a dead ring
                continue;
            }
            if (Dispatch(cs, n) != 0 && stopping.load(std::memory_order_acquire)) return;
        }
    }
};

Task* IoReactor::Impl::Complete(const io_uring_cqe& c) {
    IoRequest* r = reinterpret_cast<IoRequest*>(static_cast<std::uintptr_t>(c.user_data));
    if (!r) return nullptr;

    if (ioTrace) {
        std::fprintf(stderr, "[io] CQE    req=%p res=%d kind=%d bufCount=%u\n",
                     (void*)r, (int)c.res, (int)r->kind, (unsigned)r->bufCount);
        std::fflush(stderr);
    }

    IoResult res{};
    if (c.res >= 0) {
        res.status = IoStatus::Completed;
        res.error  = 0;

        if (r->kind == IoRequest::Kind::Accept) {
            r->aux    = static_cast<std::uintptr_t>(c.res);
            res.bytes = 0;
        } else {
            res.bytes = static_cast<std::uint32_t>(c.res);

            if (r->kind == IoRequest::Kind::Recv && r->aux) {
                auto* mh   = reinterpret_cast<struct msghdr*>(r->native);
                auto* addr = reinterpret_cast<IoAddress*>(r->aux);
                const std::size_t got = static_cast<std::size_t>(mh->msg_namelen);
                addr->len = static_cast<std::int32_t>(
                    (got > IoAddress::kBytes) ? IoAddress::kBytes : got);
            }
        }
    } else if (c.res == -ECANCELED) {

        res.status = IoStatus::Cancelled;
        res.bytes  = 0;
        res.error  = ECANCELED;
    } else {
        res.status = IoStatus::Failed;
        res.bytes  = 0;
        res.error  = static_cast<std::uint32_t>(-c.res);
    }

    if (res.status == IoStatus::Completed && r->kind == IoRequest::Kind::Send
        && r->bufCount > 0 && c.res > 0
        && static_cast<std::size_t>(c.res) < ioplat::BufsRemaining(r)) {

        r->xferred += static_cast<std::uint32_t>(c.res);
        r->bufCount = ioplat::AdvanceBufs(r, static_cast<std::uint32_t>(c.res));

        {
            std::lock_guard<std::mutex> lk(ShardFor(r).m);
            UnlinkLocked(r);
        }

        if (ioTrace) {
            std::fprintf(stderr,
                "[io] PARTIAL req=%p sent=%d total=%u remaining=%zu segs=%u -- resubmit\n",
                (void*)r, (int)c.res, (unsigned)r->xferred,
                ioplat::BufsRemaining(r), (unsigned)r->bufCount);
            std::fflush(stderr);
        }

        if (!IoReactor::Instance().SubmitPrepared(r)) return nullptr;
        res = r->out ? *r->out : res;
    }

    if (res.status == IoStatus::Completed && r->kind == IoRequest::Kind::Send)
        res.bytes += r->xferred;

    Task* resume = nullptr;
    {
        std::lock_guard<std::mutex> lk(ShardFor(r).m);
        UnlinkLocked(r);
        if (r->out) *r->out = res;
        resume = r->resume;
    }

    if (r->onComplete) r->onComplete(r);   // may resubmit or free r: not touched after

    if (resume && TaskScheduler::IsInitialized() && TaskScheduler::IsPinned(resume)) {
        TaskScheduler::Instance().WakeTask(resume);
        return nullptr;
    }
    return resume;
}

IoReactor::IoReactor() : impl(new Impl()) {
    if (uring::Init(impl->ring, 256) == uring::InitResult::Ok) impl->ringUp = true;
}

IoReactor::~IoReactor() {
    Stop();
    if (impl->ringUp) uring::Shutdown(impl->ring);
    delete impl;
}

IoReactor& IoReactor::Instance() { static IoReactor* r = new IoReactor(); return *r; }

bool IoReactor::IsAvailable() noexcept {

    {
        static const bool off = [] {
            const char* v = std::getenv("JLIB_IO_URING_OFF");
            return v && *v && *v != '0';
        }();
        if (off) return false;

        static const bool forced = [] {
            const char* v = std::getenv("JLIB_IO_URING_FORCE");
            return v && *v && *v != '0';
        }();
        if (forced) return uring::Probe() == uring::InitResult::Ok;
    }

    static const bool probed = (uring::Probe() == uring::InitResult::Ok);

    return probed;
}

namespace {
    void StartIoHook() { IoReactor::Instance().Start(); }
    void StopIoHook()  { IoReactor::Instance().Stop(); }
}

// THE OPT-IN. Nothing I/O exists until this runs: it starts the pump and registers the stop hook
// Join calls. Before or after Init: before, it only leaves a start hook and Init starts the pump
// once the pool can take completions (see detail::g_ioStartHook). A no-op where io_uring is
// unavailable (IsAvailable() says so up front).
void IoReactor::Start() noexcept {
    if (!TaskScheduler::IsInitialized()) {
        detail::g_ioStartHook.store(&StartIoHook, std::memory_order_release);
        return;
    }
    std::lock_guard<std::mutex> lk(impl->life);
    if (!impl->ringUp) return;
    impl->stopping.store(false, std::memory_order_release);
    if (!impl->pump.joinable()) impl->pump = std::thread([p = impl] { p->Pump(); });
    detail::g_ioStopHook.store(&StopIoHook, std::memory_order_release);
}

// Called from Join (through the hook) while the pool still runs. Cancels everything in flight and
// lets the pump drain the CQ -- it stays the only reader to the end -- so every resume is handed
// to the pool before it stops. A request that ignores its cancel would hold this forever, so the
// wait gives up after a bounded spell with no progress. Then the stop marker ends the pump.
void IoReactor::Stop() noexcept {
    {
        std::lock_guard<std::mutex> lk(impl->life);
        if (impl->stopping.load(std::memory_order_acquire)) return;
        impl->stopping.store(true, std::memory_order_release);
    }
    detail::g_ioStopHook.store(nullptr, std::memory_order_release);

    RequestCancel(CancelToken{});

    int quietMs = 0;
    std::size_t last = detail::g_ioOutstanding.load(std::memory_order_acquire);
    while (last != 0 && quietMs < 2000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        const std::size_t now = detail::g_ioOutstanding.load(std::memory_order_acquire);
        quietMs = (now == last) ? quietMs + 10 : 0;   // quiet = no completion in the step
        last = now;
    }

    // `stopping` is already set, so the pump returns on this marker (nothing else posts one).
    if (impl->pump.joinable()) {
        impl->PostWake();
        impl->pump.join();
    }
    if (const std::size_t left = detail::g_ioOutstanding.load(std::memory_order_acquire)) {
        std::fprintf(stderr, "[JLib::Scheduler] I/O stop: %zu request(s) still outstanding after "
                             "cancel; their resumes will not run.\n", left);
    }
}

std::size_t IoReactor::InFlight() const noexcept {
    return detail::g_ioOutstanding.load(std::memory_order_relaxed);
}

bool IoReactor::Register(void*)            { return true; }
bool IoReactor::InitSockets()              { return true; }
bool IoReactor::RegisterSocket(IoSocket)   { return true; }

template <typename Fill>
static bool SubmitOp(IoReactor::Impl* impl, IoRequest* req, IoResult* out,
                     Task* resume, CancelToken token, Fill&& fill) {
    const std::uint32_t tok = token.Raw();

    if (CancelToken(tok).Cancelled()) {
        if (out) *out = IoResult{ IoStatus::Cancelled, 0, 0 };
        return true;
    }

    if (!impl->ringUp) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    if (impl->stopping.load(std::memory_order_acquire)) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ESHUTDOWN };
        return true;
    }

    req->out    = out;
    req->resume = resume;
    req->token  = tok;

    impl->Link(req);

    {
        std::lock_guard<std::mutex> lk(impl->submitMx);
        io_uring_sqe* sqe = uring::GetSqe(impl->ring);
        if (!sqe) {
            
            impl->UnlinkUnlocked(req);
            if (out) *out = IoResult{ IoStatus::Failed, 0, EAGAIN };
            return true;
        }
        std::memset(sqe, 0, sizeof(*sqe));
        fill(sqe);
        sqe->user_data = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(req));

        const int rc = uring::Submit(impl->ring, 0);

        if (ioTrace) {
            std::fprintf(stderr,
                "[io] SUBMIT op=%u fd=%d addr=%#llx len=%u off=%#llx flags=%#x kind=%d "
                "bufCount=%u iov0={%p,%zu} rc=%d req=%p\n",
                (unsigned)sqe->opcode, (int)sqe->fd,
                (unsigned long long)sqe->addr, (unsigned)sqe->len,
                (unsigned long long)sqe->off, (unsigned)sqe->msg_flags,
                (int)req->kind, (unsigned)req->bufCount,
                Iov(req)[0].iov_base, (size_t)Iov(req)[0].iov_len,
                rc, (void*)req);
            std::fflush(stderr);
        }
        if (rc < 0) {
            impl->UnlinkUnlocked(req);
            if (out) *out = IoResult{ IoStatus::Failed, 0, -rc };
            return true;
        }
    }

    return false;      
}

bool IoReactor::SubmitRecv(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Recv;
    req->handle = reinterpret_cast<void*>(s);
    Iov(req)[0].iov_base = buf;
    Iov(req)[0].iov_len  = len;
    req->bufCount = 1;
    req->flags    = flags;
    req->xferred  = 0;
    
    req->aux      = 0;
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode    = IORING_OP_RECV;
        sqe->fd        = static_cast<int>(s);
        sqe->addr      = reinterpret_cast<std::uint64_t>(buf);
        sqe->len       = len;
        sqe->msg_flags = flags;
    });
}

bool IoReactor::SubmitSend(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Send;
    req->handle = reinterpret_cast<void*>(s);
    Iov(req)[0].iov_base = const_cast<void*>(buf);
    Iov(req)[0].iov_len  = len;
    req->bufCount = 1;
    req->flags    = flags;
    req->xferred  = 0;
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode    = IORING_OP_SEND;
        sqe->fd        = static_cast<int>(s);
        sqe->addr      = reinterpret_cast<std::uint64_t>(buf);
        sqe->len       = len;
        sqe->msg_flags = flags;
    });
}

bool IoReactor::SubmitRecvV(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                            std::uint32_t flags, IoRequest* req, IoResult* out,
                            Task* resume, CancelToken token) {
    if (!ioplat::FillBufs(req, bufs, count)) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ioplat::kErrMsgSize };
        return true;
    }
    req->kind = IoRequest::Kind::Recv;
    req->bufCount = count;
    req->handle = reinterpret_cast<void*>(s);
    req->xferred = 0;
    req->aux     = 0;   
    (void)flags;   
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_READV;
        sqe->fd     = static_cast<int>(s);
        sqe->addr   = reinterpret_cast<std::uint64_t>(Iov(req));
        sqe->len    = count;
        sqe->off    = static_cast<std::uint64_t>(-1);   
    });
}

bool IoReactor::SubmitSendV(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                            std::uint32_t flags, IoRequest* req, IoResult* out,
                            Task* resume, CancelToken token) {
    if (!ioplat::FillBufs(req, bufs, count)) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ioplat::kErrMsgSize };
        return true;
    }
    req->kind = IoRequest::Kind::Send;
    req->bufCount = count;
    req->handle = reinterpret_cast<void*>(s);
    
    req->xferred = 0;
    (void)flags;
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_WRITEV;
        sqe->fd     = static_cast<int>(s);
        sqe->addr   = reinterpret_cast<std::uint64_t>(Iov(req));
        sqe->len    = count;
        sqe->off    = static_cast<std::uint64_t>(-1);
    });
}

bool IoReactor::SubmitRead(void* handle, void* buf, std::uint32_t len, std::uint64_t offset,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Generic;
    req->handle = handle;
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_READ;
        sqe->fd     = static_cast<int>(reinterpret_cast<std::uintptr_t>(handle));
        sqe->addr   = reinterpret_cast<std::uint64_t>(buf);
        sqe->len    = len;
        sqe->off    = offset;
    });
}

bool IoReactor::SubmitWrite(void* handle, const void* buf, std::uint32_t len, std::uint64_t offset,
                            IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Generic;
    req->handle = handle;
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_WRITE;
        sqe->fd     = static_cast<int>(reinterpret_cast<std::uintptr_t>(handle));
        sqe->addr   = reinterpret_cast<std::uint64_t>(buf);
        sqe->len    = len;
        sqe->off    = offset;
    });
}

bool IoReactor::SubmitRecvFrom(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                               IoAddress* from, IoRequest* req, IoResult* out,
                               Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Recv;
    req->handle = reinterpret_cast<void*>(s);

    req->aux = reinterpret_cast<std::uintptr_t>(from);

    struct iovec* v = Iov(req);
    v[0].iov_base = buf;
    v[0].iov_len  = len;

    auto* mh = reinterpret_cast<struct msghdr*>(req->native);
    std::memset(mh, 0, sizeof(*mh));
    mh->msg_name    = from ? from->bytes : nullptr;
    mh->msg_namelen = from ? static_cast<socklen_t>(IoAddress::kBytes) : 0;
    mh->msg_iov     = v;
    mh->msg_iovlen  = 1;

    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode    = IORING_OP_RECVMSG;
        sqe->fd        = static_cast<int>(s);
        sqe->addr      = reinterpret_cast<std::uint64_t>(mh);
        sqe->len       = 1;
        sqe->msg_flags = flags;
    });
}

bool IoReactor::SubmitSendTo(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                             const void* to, std::uint32_t toLen,
                             IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Send;
    req->handle = reinterpret_cast<void*>(s);
    req->xferred = 0;   

    struct iovec* v = Iov(req);
    v[0].iov_base = const_cast<void*>(buf);
    v[0].iov_len  = len;

    auto* mh = reinterpret_cast<struct msghdr*>(req->native);
    std::memset(mh, 0, sizeof(*mh));
    mh->msg_name    = const_cast<void*>(to);
    mh->msg_namelen = toLen;
    mh->msg_iov     = v;
    mh->msg_iovlen  = 1;

    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode    = IORING_OP_SENDMSG;
        sqe->fd        = static_cast<int>(s);
        sqe->addr      = reinterpret_cast<std::uint64_t>(mh);
        sqe->len       = 1;
        sqe->msg_flags = flags;
    });
}

bool IoReactor::SubmitAccept(IoSocket listener, IoSocket accepted, IoAcceptBuffer* addrs,
                             IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    
    if (accepted) ioplat::CloseSocket(accepted);

    req->kind = IoRequest::Kind::Accept;
    req->handle = reinterpret_cast<void*>(listener);
    req->aux = 0;

    auto* alen = reinterpret_cast<socklen_t*>(req->native + sizeof(struct msghdr));
    *alen = addrs ? static_cast<socklen_t>(IoAcceptBuffer::kBytes) : 0;

    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode       = IORING_OP_ACCEPT;
        sqe->fd           = static_cast<int>(listener);
        sqe->addr         = addrs ? reinterpret_cast<std::uint64_t>(addrs->bytes) : 0;
        sqe->off          = reinterpret_cast<std::uint64_t>(alen);   
        sqe->accept_flags = 0;
    });
}

bool IoReactor::SubmitConnect(IoSocket s, const void* sockaddr, std::uint32_t sockaddrLen,
                              IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Connect;
    req->handle = reinterpret_cast<void*>(s);
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_CONNECT;
        sqe->fd     = static_cast<int>(s);
        sqe->addr   = reinterpret_cast<std::uint64_t>(sockaddr);
        sqe->off    = sockaddrLen;      
    });
}

bool IoReactor::SupportsDisconnectReuse() noexcept { return false; }

bool IoReactor::SubmitDisconnect(IoSocket s, bool reuse,
                                 IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    if (reuse) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, EOPNOTSUPP };
        return true;
    }
    req->kind = IoRequest::Kind::Generic;
    req->handle = reinterpret_cast<void*>(s);
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_SHUTDOWN;
        sqe->fd     = static_cast<int>(s);
        sqe->len    = SHUT_RDWR;        
    });
}

bool IoReactor::SubmitPrepared(IoRequest* req) {
    if (!req) return true;
    const bool isSend = (req->kind == IoRequest::Kind::Send);
    const IoSocket s = reinterpret_cast<IoSocket>(req->handle);

    if (req->bufCount == 0) {
        if (req->out) *req->out = IoResult{ IoStatus::Failed, 0, EINVAL };
        return true;
    }

    if (req->bufCount == 1) {
        return SubmitOp(impl, req, req->out, req->resume, CancelToken(req->token),
                        [&](io_uring_sqe* sqe) {
            sqe->opcode    = isSend ? IORING_OP_SEND : IORING_OP_RECV;
            sqe->fd        = static_cast<int>(s);
            sqe->addr      = reinterpret_cast<std::uint64_t>(Iov(req)[0].iov_base);
            sqe->len       = static_cast<std::uint32_t>(Iov(req)[0].iov_len);
            sqe->msg_flags = req->flags;
        });
    }

    return SubmitOp(impl, req, req->out, req->resume, CancelToken(req->token),
                    [&](io_uring_sqe* sqe) {
        sqe->opcode = isSend ? IORING_OP_WRITEV : IORING_OP_READV;
        sqe->fd     = static_cast<int>(s);
        sqe->addr   = reinterpret_cast<std::uint64_t>(Iov(req));
        sqe->len    = req->bufCount;
        sqe->off    = static_cast<std::uint64_t>(-1);
    });
}

std::size_t IoReactor::RequestCancel(CancelToken token) noexcept {
    if (!impl->ringUp) return 0;
    
    const bool all = !token.Valid();
    std::size_t asked = 0;

    for (std::size_t i = 0; i < kShards; ++i) {
        
        IoRequest* targets[64];
        std::size_t n = 0;
        {
            std::lock_guard<std::mutex> lk(impl->shards[i].m);
            for (IoRequest* r = impl->shards[i].head; r && n < 64; r = r->next)
                if (all || CancelToken(r->token).IsWithin(token)) targets[n++] = r;
        }

        for (std::size_t k = 0; k < n; ++k) {
            std::lock_guard<std::mutex> lk(impl->submitMx);
            io_uring_sqe* sqe = uring::GetSqe(impl->ring);
            if (!sqe) break;              
            std::memset(sqe, 0, sizeof(*sqe));
            sqe->opcode = IORING_OP_ASYNC_CANCEL;
            sqe->fd     = -1;
            
            sqe->addr = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(targets[k]));
            
            sqe->user_data = kCancelSentinel;
            if (uring::Submit(impl->ring, 0) < 0) break;
            ++asked;
        }
    }
    return asked;
}

} 

#endif 
