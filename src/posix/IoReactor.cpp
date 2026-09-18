// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#if defined(__linux__)

#include "../../include/IoReactor.h"
#include "../../include/TaskScheduler.h"
#include "../IoPlatform.h"
#include "IoUring.h"

#include <sys/socket.h>
#include <sys/uio.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

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

    constexpr std::uint64_t kWakeSentinel = 1;

    const bool ioTrace = [] {
        const char* v = std::getenv("JLIB_IO_TRACE");
        return v && *v && *v != '0';
    }();
}

struct IoReactor::Impl {
    uring::Ring ring;
    bool        ringUp = false;

    std::mutex submitMx;

    struct Shard {
        mutable std::mutex m;
        IoRequest* head = nullptr;
        char pad[platform::kCacheLine];
    };
    Shard shards[kShards];
    std::atomic<std::size_t> total{ 0 };

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
        total.fetch_add(1, std::memory_order_relaxed);
    }

    void UnlinkLocked(IoRequest* r) noexcept {
        Shard& s = ShardFor(r);
        if (r->prev) r->prev->next = r->next;
        else if (s.head == r) s.head = r->next;
        if (r->next) r->next->prev = r->prev;
        r->prev = r->next = nullptr;
        total.fetch_sub(1, std::memory_order_relaxed);
    }

    void UnlinkUnlocked(IoRequest* r) noexcept {
        std::lock_guard<std::mutex> lk(ShardFor(r).m);
        UnlinkLocked(r);
    }

    std::mutex               life;
    std::atomic<bool>        stopping{ true };   
    bool                     running = false;
    std::vector<std::thread> workers;

    void EnsureThreads() {
        if (running) return;                       
        std::lock_guard<std::mutex> lk(life);
        if (running || !ringUp) return;
        stopping.store(false, std::memory_order_release);
        workers.emplace_back([this] { CompletionLoopEntry(this); });
        running = true;
    }

    static void CompletionLoopEntry(Impl* impl);
};

namespace {
    IoReactor::Impl* g_impl = nullptr;   
}

void IoReactor::Impl::CompletionLoopEntry(IoReactor::Impl* impl) {
    constexpr unsigned kCqBatch = 64;
    constexpr std::size_t kBatch = 64;

    io_uring_cqe cqes[kCqBatch];
    Task* batchHi[kBatch];
    Task* batchLo[kBatch];
    std::size_t nHi = 0, nLo = 0;

    auto flush = [&]() {
        if (!TaskScheduler::IsInitialized()) { nHi = nLo = 0; return; }
        auto& s = TaskScheduler::Instance();
        // Latency completions to the shared lane intake; normal ones, and any the intake refuses,
        // as a batch spread over the compute workers (never pinned to one slot).
        if (nHi) {
            if (!(TaskScheduler::GetHotWorkers() != 0 && s.LaneIntakeEnabled()
                  && TaskScheduler::PushLaneIntake(batchHi, nHi)))
                s.PushBatch(batchHi, nHi, TaskScheduler::kAnyWorker, 64);
            nHi = 0;
        }
        if (nLo) { s.PushBatch(batchLo, nLo, TaskScheduler::kAnyWorker, 64); nLo = 0; }
    };

    for (;;) {
        if (nHi == 0 && nLo == 0) {
            
            uring::WaitCq(impl->ring, 1);
        }

        const unsigned got = uring::Reap(impl->ring, cqes, kCqBatch);
        if (got == 0) {
            
            if (nHi || nLo) { flush(); continue; }
            if (impl->stopping.load(std::memory_order_acquire)) return;
            continue;
        }

        for (unsigned i = 0; i < got; ++i) {
            const io_uring_cqe& c = cqes[i];

            if (c.user_data == kWakeSentinel) {
                
                flush();
                if (impl->stopping.load(std::memory_order_acquire)) {
                    std::lock_guard<std::mutex> lk(impl->submitMx);
                    uring::PostWake(impl->ring, kWakeSentinel);
                    return;
                }
                continue;
            }

            IoRequest* r = reinterpret_cast<IoRequest*>(static_cast<std::uintptr_t>(c.user_data));
            if (!r) continue;

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
                    std::lock_guard<std::mutex> lk(impl->ShardFor(r).m);
                    impl->UnlinkLocked(r);
                }

                if (ioTrace) {
                    std::fprintf(stderr,
                        "[io] PARTIAL req=%p sent=%d total=%u remaining=%zu segs=%u -- resubmit\n",
                        (void*)r, (int)c.res, (unsigned)r->xferred,
                        ioplat::BufsRemaining(r), (unsigned)r->bufCount);
                    std::fflush(stderr);
                }

                if (!IoReactor::Instance().SubmitPrepared(r)) continue;
                res = r->out ? *r->out : res;
            }

            if (res.status == IoStatus::Completed && r->kind == IoRequest::Kind::Send)
                res.bytes += r->xferred;

            Task* resume = nullptr;
            {
                std::lock_guard<std::mutex> lk(impl->ShardFor(r).m);
                impl->UnlinkLocked(r);
                if (r->out) *r->out = res;    
                resume = r->resume;
            }

            if (r->onComplete) r->onComplete(r);

#if defined(JLIBSCHED_COROUTINES)
            // Coroutine I/O: resume is the suspended coroutine itself, not a fresh fiber job.
            if (resume && TaskScheduler::IsInitialized() && TaskScheduler::IsPinned(resume)) {
                TaskScheduler::Instance().WakeTask(resume);
                resume = nullptr;
            }
#endif
            if (resume) {
                if (IsLowLatency(resume->lane)) batchHi[nHi++] = resume;
                else                            batchLo[nLo++] = resume;
                if (nHi == kBatch || nLo == kBatch) flush();
            }
        }

        if ((nHi || nLo) && impl->stopping.load(std::memory_order_acquire)) flush();
    }
}

IoReactor::IoReactor() : impl(new Impl()) {
    g_impl = impl;
    
    if (uring::Init(impl->ring, 256) == uring::InitResult::Ok) impl->ringUp = true;
}

IoReactor::~IoReactor() {
    Stop();
    if (impl->ringUp) uring::Shutdown(impl->ring);
    delete impl;
    g_impl = nullptr;
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

void IoReactor::Start() noexcept {
    if (!impl->ringUp) return;

    impl->EnsureThreads();
}

void IoReactor::Stop() noexcept {
    bool needJoin = false;
    {
        std::lock_guard<std::mutex> lk(impl->life);
        if (impl->stopping.load(std::memory_order_acquire)) return;
        impl->stopping.store(true, std::memory_order_release);
        needJoin = impl->running;
    }

    RequestCancel(CancelToken{});

    if (needJoin) {
        std::vector<std::thread> ts;
        { std::lock_guard<std::mutex> lk(impl->life); ts.swap(impl->workers); impl->running = false; }
        for (std::size_t i = 0; i < ts.size(); ++i) {
            std::lock_guard<std::mutex> lk(impl->submitMx);
            uring::PostWake(impl->ring, kWakeSentinel);
        }
        for (auto& t : ts) if (t.joinable()) t.join();
    }
}

std::size_t IoReactor::InFlight() const noexcept {
    return impl->total.load(std::memory_order_relaxed);
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

    if (resume && resume->type == TaskType::Fiber && resume->stackClass == StackClass::Standard)
        resume->stackClass = StackClass::Tiny;

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

    impl->EnsureThreads();
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
            
            sqe->user_data = kWakeSentinel;
            if (uring::Submit(impl->ring, 0) < 0) break;
            ++asked;
        }
    }
    return asked;
}

} 

#endif 
