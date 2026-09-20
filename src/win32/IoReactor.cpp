// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../../include/IoReactor.h"
#include "../../include/TaskScheduler.h"
#include "../../include/platform.h"      
#include "../../include/Timer.h"         
#include "../IoPlatform.h"                

#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#pragma comment(lib, "ws2_32.lib")

#include <cstddef>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>
#include <chrono>

namespace JLib {

#if defined(JLIBSCHED_IO_LOCK_STATS)
    namespace detail {
        
        std::atomic<unsigned> g_complCore[64];
    }
#endif

    static_assert(sizeof(OVERLAPPED) <= IoRequest::kNativeBytes,
                  "IoRequest::kNativeBytes is too small for OVERLAPPED");
    static_assert(alignof(OVERLAPPED) <= 16,
                  "IoRequest::native is not aligned enough for OVERLAPPED");

    static_assert(IoAcceptBuffer::kBytes >= 2 * (sizeof(sockaddr_in6) + 16),
                  "IoAcceptBuffer is too small for AcceptEx: it needs sizeof(sockaddr)+16 per address");

    static_assert(IoAddress::kBytes >= sizeof(sockaddr_storage),
                  "IoAddress is too small for sockaddr_storage");
    static_assert(sizeof(OVERLAPPED) + IoRequest::kMaxVectors * sizeof(WSABUF)
                      <= IoRequest::kNativeBytes,
                  "IoRequest::kNativeBytes cannot hold OVERLAPPED plus kMaxVectors WSABUFs");

#if defined(JLIBSCHED_IO_LOCK_STATS)
    static std::atomic<std::uint64_t> g_ioAcquires{ 0 };
    static std::atomic<std::uint64_t> g_ioContended{ 0 };

    struct CountingMutex {
        std::mutex m;
        void lock() {
            if (!m.try_lock()) {
                g_ioContended.fetch_add(1, std::memory_order_relaxed);
                m.lock();
            }
            g_ioAcquires.fetch_add(1, std::memory_order_relaxed);
        }
        void unlock() { m.unlock(); }
    };
    using IoMutex = CountingMutex;
#else
    using IoMutex = std::mutex;
#endif

    IoLockStats ReadIoLockStats() noexcept {
#if defined(JLIBSCHED_IO_LOCK_STATS)
        return IoLockStats{ g_ioAcquires.load(std::memory_order_relaxed),
                            g_ioContended.load(std::memory_order_relaxed) };
#else
        return IoLockStats{};   
#endif
    }

    void ResetIoLockStats() noexcept {
#if defined(JLIBSCHED_IO_LOCK_STATS)
        g_ioAcquires.store(0, std::memory_order_relaxed);
        g_ioContended.store(0, std::memory_order_relaxed);
#endif
    }

    static OVERLAPPED* Ov(IoRequest* r) { return reinterpret_cast<OVERLAPPED*>(r->native); }

    static WSABUF* Bufs(IoRequest* r) {
        return reinterpret_cast<WSABUF*>(r->native + sizeof(OVERLAPPED));
    }

    static bool FillBufs(IoRequest* r, const IoBuffer* bufs, std::uint32_t count) {
        if (count == 0 || count > IoRequest::kMaxVectors) return false;
        WSABUF* w = Bufs(r);
        for (std::uint32_t i = 0; i < count; ++i) {
            w[i].len = static_cast<ULONG>(bufs[i].len);
            w[i].buf = static_cast<CHAR*>(bufs[i].data);
        }
        return true;
    }

    namespace ioplat {
        bool FillBufs(IoRequest* r, const IoBuffer* bufs, std::uint32_t count) noexcept {
            return ::JLib::FillBufs(r, bufs, count);
        }
    }

    static LPFN_ACCEPTEX      g_acceptEx  = nullptr;
    static LPFN_CONNECTEX     g_connectEx = nullptr;
    static LPFN_DISCONNECTEX  g_disconnectEx = nullptr;
    static std::mutex         g_extMutex;

    static bool ResolveExtensions() {
        std::lock_guard<std::mutex> lk(g_extMutex);
        if (g_acceptEx && g_connectEx) return true;

        SOCKET probe = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (probe == INVALID_SOCKET) return false;   

        GUID gAccept  = WSAID_ACCEPTEX;
        GUID gConnect = WSAID_CONNECTEX;
        DWORD got = 0;
        bool ok = ::WSAIoctl(probe, SIO_GET_EXTENSION_FUNCTION_POINTER, &gAccept, sizeof gAccept,
                             &g_acceptEx, sizeof g_acceptEx, &got, nullptr, nullptr) == 0;
        ok = ok && ::WSAIoctl(probe, SIO_GET_EXTENSION_FUNCTION_POINTER, &gConnect, sizeof gConnect,
                              &g_connectEx, sizeof g_connectEx, &got, nullptr, nullptr) == 0;

        GUID gDisc = WSAID_DISCONNECTEX;
        ::WSAIoctl(probe, SIO_GET_EXTENSION_FUNCTION_POINTER, &gDisc, sizeof gDisc,
                   &g_disconnectEx, sizeof g_disconnectEx, &got, nullptr, nullptr);
        ::closesocket(probe);
        return ok && g_acceptEx && g_connectEx;
    }

    struct IoReactor::Impl {
        HANDLE port = nullptr;

        static constexpr std::size_t kShards = 16;

        struct Shard {
            mutable IoMutex m;
            IoRequest* head = nullptr;      
            
            char pad[platform::kCacheLine];
        };

        Shard shards[kShards];
        std::atomic<std::size_t> total{ 0 };     

        Shard& ShardFor(const IoRequest* r) noexcept {
            const std::uintptr_t x = reinterpret_cast<std::uintptr_t>(r) >> 4;
            return shards[(x * 2654435761u) % kShards];
        }

        std::mutex life;                          
        std::vector<std::thread> workers;
        bool running = false;
        std::atomic<bool> stopping{ false };      

        void Link(IoRequest* r) {
            Shard& s = ShardFor(r);
            r->prev = nullptr;
            r->next = s.head;
            if (s.head) s.head->prev = r;
            s.head = r;
            total.fetch_add(1, std::memory_order_relaxed);
        }

        void Unlink(IoRequest* r) {
            Shard& s = ShardFor(r);
            if (r->prev) r->prev->next = r->next;
            else if (s.head == r) s.head = r->next;
            if (r->next) r->next->prev = r->prev;
            r->prev = r->next = nullptr;
            total.fetch_sub(1, std::memory_order_relaxed);
        }

        void EnsureThreads() {
            std::lock_guard<std::mutex> lk(life);
            if (running) return;
            running = true;
            const unsigned n = TaskScheduler::IoCompletionThreads();
            workers.reserve(n);
            for (unsigned i = 0; i < n; ++i) workers.emplace_back([this] { Run(); });
        }

        static IoResult Classify(BOOL ok, DWORD err, DWORD bytes) {
            IoResult res;
            if (ok) {
                res.status = IoStatus::Completed;
                res.bytes  = static_cast<std::uint32_t>(bytes);
            } else if (err == ERROR_OPERATION_ABORTED) {
                
                res.status = IoStatus::Cancelled;
            } else if (err == ERROR_HANDLE_EOF || err == ERROR_BROKEN_PIPE) {
                
                res.status = IoStatus::Completed;
                res.bytes  = 0;
            } else {
                res.status = IoStatus::Failed;
                res.error  = static_cast<std::int32_t>(err);
            }
            return res;
        }

        static constexpr std::size_t kBatch = 32;

        void Run() {
            
            Task* batchHi[kBatch]; std::size_t nHi = 0;
            Task* batchLo[kBatch]; std::size_t nLo = 0;
#if defined(JLIBSCHED_IO_LOCK_STATS)
            IoResult* outHi[kBatch]; IoResult* outLo[kBatch];
#endif

            const auto Flush = [&](Task** hi, std::size_t& nh, Task** lo, std::size_t& nl) {
                if (!TaskScheduler::IsInitialized()) { nh = 0; nl = 0; return; }
#if defined(JLIBSCHED_IO_LOCK_STATS)
                
                const std::int64_t now = MonotonicNs();
                for (std::size_t i = 0; i < nh; ++i) if (outHi[i]) outHi[i]->flushedAtNs = now;
                for (std::size_t i = 0; i < nl; ++i) if (outLo[i]) outLo[i]->flushedAtNs = now;
#endif
                
                const std::size_t hotN = TaskScheduler::GetHotWorkers();
                // Latency completions go to the shared lane intake. Normal ones, and latency ones
                // the intake refuses (off, or no K), are a batch for the compute workers.
                auto pushSteered = [&](Task** arr, std::size_t n, Lane lane) {
                    if (!n) return;
                    auto& s = TaskScheduler::Instance();

                    if (hotN != 0 && IsLowLatency(lane) && s.LaneIntakeEnabled()) {
                        if (TaskScheduler::PushLaneIntake(arr, n)) {
                            detail::g_ioToLane.fetch_add(n, std::memory_order_relaxed);
                            return;
                        }
                        detail::g_ioFloorFallback.fetch_add(n, std::memory_order_relaxed);
                    }
                    detail::g_ioToFloor.fetch_add(n, std::memory_order_relaxed);
                    // Each completion to a compute worker's hi-pri inbox, round-robin with a wake.
                    // That inbox is checked every pass ahead of the worker's own deque, so a busy
                    // worker takes it at its next task boundary. A normal inbox is drained only
                    // when the deque runs dry or on the fairness tick: under load a completion
                    // placed there waited behind the worker's own successors (measured ~3 ms vs
                    // ~100 us here, 200 us tasks).
                    for (std::size_t i = 0; i < n; ++i) s.PushTo(arr[i], CorePref::Any, true);
                };
                pushSteered(hi, nh, Lane::LowLatency);  nh = 0;
                pushSteered(lo, nl, Lane::Normal); nl = 0;
            };

            for (;;) {
                DWORD bytes = 0;
                ULONG_PTR key = 0;
                LPOVERLAPPED ov = nullptr;

                const BOOL ok = (nHi == 0 && nLo == 0)
                    ? GetQueuedCompletionStatus(port, &bytes, &key, &ov, INFINITE)
                    : GetQueuedCompletionStatus(port, &bytes, &key, &ov, 0);

#if defined(JLIBSCHED_IO_LOCK_STATS)
                
                if (const DWORD cpu = ::GetCurrentProcessorNumber(); cpu < 64)
                    detail::g_complCore[cpu].fetch_add(1, std::memory_order_relaxed);
#endif

                if ((nHi || nLo) && ov == nullptr && !ok) {
                    Flush(batchHi, nHi, batchLo, nLo);
                    continue;
                }

                if (ov == nullptr) {
                    
                    if (!ok) {
                        if (::GetLastError() == WAIT_TIMEOUT) continue;
                        return;
                    }
                    if (stopping.load(std::memory_order_acquire) && total.load(std::memory_order_acquire) == 0) {
                        
                        ::PostQueuedCompletionStatus(port, 0, 0, nullptr);
                        return;
                    }
                    continue;
                }

                IoRequest* r = reinterpret_cast<IoRequest*>(
                    reinterpret_cast<unsigned char*>(ov) - offsetof(IoRequest, native));
                const IoResult res = Classify(ok, ok ? ERROR_SUCCESS : ::GetLastError(), bytes);

                if (res.status == IoStatus::Completed && r->kind != IoRequest::Kind::Generic) {
                    if (r->kind == IoRequest::Kind::Accept) {
                        
                        const SOCKET listener = reinterpret_cast<SOCKET>(r->handle);
                        const SOCKET accepted = static_cast<SOCKET>(r->aux);
                        ::setsockopt(accepted, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                                     reinterpret_cast<const char*>(&listener), sizeof listener);
                    } else {
                        const SOCKET s = reinterpret_cast<SOCKET>(r->handle);
                        ::setsockopt(s, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0);
                    }
                }

                Task* resume = nullptr;
                bool  lastOne = false;
                {
                    std::lock_guard<IoMutex> lk(ShardFor(r).m);
                    Unlink(r);                     
                    if (r->out) *r->out = res;     
                    resume = r->resume;
                    lastOne = false;
                }

#if defined(JLIBSCHED_IO_LOCK_STATS)
                
                if (r->out) r->out->completedAtNs = MonotonicNs();
#endif

                if (r->onComplete) r->onComplete(r);

#if defined(JLIBSCHED_COROUTINES)
                // Coroutine I/O: resume is the suspended coroutine itself, not a fresh fiber job.
                if (resume && TaskScheduler::IsInitialized() && TaskScheduler::IsPinned(resume)) {
                    TaskScheduler::Instance().WakeTask(resume);
                    resume = nullptr;
                }
#endif
                if (resume) {
#if defined(JLIBSCHED_IO_LOCK_STATS)
                    if (IsLowLatency(resume->lane)) outHi[nHi] = r->out; else outLo[nLo] = r->out;
#endif
                    if (IsLowLatency(resume->lane)) batchHi[nHi++] = resume;
                    else               batchLo[nLo++] = resume;
                    if (nHi == kBatch || nLo == kBatch) Flush(batchHi, nHi, batchLo, nLo);
                }

                if (lastOne || ((nHi || nLo) && stopping.load(std::memory_order_acquire)))
                    Flush(batchHi, nHi, batchLo, nLo);
                if (lastOne) {
                    ::PostQueuedCompletionStatus(port, 0, 0, nullptr);
                    return;
                }
            }
        }

        template <typename Call>
        bool Submit(HANDLE h, std::uint64_t offset, IoRequest* req, IoResult* out,
                    Task* resume, CancelToken token, Call&& call) {
            const std::uint32_t tok = token.Raw();

            if (CancelToken(tok).Cancelled()) {
                if (out) *out = IoResult{ IoStatus::Cancelled, 0, 0 };
                return true;
            }

            *Ov(req) = OVERLAPPED{};
            Ov(req)->Offset     = static_cast<DWORD>(offset & 0xFFFFFFFFull);
            Ov(req)->OffsetHigh = static_cast<DWORD>(offset >> 32);
            req->out    = out;
            req->resume = resume;
            req->token  = tok;
            req->handle = h;

            if (resume && resume->type == TaskType::Fiber && resume->stackClass == StackClass::Standard)
                resume->stackClass = StackClass::Tiny;

            {
                if (stopping.load(std::memory_order_acquire)) {
                    if (out) *out = IoResult{ IoStatus::Failed, 0, ERROR_SHUTDOWN_IN_PROGRESS };
                    return true;
                }
                EnsureThreads();
                std::lock_guard<IoMutex> lk(ShardFor(req).m);
                
                Link(req);
            }

            if (!call(Ov(req))) {
                const DWORD err = ::GetLastError();
                if (err != ERROR_IO_PENDING) {
                    
                    std::lock_guard<IoMutex> lk(ShardFor(req).m);
                    Unlink(req);
                    if (out) *out = IoResult{ IoStatus::Failed, 0, static_cast<std::int32_t>(err) };
                    return true;
                }
            }

            return false;
        }
    };

    IoReactor::IoReactor() : impl(new Impl()) {
        
        impl->port = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0,
                                              TaskScheduler::IoCompletionThreads());
    }

    IoReactor::~IoReactor() {
        Stop();
        if (impl->port) ::CloseHandle(impl->port);
        delete impl;
    }

    IoReactor& IoReactor::Instance() {
        
        static IoReactor* r = new IoReactor();
        return *r;
    }

    bool IoReactor::IsAvailable() noexcept { return true; }

    static bool IoLayerUsable() {
        if (!TaskScheduler::IsInitialized() || TaskScheduler::IoReactorEnabled()) return true;
        std::fprintf(stderr,
            "[JLib::Scheduler] IoReactor used but the I/O layer is not enabled -- the pool was sized "
            "without a core for its completion thread. Set Config::io = true at Init.\n");
        return false;
    }

    bool IoReactor::Register(void* handle) {
        if (!IoLayerUsable()) return false;
        if (!handle || handle == INVALID_HANDLE_VALUE || !impl->port) return false;
        {
            if (impl->stopping.load(std::memory_order_acquire)) return false;
            impl->EnsureThreads();
        }
        
        return ::CreateIoCompletionPort(static_cast<HANDLE>(handle), impl->port, 0, 0) != nullptr;
    }

    bool IoReactor::SubmitRead(void* handle, void* buf, std::uint32_t len, std::uint64_t offset,
                               IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        HANDLE h = static_cast<HANDLE>(handle);
        return impl->Submit(h, offset, req, out, resume, token, [&](OVERLAPPED* ov) {
            return ::ReadFile(h, buf, len, nullptr, ov) != FALSE;
        });
    }

    bool IoReactor::SubmitWrite(void* handle, const void* buf, std::uint32_t len,
                                std::uint64_t offset, IoRequest* req, IoResult* out,
                                Task* resume, CancelToken token) {
        HANDLE h = static_cast<HANDLE>(handle);
        return impl->Submit(h, offset, req, out, resume, token, [&](OVERLAPPED* ov) {
            return ::WriteFile(h, buf, len, nullptr, ov) != FALSE;
        });
    }

    bool IoReactor::InitSockets() { return IoLayerUsable() && ResolveExtensions(); }

    bool IoReactor::RegisterSocket(IoSocket s) {
        
        return Register(reinterpret_cast<void*>(static_cast<SOCKET>(s)));
    }

    bool IoReactor::SubmitRecv(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                               IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        const SOCKET sock = static_cast<SOCKET>(s);
        HANDLE h = reinterpret_cast<HANDLE>(sock);
        const IoBuffer one{ buf, len };
        return SubmitRecvV(s, &one, 1, flags, req, out, resume, token);
    }

    bool IoReactor::SubmitSend(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                               IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        const SOCKET sock = static_cast<SOCKET>(s);
        HANDLE h = reinterpret_cast<HANDLE>(sock);
        const IoBuffer one{ const_cast<void*>(buf), len };
        return SubmitSendV(s, &one, 1, flags, req, out, resume, token);
    }

    bool IoReactor::SubmitRecvV(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                                std::uint32_t flags, IoRequest* req, IoResult* out,
                                Task* resume, CancelToken token) {
        if (!FillBufs(req, bufs, count)) {
            
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEMSGSIZE };
            return true;
        }
        const SOCKET sock = static_cast<SOCKET>(s);
        req->flags = flags;
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            
            return ::WSARecv(sock, Bufs(req), count, nullptr,
                             reinterpret_cast<LPDWORD>(&req->flags), ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitSendV(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                                std::uint32_t flags, IoRequest* req, IoResult* out,
                                Task* resume, CancelToken token) {
        if (!FillBufs(req, bufs, count)) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEMSGSIZE };
            return true;
        }
        const SOCKET sock = static_cast<SOCKET>(s);
        req->flags = flags;
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            return ::WSASend(sock, Bufs(req), count, nullptr,
                             static_cast<DWORD>(flags), ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitRecvFrom(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                                   IoAddress* from, IoRequest* req, IoResult* out,
                                   Task* resume, CancelToken token) {
        const IoBuffer one{ buf, len };
        if (!FillBufs(req, &one, 1) || !from) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEINVAL };
            return true;
        }
        const SOCKET sock = static_cast<SOCKET>(s);
        req->flags = flags;
        from->len = static_cast<std::int32_t>(IoAddress::kBytes);   

        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            
            return ::WSARecvFrom(sock, Bufs(req), 1, nullptr,
                                 reinterpret_cast<LPDWORD>(&req->flags),
                                 reinterpret_cast<sockaddr*>(from->bytes),
                                 reinterpret_cast<LPINT>(&from->len), ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitSendTo(IoSocket s, const void* buf, std::uint32_t len,
                                 std::uint32_t flags, const void* addr, std::uint32_t addrLen,
                                 IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        const IoBuffer one{ const_cast<void*>(buf), len };
        if (!FillBufs(req, &one, 1) || !addr) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEINVAL };
            return true;
        }
        const SOCKET sock = static_cast<SOCKET>(s);
        req->flags = flags;
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            
            return ::WSASendTo(sock, Bufs(req), 1, nullptr, static_cast<DWORD>(flags),
                               static_cast<const sockaddr*>(addr), static_cast<int>(addrLen),
                               ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitAccept(IoSocket listener, IoSocket accepted, IoAcceptBuffer* addrs,
                                 IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        if (!g_acceptEx && !ResolveExtensions()) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSANOTINITIALISED };
            return true;
        }

        const SOCKET lis = static_cast<SOCKET>(listener);
        const SOCKET acc = static_cast<SOCKET>(accepted);

        req->kind = IoRequest::Kind::Accept;
        req->aux  = static_cast<std::uintptr_t>(acc);

        return impl->Submit(reinterpret_cast<HANDLE>(lis), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            
            constexpr DWORD kAddrLen = sizeof(sockaddr_in6) + 16;
            DWORD got = 0;
            return g_acceptEx(lis, acc, addrs->bytes, 0, kAddrLen, kAddrLen, &got, ov) != FALSE;
        });
    }

    bool IoReactor::SubmitConnect(IoSocket s, const void* addr, std::uint32_t addrLen,
                                  IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        if (!g_connectEx && !ResolveExtensions()) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSANOTINITIALISED };
            return true;
        }

        const SOCKET sock = static_cast<SOCKET>(s);
        req->kind = IoRequest::Kind::Connect;

        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            DWORD sent = 0;
            return g_connectEx(sock, static_cast<const struct sockaddr*>(addr),
                               static_cast<int>(addrLen), nullptr, 0, &sent, ov) != FALSE;
        });
    }

    bool IoReactor::SupportsDisconnectReuse() noexcept { return true; }

    bool IoReactor::SubmitDisconnect(IoSocket s, bool reuse, IoRequest* req, IoResult* out,
                                     Task* resume, CancelToken token) {
        if (!g_disconnectEx && !ResolveExtensions()) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSANOTINITIALISED };
            return true;
        }
        if (!g_disconnectEx) {
            
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEOPNOTSUPP };
            return true;
        }

        const SOCKET sock = static_cast<SOCKET>(s);
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            return g_disconnectEx(sock, ov, reuse ? TF_REUSE_SOCKET : 0, 0) != FALSE;
        });
    }

    bool IoReactor::SubmitPrepared(IoRequest* req) {
        const SOCKET sock = reinterpret_cast<SOCKET>(req->handle);

        const CancelToken tok(req->token);

        if (req->kind == IoRequest::Kind::Send) {
            return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, req->out, req->resume, tok,
                                [&](OVERLAPPED* ov) {
                return ::WSASend(sock, Bufs(req), req->bufCount, nullptr,
                                 static_cast<DWORD>(req->flags), ov, nullptr) == 0;
            });
        }
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, req->out, req->resume, tok,
                            [&](OVERLAPPED* ov) {
            return ::WSARecv(sock, Bufs(req), req->bufCount, nullptr,
                             reinterpret_cast<LPDWORD>(&req->flags), ov, nullptr) == 0;
        });
    }

    std::size_t IoReactor::RequestCancel(CancelToken token) noexcept {
        
        std::size_t asked = 0;
        for (std::size_t i = 0; i < Impl::kShards; ++i) {
            std::lock_guard<IoMutex> lk(impl->shards[i].m);
            for (IoRequest* r = impl->shards[i].head; r; r = r->next) {
                if (token.Valid() && !CancelToken(r->token).IsWithin(token)) continue;
                ::CancelIoEx(r->handle, Ov(r));
                ++asked;
            }
        }
        return asked;
    }

    std::size_t IoReactor::CompletionCoreHistogram(unsigned* counts, std::size_t max) noexcept {
        if (!counts || max == 0) return 0;
        const std::size_t n = max < 64 ? max : 64;
#if defined(JLIBSCHED_IO_LOCK_STATS)
        for (std::size_t i = 0; i < n; ++i)
            counts[i] = detail::g_complCore[i].load(std::memory_order_relaxed);
#else
        for (std::size_t i = 0; i < n; ++i) counts[i] = 0;
#endif
        return n;
    }

    std::size_t IoReactor::InFlight() const noexcept {
        return impl->total.load(std::memory_order_acquire);
    }

    void IoReactor::Start() noexcept {
        std::lock_guard<std::mutex> lk(impl->life);
        impl->stopping.store(false, std::memory_order_release);
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
            { std::lock_guard<std::mutex> lk(impl->life); ts.swap(impl->workers); }
            
            for (std::size_t i = 0; i < ts.size(); ++i)
                ::PostQueuedCompletionStatus(impl->port, 0, 0, nullptr);
            for (auto& t : ts) if (t.joinable()) t.join();
        }
        impl->running = false;
    }

} 
