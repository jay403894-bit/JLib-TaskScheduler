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

        Shard& ShardFor(const IoRequest* r) noexcept {
            const std::uintptr_t x = reinterpret_cast<std::uintptr_t>(r) >> 4;
            return shards[(x * 2654435761u) % kShards];
        }

        std::mutex life;
        // Starts TRUE: until Start() there is no pump, so a submit must be refused
        // (ERROR_SHUTDOWN_IN_PROGRESS) rather than accepted into a port nobody reads.
        std::atomic<bool> stopping{ true };

        // THE PUMP: a plain thread, not a pool worker, and the port's ONLY reader. It sleeps in
        // GetQueuedCompletionStatusEx, moves every completion into the injector (no wake -- every
        // worker takes from it each pass, and the last hunter never parks), and goes straight back.
        // It runs nothing itself, so it is never away from the port while completions arrive.
        // Workers never touch the port: here every look is a kernel call. See kport_model.c.
        //
        // Not K: a pump needs no deque, inbox, hunt state or reserved slot, and counting it as K
        // would cost a compute worker for a thread that is asleep nearly always.
        std::thread pump;

        // Key 0 is every registered handle; a packet with this key and no OVERLAPPED is the stop
        // marker Stop() posts to end the pump.
        static constexpr ULONG_PTR    kWakeKey   = 1;
        static constexpr ULONG        kBatch     = 64;

        void Link(IoRequest* r) {
            Shard& s = ShardFor(r);
            r->prev = nullptr;
            r->next = s.head;
            if (s.head) s.head->prev = r;
            s.head = r;
            detail::g_ioOutstanding.fetch_add(1, std::memory_order_relaxed);
        }

        void Unlink(IoRequest* r) {
            Shard& s = ShardFor(r);
            if (r->prev) r->prev->next = r->next;
            else if (s.head == r) s.head = r->next;
            if (r->next) r->next->prev = r->prev;
            r->prev = r->next = nullptr;
            detail::g_ioOutstanding.fetch_sub(1, std::memory_order_relaxed);
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

        // One dequeued completion: publish the result and return the resume to run, or null if
        // there is none or it was handed on (a pinned resume goes where its pin says).
        Task* Complete(const OVERLAPPED_ENTRY& e) {
            OVERLAPPED* ov = e.lpOverlapped;
            IoRequest* r = reinterpret_cast<IoRequest*>(
                reinterpret_cast<unsigned char*>(ov) - offsetof(IoRequest, native));

#if defined(JLIBSCHED_IO_LOCK_STATS)
            if (const DWORD cpu = ::GetCurrentProcessorNumber(); cpu < 64)
                detail::g_complCore[cpu].fetch_add(1, std::memory_order_relaxed);
#endif
            // GetQueuedCompletionStatusEx reports no per-entry error; the status is in the
            // OVERLAPPED, and GetOverlappedResult (no wait) maps it the same way the single-packet
            // GetQueuedCompletionStatus did.
            DWORD bytes = e.dwNumberOfBytesTransferred;
            const BOOL ok = ::GetOverlappedResult(static_cast<HANDLE>(r->handle), ov, &bytes, FALSE);
            const IoResult res = Classify(ok, ok ? ERROR_SUCCESS : ::GetLastError(), bytes);

            if (res.status == IoStatus::Completed && r->kind != IoRequest::Kind::Generic) {
                if (r->kind == IoRequest::Kind::Accept) {

                    const SOCKET listener = reinterpret_cast<SOCKET>(r->handle);
                    const SOCKET accepted = static_cast<SOCKET>(r->aux);
                    ::setsockopt(accepted, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                                 reinterpret_cast<const char*>(&listener), sizeof listener);
                } else if (r->kind == IoRequest::Kind::Connect) {
                    // Only ConnectEx needs it. Send/Recv (IoStream chains) are non-Generic too,
                    // and paid a wasted setsockopt on every completion.
                    const SOCKET s = reinterpret_cast<SOCKET>(r->handle);
                    ::setsockopt(s, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0);
                }
            }

            Task* resume = nullptr;
            {
                std::lock_guard<IoMutex> lk(ShardFor(r).m);
                Unlink(r);
                if (r->out) *r->out = res;
                resume = r->resume;
            }

#if defined(JLIBSCHED_IO_LOCK_STATS)
            if (r->out) r->out->completedAtNs = MonotonicNs();
#endif

            if (r->onComplete) r->onComplete(r);   // may resubmit or free r: not touched after

            if (resume && TaskScheduler::IsInitialized() && TaskScheduler::IsPinned(resume)) {
                TaskScheduler::Instance().WakeTask(resume);
                return nullptr;
            }
            return resume;
        }

        // A batch from the port, ALL of it to the injector in one bulk push (no wake: every worker
        // reads the injector each pass). Pinned resumes never get here -- Complete() routes them
        // through WakeTask. Returns the number of stop markers seen.
        unsigned Dispatch(const OVERLAPPED_ENTRY* es, ULONG n) {
            Task* ts[kBatch];
            std::size_t nt = 0;
            unsigned markers = 0;
            for (ULONG i = 0; i < n; ++i) {
                if (!es[i].lpOverlapped) {
                    if (es[i].lpCompletionKey == kWakeKey) ++markers;
                    continue;
                }
                if (Task* t = Complete(es[i])) ts[nt++] = t;
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

        // The pump's whole life: sleep in the port, inject, repeat -- until Stop() posts a marker.
        // Above the compute workers, as the clock is: a descheduled pump delays every completion,
        // and it is asleep nearly always, so it costs nothing to run first when it does wake.
        void Pump() {
            ::SetThreadPriority(::GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
            for (;;) {
                OVERLAPPED_ENTRY es[kBatch];
                ULONG n = 0;
                if (!::GetQueuedCompletionStatusEx(port, es, kBatch, &n, INFINITE, FALSE)) {
                    if (stopping.load(std::memory_order_acquire)) return;   // never spin on a dead port
                    continue;
                }
                if (Dispatch(es, n) != 0 && stopping.load(std::memory_order_acquire)) return;
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

            {
                if (stopping.load(std::memory_order_acquire)) {
                    if (out) *out = IoResult{ IoStatus::Failed, 0, ERROR_SHUTDOWN_IN_PROGRESS };
                    return true;
                }
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
        // Only the pump ever waits on this port, so the concurrency value never binds. It is set
        // far above any pool so it can never be what holds the pump back with packets queued.
        constexpr DWORD kNoThrottle = 4096;
        impl->port = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, kNoThrottle);
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

    // I/O is opt-in: until Start() there is no pump, so nothing may be registered or set up. Say
    // so, instead of failing quietly. "Not started" is exactly `stopping` (it starts true).
    static bool IoStarted(const IoReactor::Impl* impl) {
        if (!impl->stopping.load(std::memory_order_acquire)) return true;
        std::fprintf(stderr,
            "[JLib::Scheduler] IoReactor used before IoReactor::Instance().Start() -- I/O is "
            "opt-in; start it after TaskScheduler::Init.\n");
        return false;
    }

    bool IoReactor::Register(void* handle) {
        if (!IoStarted(impl)) return false;
        if (!handle || handle == INVALID_HANDLE_VALUE || !impl->port) return false;
        if (impl->stopping.load(std::memory_order_acquire)) return false;
        
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

    bool IoReactor::InitSockets() { return IoStarted(impl) && ResolveExtensions(); }

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
        return detail::g_ioOutstanding.load(std::memory_order_acquire);
    }

    namespace {
        void StartIoHook() { IoReactor::Instance().Start(); }
        void StopIoHook()  { IoReactor::Instance().Stop(); }
    }

    // THE OPT-IN. Nothing I/O exists until this runs: it starts the pump and registers the stop
    // hook Join calls. Before or after Init: before, it only leaves a start hook and Init starts
    // the pump once the pool can take completions (see detail::g_ioStartHook).
    void IoReactor::Start() noexcept {
        if (!TaskScheduler::IsInitialized()) {
            detail::g_ioStartHook.store(&StartIoHook, std::memory_order_release);
            return;
        }
        std::lock_guard<std::mutex> lk(impl->life);
        impl->stopping.store(false, std::memory_order_release);
        if (!impl->pump.joinable()) impl->pump = std::thread([p = impl] { p->Pump(); });
        detail::g_ioStopHook.store(&StopIoHook, std::memory_order_release);
    }

    // Called from Join (through the hook) while the pool still runs. Cancels everything in flight
    // and lets the pump drain the port -- it stays the port's only reader to the end -- so every
    // resume is handed to the pool before it stops. A request that ignores its cancel would hold
    // this forever, so the wait gives up after a bounded spell with no progress. Then the stop
    // marker ends the pump.
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
            ::Sleep(10);
            const std::size_t now = detail::g_ioOutstanding.load(std::memory_order_acquire);
            quietMs = (now == last) ? quietMs + 10 : 0;   // quiet = no completion in the step
            last = now;
        }

        // `stopping` is already set, so the pump returns on this marker (and never before it:
        // nothing else posts one).
        if (impl->pump.joinable()) {
            ::PostQueuedCompletionStatus(impl->port, 0, Impl::kWakeKey, nullptr);
            impl->pump.join();
        }
        if (const std::size_t left = detail::g_ioOutstanding.load(std::memory_order_acquire)) {
            std::fprintf(stderr, "[JLib::Scheduler] I/O stop: %zu request(s) still outstanding after "
                                 "cancel; their resumes will not run.\n", left);
        }
    }

}
