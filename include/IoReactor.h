// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include "CancelToken.h"
#include "TaskScheduler.h"   

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace JLib {

    struct Task;

    enum class IoStatus : std::uint8_t {
        Pending,      
        Completed,    
        Cancelled,    
        Failed        
    };

    struct IoResult {
        IoStatus      status = IoStatus::Pending;
        std::uint32_t bytes  = 0;
        std::int32_t  error  = 0;      

        std::int64_t  completedAtNs = 0;

        std::int64_t  flushedAtNs = 0;

        bool Ok() const noexcept { return status == IoStatus::Completed; }
    };

    struct IoBuffer {
        void*         data = nullptr;
        std::uint32_t len  = 0;
    };

    struct IoAddress {
        static constexpr std::size_t kBytes = 128;

        alignas(16) unsigned char bytes[kBytes] = {};
        
        std::int32_t len = static_cast<std::int32_t>(kBytes);
    };

    struct IoRequest {
        
        static constexpr std::size_t kMaxVectors = 8;
        static constexpr std::size_t kNativeBytes = 192;

        alignas(16) unsigned char native[kNativeBytes] = {};

        Task*         resume = nullptr;   
        IoResult*     out    = nullptr;   
        std::uint32_t token  = 0xFFFFFFFFu;
        void*         handle = nullptr;

        enum class Kind : std::uint8_t { Generic, Accept, Connect, Send, Recv };
        Kind          kind = Kind::Generic;
        std::uintptr_t aux = 0;

        std::uint32_t bufCount = 0;

        std::uint32_t xferred = 0;

        void (*onComplete)(IoRequest*) = nullptr;

        std::uintptr_t hookCtx = 0;

        std::uint32_t flags = 0;

        IoRequest*    prev = nullptr;     
        IoRequest*    next = nullptr;
    };

    using IoSocket = std::uintptr_t;

    struct IoAcceptBuffer {
        static constexpr std::size_t kBytes = 128;
        alignas(16) unsigned char bytes[kBytes] = {};
    };

    struct IoLockStats {
        std::uint64_t acquires  = 0;
        std::uint64_t contended = 0;
    };
    IoLockStats ReadIoLockStats() noexcept;
    void        ResetIoLockStats() noexcept;

    struct IoRoutingStats {
        std::uint64_t toLane  = 0;
        std::uint64_t toFloor = 0;
        
        std::uint64_t floorFallback = 0;
    };
    IoRoutingStats ReadIoRoutingStats() noexcept;
    void           ResetIoRoutingStats() noexcept;

    namespace detail {
        
        extern std::atomic<std::uint64_t> g_ioToLane;
        extern std::atomic<std::uint64_t> g_ioToFloor;
        extern std::atomic<std::uint64_t> g_ioFloorFallback;
    }

    class IoReactor {
    public:
        static IoReactor& Instance();

        static bool IsAvailable() noexcept;

        bool Register(void* handle);

        bool SubmitRead (void* handle, void* buf, std::uint32_t len, std::uint64_t offset,
                         IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitWrite(void* handle, const void* buf, std::uint32_t len, std::uint64_t offset,
                         IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        bool InitSockets();

        bool RegisterSocket(IoSocket s);

        bool SubmitRecv(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                        IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitSend(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                        IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        bool SubmitRecvV(IoSocket s, const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                         IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitSendV(IoSocket s, const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                         IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        bool SubmitRecvFrom(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                            IoAddress* from,
                            IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitSendTo(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                          const void* addr, std::uint32_t addrLen,
                          IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        bool SubmitAccept(IoSocket listener, IoSocket accepted, IoAcceptBuffer* addrs,
                          IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        bool SubmitConnect(IoSocket s, const void* sockaddr, std::uint32_t sockaddrLen,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        bool SubmitDisconnect(IoSocket s, bool reuse,
                              IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        static bool SupportsDisconnectReuse() noexcept;

        bool SubmitPrepared(IoRequest* req);

        std::size_t RequestCancel(CancelToken token) noexcept;

        std::size_t InFlight() const noexcept;

        static std::size_t CompletionCoreHistogram(unsigned* counts, std::size_t max) noexcept;

        void Stop() noexcept;

        void Start() noexcept;

        struct Impl;

    private:
        IoReactor();
        ~IoReactor();
        IoReactor(const IoReactor&) = delete;
        IoReactor& operator=(const IoReactor&) = delete;

        Impl* impl;
    };

    class IoStream {
    public:
        explicit IoStream(IoSocket s) noexcept : sock_(s) {}

        IoStream(const IoStream&) = delete;
        IoStream& operator=(const IoStream&) = delete;

        IoSocket Socket() const noexcept { return sock_; }

        bool SubmitSend(const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                        IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitRecv(const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                        IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        std::size_t QueuedSends() const noexcept;
        std::size_t QueuedRecvs() const noexcept;

    private:
        
        static void OnSendComplete(IoRequest* r);
        static void OnRecvComplete(IoRequest* r);

        struct Chain {
            IoRequest* head = nullptr;      
            IoRequest* tail = nullptr;
            bool       busy = false;
            std::size_t queued = 0;
        };

        bool SubmitChained(Chain& c, IoRequest::Kind kind,
                           const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitOne(IoRequest* req);
        void Advance(Chain& c, IoRequest::Kind kind);

        IoSocket sock_;
        
        mutable std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
        Chain send_;
        Chain recv_;
    };

    struct IoAcceptWaiter {
        Task*           resume = nullptr;
        IoSocket*       out    = nullptr;   
        std::uint32_t   token  = 0xFFFFFFFFu;
        IoAcceptWaiter* next   = nullptr;
    };

    class IoAcceptor {
    public:
        IoAcceptor() noexcept = default;
        ~IoAcceptor();

        IoAcceptor(const IoAcceptor&) = delete;
        IoAcceptor& operator=(const IoAcceptor&) = delete;

        bool Start(IoSocket listener, unsigned depth = 8);

        void Stop() noexcept;

        IoSocket TryTake() noexcept;

        bool TakeOrQueue(IoAcceptWaiter* w, IoSocket* out, Task* resume, CancelToken token);

        std::size_t CancelWaiters(CancelToken token) noexcept;

        std::size_t Outstanding() const noexcept;   
        std::size_t Available() const noexcept;     

    private:
        struct Impl;
        Impl* impl = nullptr;
    };

    void EjectIoReactor(void* ctx, CancelToken token);

    void EjectIoAcceptor(void* ctx, CancelToken token);

} 
