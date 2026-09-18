// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#if !defined(_WIN32) && !defined(__linux__)

#include "../include/IoReactor.h"

#include <cerrno>

namespace JLib {

    struct IoReactor::Impl { int unused = 0; };

    IoReactor::IoReactor() : impl(new Impl()) {}
    IoReactor::~IoReactor() { delete impl; }

    IoReactor& IoReactor::Instance() { static IoReactor r; return r; }

    bool IoReactor::IsAvailable() noexcept { return false; }
    bool IoReactor::Register(void*) { return false; }

    bool IoReactor::SubmitRead(void*, void*, std::uint32_t, std::uint64_t,
                               IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitWrite(void*, const void*, std::uint32_t, std::uint64_t,
                                IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }

    bool IoReactor::InitSockets() { return false; }
    bool IoReactor::RegisterSocket(IoSocket) { return false; }

    bool IoReactor::SubmitRecv(IoSocket, void*, std::uint32_t, std::uint32_t,
                               IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitSend(IoSocket, const void*, std::uint32_t, std::uint32_t,
                               IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitAccept(IoSocket, IoSocket, IoAcceptBuffer*,
                                 IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitConnect(IoSocket, const void*, std::uint32_t,
                                  IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }

    bool IoReactor::SubmitRecvV(IoSocket, const IoBuffer*, std::uint32_t, std::uint32_t,
                                IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitSendV(IoSocket, const IoBuffer*, std::uint32_t, std::uint32_t,
                                IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitRecvFrom(IoSocket, void*, std::uint32_t, std::uint32_t, IoAddress*,
                                   IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitSendTo(IoSocket, const void*, std::uint32_t, std::uint32_t,
                                 const void*, std::uint32_t,
                                 IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitDisconnect(IoSocket, bool, IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }

    std::size_t IoReactor::RequestCancel(CancelToken) noexcept { return 0; }
    std::size_t IoReactor::InFlight() const noexcept { return 0; }
    
    void IoReactor::Stop() noexcept {}
    void IoReactor::Start() noexcept {}

    struct IoAcceptor::Impl { int unused = 0; };
    IoAcceptor::~IoAcceptor() { delete impl; }
    bool IoAcceptor::Start(IoSocket, unsigned) { return false; }
    void IoAcceptor::Stop() noexcept {}
    IoSocket IoAcceptor::TryTake() noexcept { return 0; }
    std::size_t IoAcceptor::Outstanding() const noexcept { return 0; }
    std::size_t IoAcceptor::Available() const noexcept { return 0; }
    
    bool IoAcceptor::TakeOrQueue(IoAcceptWaiter*, IoSocket* out, Task*, CancelToken) {
        if (out) *out = 0;
        return true;
    }
    std::size_t IoAcceptor::CancelWaiters(CancelToken) noexcept { return 0; }

    void EjectIoReactor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoReactor*>(ctx)->RequestCancel(token);
    }

    void EjectIoAcceptor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoAcceptor*>(ctx)->CancelWaiters(token);
    }

} 

#endif 
