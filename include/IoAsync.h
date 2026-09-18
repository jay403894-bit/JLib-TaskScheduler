// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include "IoReactor.h"
#include "Coroutine.h"

#include <cstdint>
#include <utility>

namespace JLib {

    namespace detail {

        template <typename Fn>
        class IoOpAwaiter {
        public:
            IoOpAwaiter(Pin pin, Fn fn) noexcept : fn_(std::move(fn)), pin_(pin) {}

            IoOpAwaiter(const IoOpAwaiter&) = delete;
            IoOpAwaiter& operator=(const IoOpAwaiter&) = delete;

            bool await_ready() const noexcept { return false; }

            template <typename P>
            bool await_suspend(std::coroutine_handle<P> h) {
                Task* t = detail::ArmResume(h, pin_);
                return !fn_(&req_, &result_, t);
            }

            [[nodiscard]] IoResult await_resume() const noexcept { return result_; }

        private:
            Fn fn_;
            Pin pin_;

            IoRequest req_{};
            IoResult  result_{};
        };

        template <typename Fn>
        IoOpAwaiter<Fn> MakeIoAwaiter(Pin pin, Fn fn) noexcept { return IoOpAwaiter<Fn>(pin, std::move(fn)); }

    } 

    [[nodiscard]] inline auto ReadAsync(void* handle, void* buf, std::uint32_t len,
                                        std::uint64_t offset = 0,
                                        CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitRead(handle, buf, len, offset, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto WriteAsync(void* handle, const void* buf, std::uint32_t len,
                                         std::uint64_t offset = 0,
                                         CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitWrite(handle, buf, len, offset, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto RecvRawAsync(IoSocket s, void* buf, std::uint32_t len,
                                        std::uint32_t flags = 0,
                                        CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitRecv(s, buf, len, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendRawAsync(IoSocket s, const void* buf, std::uint32_t len,
                                        std::uint32_t flags = 0,
                                        CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitSend(s, buf, len, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto AcceptAsync(IoSocket listener, IoSocket accepted,
                                          IoAcceptBuffer* addrs,
                                          CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitAccept(listener, accepted, addrs, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto ConnectAsync(IoSocket s, const void* sockaddr,
                                           std::uint32_t sockaddrLen,
                                           CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitConnect(s, sockaddr, sockaddrLen, r, o, t, token);
        });
    }

    class AcceptAwaiter {
    public:
        AcceptAwaiter(IoAcceptor& a, CancelToken t, Pin pin) noexcept : acc_(a), token_(t), pin_(pin) {}
        AcceptAwaiter(const AcceptAwaiter&) = delete;
        AcceptAwaiter& operator=(const AcceptAwaiter&) = delete;

        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            Task* t = detail::ArmResume(h, pin_);
            return !acc_.TakeOrQueue(&w_, &sock_, t, token_);
        }

        [[nodiscard]] IoSocket await_resume() const noexcept { return sock_; }

    private:
        IoAcceptor&    acc_;
        CancelToken    token_;
        Pin            pin_;
        
        IoAcceptWaiter w_{};
        IoSocket       sock_ = 0;
    };

    [[nodiscard]] inline AcceptAwaiter AcceptAsync(IoAcceptor& acc,
                                                   CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return AcceptAwaiter(acc, token, pin);
    }

    [[nodiscard]] inline auto RecvVRawAsync(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                                         std::uint32_t flags = 0,
                                         CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitRecvV(s, bufs, count, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendVRawAsync(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                                         std::uint32_t flags = 0,
                                         CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitSendV(s, bufs, count, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto DisconnectAsync(IoSocket s, bool reuse = true,
                                              CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitDisconnect(s, reuse, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto RecvFromAsync(IoSocket s, void* buf, std::uint32_t len,
                                            IoAddress* from, std::uint32_t flags = 0,
                                            CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitRecvFrom(s, buf, len, flags, from, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendToAsync(IoSocket s, const void* buf, std::uint32_t len,
                                          const void* addr, std::uint32_t addrLen,
                                          std::uint32_t flags = 0,
                                          CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitSendTo(s, buf, len, flags, addr, addrLen,
                                                      r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendAsync(IoStream& s, const void* buf, std::uint32_t len,
                                        std::uint32_t flags = 0,
                                        CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [&s, buf, len, flags, token](IoRequest* r, IoResult* o, Task* t) {
            const IoBuffer one{ const_cast<void*>(buf), len };
            return s.SubmitSend(&one, 1, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendVAsync(IoStream& s, const IoBuffer* bufs, std::uint32_t count,
                                         std::uint32_t flags = 0,
                                         CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [&s, bufs, count, flags, token](IoRequest* r, IoResult* o, Task* t) {
            return s.SubmitSend(bufs, count, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto RecvAsync(IoStream& s, void* buf, std::uint32_t len,
                                        std::uint32_t flags = 0,
                                        CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [&s, buf, len, flags, token](IoRequest* r, IoResult* o, Task* t) {
            const IoBuffer one{ buf, len };
            return s.SubmitRecv(&one, 1, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto RecvVAsync(IoStream& s, const IoBuffer* bufs, std::uint32_t count,
                                         std::uint32_t flags = 0,
                                         CancelToken token = CancelToken{}, Pin pin = Pin::None) noexcept {
        return detail::MakeIoAwaiter(pin, [&s, bufs, count, flags, token](IoRequest* r, IoResult* o, Task* t) {
            return s.SubmitRecv(bufs, count, flags, r, o, t, token);
        });
    }

} 
