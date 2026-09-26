// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/IoReactor.h"
#include "../include/TaskScheduler.h"
#include "IoPlatform.h"

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

namespace JLib {

    void EjectIoReactor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoReactor*>(ctx)->RequestCancel(token);
    }

    void EjectIoAcceptor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoAcceptor*>(ctx)->CancelWaiters(token);
    }

    namespace {
        struct SpinGuard {
            std::atomic_flag& f;
            explicit SpinGuard(std::atomic_flag& x) : f(x) {
                while (f.test_and_set(std::memory_order_acquire)) platform::CpuRelax();
            }
            ~SpinGuard() { f.clear(std::memory_order_release); }
        };
    }

    bool IoStream::SubmitChained(Chain& c, IoRequest::Kind kind,
                                 const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                                 IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        
        if (!ioplat::FillBufs(req, bufs, count)) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, ioplat::kErrMsgSize };
            return true;
        }

        if (CancelToken(token.Raw()).Cancelled()) {
            if (out) *out = IoResult{ IoStatus::Cancelled, 0, 0 };
            return true;
        }

        req->kind      = kind;
        req->bufCount  = count;
        req->flags     = flags;
        
        req->xferred   = 0;
        req->out       = out;
        req->resume    = resume;
        req->token     = token.Raw();
        req->handle    = reinterpret_cast<void*>(static_cast<std::uintptr_t>(sock_));
        req->hookCtx   = reinterpret_cast<std::uintptr_t>(this);
        req->onComplete = (kind == IoRequest::Kind::Send) ? &IoStream::OnSendComplete
                                                          : &IoStream::OnRecvComplete;
        req->next = nullptr;

        {
            SpinGuard g(lock_);
            if (c.busy) {
                
                if (c.tail) c.tail->next = req; else c.head = req;
                c.tail = req;
                ++c.queued;
                return false;                  
            }
            c.busy = true;
        }

        return SubmitOne(req);
    }

    bool IoStream::SubmitOne(IoRequest* req) {
        const bool immediate = IoReactor::Instance().SubmitPrepared(req);

        if (immediate) {
            
            Chain& c = (req->kind == IoRequest::Kind::Send) ? send_ : recv_;
            Advance(c, req->kind);
        }
        return immediate;
    }

    void IoStream::Advance(Chain& c, IoRequest::Kind kind) {
        IoRequest* next = nullptr;
        {
            SpinGuard g(lock_);
            if (c.head) {
                next = c.head;
                c.head = next->next;
                if (!c.head) c.tail = nullptr;
                --c.queued;
                next->next = nullptr;
                
            } else {
                c.busy = false;
            }
        }
        if (next) SubmitOne(next);
        (void)kind;
    }

    void IoStream::OnSendComplete(IoRequest* r) {
        IoStream* s = reinterpret_cast<IoStream*>(r->hookCtx);
        if (s) s->Advance(s->send_, IoRequest::Kind::Send);
    }

    void IoStream::OnRecvComplete(IoRequest* r) {
        IoStream* s = reinterpret_cast<IoStream*>(r->hookCtx);
        if (s) s->Advance(s->recv_, IoRequest::Kind::Recv);
    }

    bool IoStream::SubmitSend(const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                              IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        return SubmitChained(send_, IoRequest::Kind::Send, bufs, count, flags,
                             req, out, resume, token);
    }

    bool IoStream::SubmitRecv(const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                              IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        return SubmitChained(recv_, IoRequest::Kind::Recv, bufs, count, flags,
                             req, out, resume, token);
    }

    std::size_t IoStream::QueuedSends() const noexcept {
        SpinGuard g(lock_);
        return send_.queued;
    }

    std::size_t IoStream::QueuedRecvs() const noexcept {
        SpinGuard g(lock_);
        return recv_.queued;
    }

    struct IoAcceptor::Impl {
        
        struct Slot {
            IoRequest      req{};
            IoAcceptBuffer addrs{};
            IoResult       result{};
            IoSocket         sock = ioplat::kNoSocket;
            Impl*          owner = nullptr;
            unsigned       index = 0;
        };

        IoSocket listener = ioplat::kNoSocket;
        int    family = AF_INET, type = SOCK_STREAM, proto = IPPROTO_TCP;

        std::vector<Slot>   slots;
        mutable std::mutex  m;
        std::vector<IoSocket> ready;          
        IoAcceptWaiter*     waitHead = nullptr;   
        IoAcceptWaiter*     waitTail = nullptr;
        std::size_t         outstanding = 0;
        bool                stopping = false;

        CancelScope         scope;          

        IoSocket MakeSocket() const {
            return ioplat::MakeSocket(family, type, proto);
        }

        void Post(unsigned i) {
            Slot& s = slots[i];
            s.sock = MakeSocket();
            if (s.sock == ioplat::kNoSocket) return;

            if (!IoReactor::Instance().RegisterSocket(static_cast<IoSocket>(s.sock))) {
                ioplat::CloseSocket(s.sock);
                s.sock = ioplat::kNoSocket;
                return;
            }

            s.req.onComplete = &Impl::AcceptCompleted;
            s.req.hookCtx    = reinterpret_cast<std::uintptr_t>(&s);

            { std::lock_guard<std::mutex> lk(m); ++outstanding; }

            if (IoReactor::Instance().SubmitAccept(static_cast<IoSocket>(listener),
                                                   static_cast<IoSocket>(s.sock), &s.addrs,
                                                   &s.req, &s.result, nullptr, scope.Token())) {
                
                { std::lock_guard<std::mutex> lk(m); --outstanding; }
                ioplat::CloseSocket(s.sock);
                s.sock = ioplat::kNoSocket;
            }
        }

        static void AcceptCompleted(IoRequest* r) {
            Slot* s = reinterpret_cast<Slot*>(r->hookCtx);
            if (s && s->owner) s->owner->OnComplete(s->index);
        }

        void OnComplete(unsigned i) {
            Slot& s = slots[i];
            const IoSocket accepted = s.sock;     
            s.sock = ioplat::kNoSocket;

            bool   repost = false;
            bool   keep   = false;              
            IoAcceptWaiter* handTo = nullptr;
            IoAcceptWaiter* skipped = nullptr;   
            {
                std::lock_guard<std::mutex> lk(m);
                --outstanding;
                if (s.result.status == IoStatus::Completed && !stopping) {
                    repost = true;
                    keep   = true;
                    
                    while (waitHead) {
                        IoAcceptWaiter* w = waitHead;
                        waitHead = w->next;
                        if (!waitHead) waitTail = nullptr;
                        w->next = nullptr;

                        if (CancelToken(w->token).Cancelled()) {
                            w->next = skipped;      
                            skipped = w;
                            continue;
                        }
                        handTo = w;
                        break;
                    }
                    if (!handTo) ready.push_back(accepted);
                }
            }

            if (!keep && accepted != ioplat::kNoSocket) ioplat::CloseSocket(accepted);

            if (handTo) {
                if (handTo->out) *handTo->out = static_cast<IoSocket>(accepted);
                Task* t = handTo->resume;
                if (t && TaskScheduler::IsInitialized()) TaskScheduler::Instance().WakeTask(t);
            }

            while (skipped) {
                IoAcceptWaiter* w = skipped;
                skipped = w->next;
                if (w->out) *w->out = 0;
                Task* t = w->resume;
                if (t && TaskScheduler::IsInitialized()) TaskScheduler::Instance().WakeTask(t);
            }

            if (repost) Post(i);
        }
    };

    IoAcceptor::~IoAcceptor() {
        Stop();
        delete impl;
        impl = nullptr;
    }

    bool IoAcceptor::Start(IoSocket listener, unsigned depth) {
        if (impl || depth == 0) return false;
        if (!IoReactor::IsAvailable()) return false;

        impl = new Impl();
        impl->listener = static_cast<IoSocket>(listener);

        int fam = 0, typ = 0, prot = 0;
        if (ioplat::QuerySocketTriple(impl->listener, fam, typ, prot)) {
            impl->family = fam;
            impl->type   = typ;
            impl->proto  = prot;
        }

        impl->slots.resize(depth);
        for (unsigned i = 0; i < depth; ++i) {
            impl->slots[i].owner = impl;
            impl->slots[i].index = i;
        }
        for (unsigned i = 0; i < depth; ++i) impl->Post(i);

        std::lock_guard<std::mutex> lk(impl->m);
        return impl->outstanding > 0;
    }

    void IoAcceptor::Stop() noexcept {
        if (!impl) return;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            if (impl->stopping) return;
            impl->stopping = true;
        }

        CancelWaiters(CancelToken{});

        impl->scope.Cancel();
        IoReactor::Instance().RequestCancel(impl->scope.Token());

        for (int spins = 0; spins < 5000; ++spins) {
            { std::lock_guard<std::mutex> lk(impl->m); if (impl->outstanding == 0) break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        std::vector<IoSocket> leftovers;
        { std::lock_guard<std::mutex> lk(impl->m); leftovers.swap(impl->ready); }
        for (IoSocket s : leftovers) ioplat::CloseSocket(s);
    }

    IoSocket IoAcceptor::TryTake() noexcept {
        if (!impl) return 0;
        std::lock_guard<std::mutex> lk(impl->m);
        if (impl->ready.empty()) return 0;
        const IoSocket s = impl->ready.back();
        impl->ready.pop_back();
        return static_cast<IoSocket>(s);
    }

    bool IoAcceptor::TakeOrQueue(IoAcceptWaiter* w, IoSocket* out, Task* resume, CancelToken token) {
        if (!impl || !w || !out) { if (out) *out = 0; return true; }

        if (CancelToken(token.Raw()).Cancelled()) { *out = 0; return true; }

        std::lock_guard<std::mutex> lk(impl->m);
        if (!impl->ready.empty()) {
            *out = static_cast<IoSocket>(impl->ready.back());
            impl->ready.pop_back();
            return true;                       
        }
        if (impl->stopping) { *out = 0; return true; }

        w->resume = resume;
        w->out    = out;
        w->token  = token.Raw();
        w->next   = nullptr;
        if (impl->waitTail) impl->waitTail->next = w; else impl->waitHead = w;
        impl->waitTail = w;
        return false;                          
    }

    std::size_t IoAcceptor::CancelWaiters(CancelToken token) noexcept {
        if (!impl) return 0;

        IoAcceptWaiter* taken = nullptr;
        IoAcceptWaiter* takenTail = nullptr;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            IoAcceptWaiter* keepHead = nullptr;
            IoAcceptWaiter* keepTail = nullptr;
            for (IoAcceptWaiter* w = impl->waitHead; w; ) {
                IoAcceptWaiter* next = w->next;
                w->next = nullptr;
                const bool match = !token.Valid() || CancelToken(w->token).IsWithin(token);
                if (match) {
                    if (takenTail) takenTail->next = w; else taken = w;
                    takenTail = w;
                } else {
                    if (keepTail) keepTail->next = w; else keepHead = w;
                    keepTail = w;
                }
                w = next;
            }
            impl->waitHead = keepHead;
            impl->waitTail = keepTail;
        }

        std::size_t n = 0;
        while (taken) {
            IoAcceptWaiter* next = taken->next;
            if (taken->out) *taken->out = 0;
            Task* t = taken->resume;
            if (t && TaskScheduler::IsInitialized()) TaskScheduler::Instance().WakeTask(t);
            ++n;
            taken = next;
        }
        return n;
    }

    std::size_t IoAcceptor::Outstanding() const noexcept {
        if (!impl) return 0;
        std::lock_guard<std::mutex> lk(impl->m);
        return impl->outstanding;
    }

    std::size_t IoAcceptor::Available() const noexcept {
        if (!impl) return 0;
        std::lock_guard<std::mutex> lk(impl->m);
        return impl->ready.size();
    }

    namespace detail {
        std::atomic<std::uint64_t> g_ioToLane{ 0 };
        std::atomic<std::uint64_t> g_ioToFloor{ 0 };
        std::atomic<std::uint64_t> g_ioFloorFallback{ 0 };

        std::atomic<std::size_t>  g_ioOutstanding{ 0 };
    }

    IoRoutingStats ReadIoRoutingStats() noexcept {
        IoRoutingStats s;
        s.toLane  = detail::g_ioToLane.load(std::memory_order_relaxed);
        s.toFloor = detail::g_ioToFloor.load(std::memory_order_relaxed);
        s.floorFallback = detail::g_ioFloorFallback.load(std::memory_order_relaxed);
        return s;
    }

    void ResetIoRoutingStats() noexcept {
        detail::g_ioToLane.store(0, std::memory_order_relaxed);
        detail::g_ioToFloor.store(0, std::memory_order_relaxed);
        detail::g_ioFloorFallback.store(0, std::memory_order_relaxed);
    }

} 
