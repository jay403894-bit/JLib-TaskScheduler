// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include "CancelToken.h"
#include "TaskScheduler.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <new>
#include <utility>

namespace JLib {

    enum class FutureStatus : std::uint8_t {
        Ready,       
        Cancelled,   
        Broken       
    };

    namespace detail {

        struct FutureWaiter {
            Task*          resume = nullptr;
            std::uint32_t  token  = CancelToken::kNone;
            FutureWaiter*  next   = nullptr;
            FutureStatus   status = FutureStatus::Ready;
        };

        class FutureStateBase {
        public:
            void Retain() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }

            bool Release() noexcept {
                if (refs_.fetch_sub(1, std::memory_order_release) != 1) return false;
                std::atomic_thread_fence(std::memory_order_acquire);
                return true;
            }

            int  UseCount() const noexcept { return refs_.load(std::memory_order_relaxed); }
            bool Ready()  const noexcept { std::lock_guard<std::mutex> lk(m_); return ready_; }
            bool Broken() const noexcept { std::lock_guard<std::mutex> lk(m_); return broken_ && !ready_; }

            bool ReadyOrQueue(FutureWaiter* w, Task* resume, CancelToken token) {
                std::lock_guard<std::mutex> lk(m_);
                if (ready_)  { w->status = FutureStatus::Ready;  return true; }
                if (broken_) { w->status = FutureStatus::Broken; return true; }
                if (token.Valid() && token.Cancelled()) {
                    w->status = FutureStatus::Cancelled;
                    return true;
                }
                w->resume = resume;
                w->token  = token.Raw();
                w->status = FutureStatus::Ready;
                w->next   = waiters_;
                waiters_  = w;
                return false;
            }

            std::size_t CancelWaiters(CancelToken token) noexcept {
                FutureWaiter* taken = nullptr;
                {
                    std::lock_guard<std::mutex> lk(m_);
                    FutureWaiter** link = &waiters_;
                    while (*link) {
                        FutureWaiter* w = *link;
                        const bool match = !token.Valid() || CancelToken(w->token).IsWithin(token);
                        if (match) {
                            *link = w->next;          
                            w->status = FutureStatus::Cancelled;   
                            w->next = taken;          
                            taken = w;
                        } else {
                            link = &w->next;
                        }
                    }
                }
                return Wake(taken);
            }

            std::size_t Publish(bool broken) noexcept {
                FutureWaiter* taken = nullptr;
                {
                    std::lock_guard<std::mutex> lk(m_);
                    if (ready_ || broken_) return 0;      
                    if (broken) broken_ = true; else ready_ = true;
                    taken = waiters_;
                    waiters_ = nullptr;
                    for (FutureWaiter* w = taken; w; w = w->next)
                        w->status = broken ? FutureStatus::Broken : FutureStatus::Ready;
                }
                return Wake(taken);
            }

        protected:
            ~FutureStateBase() = default;

            static std::size_t Wake(FutureWaiter* list) noexcept {
                std::size_t n = 0;
                while (list) {
                    FutureWaiter* next = list->next;   
                    Task* t = list->resume;
                    if (t && TaskScheduler::IsInitialized()) TaskScheduler::Instance().WakeTask(t);
                    ++n;
                    list = next;
                }
                return n;
            }

            mutable std::mutex m_;
            FutureWaiter*      waiters_ = nullptr;
            std::atomic<int>   refs_{ 1 };
            bool               ready_   = false;
            bool               broken_  = false;
        };

        template <class T>
        class FutureState final : public FutureStateBase {
        public:
            ~FutureState() { if (ready_) Value()->~T(); }

            template <class U>
            std::size_t SetValue(U&& v) {
                
                {
                    std::lock_guard<std::mutex> lk(m_);
                    if (ready_ || broken_) return 0;
                }
                ::new (static_cast<void*>(storage_)) T(std::forward<U>(v));
                return Publish( false);
            }

            T*       Value()       noexcept { return reinterpret_cast<T*>(storage_); }
            const T* Value() const noexcept { return reinterpret_cast<const T*>(storage_); }

        private:
            alignas(T) unsigned char storage_[sizeof(T)];
        };

        template <>
        class FutureState<void> final : public FutureStateBase {
        public:
            std::size_t SetValue() { return Publish( false); }
        };

        template <class State>
        class FutureHandle {
        public:
            FutureHandle() noexcept = default;
            FutureHandle(const FutureHandle& o) noexcept : s_(o.s_) { if (s_) s_->Retain(); }
            FutureHandle(FutureHandle&& o) noexcept : s_(std::exchange(o.s_, nullptr)) {}

            FutureHandle& operator=(const FutureHandle& o) noexcept {
                if (this != &o) { FutureHandle tmp(o); Swap(tmp); }
                return *this;
            }
            FutureHandle& operator=(FutureHandle&& o) noexcept {
                if (this != &o) { FutureHandle tmp(std::move(o)); Swap(tmp); }
                return *this;
            }
            ~FutureHandle() { Reset(); }

            void Swap(FutureHandle& o) noexcept { std::swap(s_, o.s_); }
            void Reset() noexcept {
                if (s_ && s_->Release()) delete s_;
                s_ = nullptr;
            }

            [[nodiscard]] bool Valid()  const noexcept { return s_ != nullptr; }
            [[nodiscard]] bool Ready()  const noexcept { return s_ && s_->Ready(); }
            [[nodiscard]] bool Broken() const noexcept { return s_ && s_->Broken(); }
            [[nodiscard]] int  UseCount() const noexcept { return s_ ? s_->UseCount() : 0; }

            std::size_t CancelWaiters(CancelToken token) noexcept {
                return s_ ? s_->CancelWaiters(token) : 0;
            }

            State* State_() const noexcept { return s_; }

        protected:
            explicit FutureHandle(State* s) noexcept : s_(s) { if (s_) s_->Retain(); }
            State* s_ = nullptr;
        };

        template <class State>
        class PromiseHandle {
        public:
            PromiseHandle() : s_(new State()) {}
            PromiseHandle(const PromiseHandle&) = delete;
            PromiseHandle& operator=(const PromiseHandle&) = delete;
            PromiseHandle(PromiseHandle&& o) noexcept : s_(std::exchange(o.s_, nullptr)) {}
            PromiseHandle& operator=(PromiseHandle&& o) noexcept {
                if (this != &o) { Break(); s_ = std::exchange(o.s_, nullptr); }
                return *this;
            }

            ~PromiseHandle() { Break(); }

            [[nodiscard]] bool Valid() const noexcept { return s_ != nullptr; }

        protected:
            void Break() noexcept {
                if (!s_) return;
                s_->Publish( true);      
                if (s_->Release()) delete s_;
                s_ = nullptr;
            }
            State* s_ = nullptr;
        };

    } 

    template <class T> class Promise;
    
    template <class T>
    class Future : public detail::FutureHandle<detail::FutureState<T>> {
        using Base = detail::FutureHandle<detail::FutureState<T>>;
    public:
        using Base::Base;
        Future() noexcept = default;

        [[nodiscard]] const T& Get() const noexcept {
            assert(this->s_ && this->s_->Ready() && "Future::Get() before the value is set");
            return *this->s_->Value();
        }

        [[nodiscard]] T Take() {
            assert(this->s_ && this->s_->Ready() && "Future::Take() before the value is set");
            assert(this->UseCount() == 1 &&
                   "Future::Take() with other consumers still holding this result");
            return std::move(*this->s_->Value());
        }

    private:
        friend class Promise<T>;
    };

    template <>
    class Future<void> : public detail::FutureHandle<detail::FutureState<void>> {
        using Base = detail::FutureHandle<detail::FutureState<void>>;
    public:
        using Base::Base;
        Future() noexcept = default;
    private:
        friend class Promise<void>;
    };

    template <class T>
    class Promise : public detail::PromiseHandle<detail::FutureState<T>> {
        using Base = detail::PromiseHandle<detail::FutureState<T>>;
    public:
        [[nodiscard]] Future<T> GetFuture() const noexcept { return Future<T>(this->s_); }

        template <class U>
        std::size_t Set(U&& v) { return this->s_ ? this->s_->SetValue(std::forward<U>(v)) : 0; }
    };

    template <>
    class Promise<void> : public detail::PromiseHandle<detail::FutureState<void>> {
        using Base = detail::PromiseHandle<detail::FutureState<void>>;
    public:
        [[nodiscard]] Future<void> GetFuture() const noexcept { return Future<void>(this->s_); }
        std::size_t Set() { return this->s_ ? this->s_->SetValue() : 0; }
    };

} 
