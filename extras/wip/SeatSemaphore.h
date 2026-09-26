// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

#if defined(_WIN32)
	#ifndef WIN32_LEAN_AND_MEAN
		#define WIN32_LEAN_AND_MEAN
	#endif
	#ifndef NOMINMAX
		#define NOMINMAX
	#endif
	#include <windows.h>
	#pragma comment(lib, "Synchronization.lib")
#elif defined(__linux__)
	#include <unistd.h>
	#include <sys/syscall.h>
	#include <linux/futex.h>
#else
	
	#include <condition_variable>
	#include <mutex>
#endif

namespace JLib {

class SeatSemaphore {
public:
	static constexpr int kMinSeats = 2;

	SeatSemaphore() = default;
	SeatSemaphore(const SeatSemaphore&) = delete;
	SeatSemaphore& operator=(const SeatSemaphore&) = delete;

	void Init(int workerCount, std::chrono::nanoseconds interval) noexcept {
		count_.store(0, std::memory_order_relaxed);
		sleepers_.store(0, std::memory_order_relaxed);
		awake_.store(0, std::memory_order_relaxed);
		armed_.store(0, std::memory_order_relaxed);
		ready_.store(0, std::memory_order_relaxed);
		wideInFlight_.store(0, std::memory_order_relaxed);
		handoffFrom_.store(-1, std::memory_order_relaxed);
		advertised_.store(0, std::memory_order_relaxed);
		handoffs_.store(0, std::memory_order_relaxed);
		lastGrowNs_.store(0, std::memory_order_relaxed);
		intervalNs_.store((long long)interval.count(), std::memory_order_relaxed);
		cursor_.store(0, std::memory_order_relaxed);

		const int n = (workerCount < 1) ? 1 : workerCount;
		uint64_t seats = 1ull;
		if (n > 1) seats |= 2ull;
		seatMask_.store(seats, std::memory_order_relaxed);
		destMask_.store(seats, std::memory_order_relaxed);
	}

	int PickDest(unsigned long long ticket) const noexcept {
		const uint64_t m = destMask_.load(std::memory_order_acquire);
		if (m == 0) return -1;
		unsigned n = 0;
		for (uint64_t x = m; x; x &= (x - 1)) ++n;
		unsigned k = (unsigned)(ticket % n);
		uint64_t r = m;
		while (k--) r &= (r - 1);
		return TrailingZero(r);
	}
	uint64_t DestMask() const noexcept { return destMask_.load(std::memory_order_acquire); }
	unsigned DestCount() const noexcept {
		unsigned c = 0;
		for (uint64_t m = destMask_.load(std::memory_order_acquire); m; m &= (m - 1)) ++c;
		return c;
	}

	void WideEnter() noexcept { wideInFlight_.fetch_add(1, std::memory_order_acq_rel); }
	void WideLeave() noexcept { wideInFlight_.fetch_sub(1, std::memory_order_acq_rel); }
	bool WideLive()  const noexcept { return wideInFlight_.load(std::memory_order_acquire) > 0; }
	uint64_t SeatMask() const noexcept { return seatMask_.load(std::memory_order_acquire); }
	bool IsSeat(int id) const noexcept {
		return id >= 0 && id < 64 && (seatMask_.load(std::memory_order_acquire) & (1ull << id)) != 0;
	}

	void AddDest(int id) noexcept {
		if (id >= 0 && id < 64) destMask_.fetch_or(1ull << id, std::memory_order_release);
	}
	
	void RetireDest(int id) noexcept {
		if (id < 0 || id >= 64) return;
		const uint64_t bit = 1ull << id;
		if (seatMask_.load(std::memory_order_acquire) & bit) return;
		destMask_.fetch_and(~bit, std::memory_order_release);
	}
	void ShrinkDestToSeats() noexcept {
		destMask_.store(seatMask_.load(std::memory_order_acquire), std::memory_order_release);
	}

	void Advertise(int id) noexcept {
		if (id >= 0 && id < 64) advertised_.fetch_or(1ull << id, std::memory_order_release);
	}
	void Unadvertise(int id) noexcept {
		if (id >= 0 && id < 64) advertised_.fetch_and(~(1ull << id), std::memory_order_release);
	}
	uint64_t Advertised() const noexcept { return advertised_.load(std::memory_order_acquire); }

	bool ArmHandoff(int fromSeat) noexcept {
		if (fromSeat < 0 || fromSeat >= 64) return false;
		int expected = 0;
		if (!armed_.compare_exchange_strong(expected, 1, std::memory_order_acq_rel,
		                                    std::memory_order_relaxed)) return false;
		handoffFrom_.store(fromSeat, std::memory_order_release);
		return true;
	}

	int ClaimSeat(int myId) noexcept {
		if (myId < 0 || myId >= 64) return -1;
		if (armed_.load(std::memory_order_acquire) == 0) return -1;
		const int from = handoffFrom_.load(std::memory_order_acquire);
		if (from < 0 || from == myId) return -1;
		int expected = 1;
		if (!armed_.compare_exchange_strong(expected, 0, std::memory_order_acq_rel,
		                                    std::memory_order_relaxed)) return -1;

		const uint64_t fromBit = 1ull << from;
		const uint64_t myBit   = 1ull << myId;
		uint64_t seats = seatMask_.load(std::memory_order_acquire);
		while (!seatMask_.compare_exchange_weak(seats, (seats & ~fromBit) | myBit,
		                                        std::memory_order_acq_rel,
		                                        std::memory_order_relaxed)) {}
		destMask_.fetch_or(myBit, std::memory_order_release);   
		destMask_.fetch_and(~fromBit, std::memory_order_release);
		handoffFrom_.store(-1, std::memory_order_release);
		handoffs_.fetch_add(1, std::memory_order_relaxed);
		return from;
	}
	unsigned long long Handoffs() const noexcept { return handoffs_.load(std::memory_order_relaxed); }

	bool TryArm() noexcept {
		int expected = 0;
		return armed_.compare_exchange_strong(expected, 1, std::memory_order_acq_rel,
		                                      std::memory_order_relaxed);
	}
	bool SignalReady() noexcept {
		int expected = 0;
		return ready_.compare_exchange_strong(expected, 1, std::memory_order_acq_rel,
		                                      std::memory_order_relaxed);
	}
	bool Ready() const noexcept { return ready_.load(std::memory_order_acquire) != 0; }
	void ClearWave() noexcept {
		ready_.store(0, std::memory_order_release);
		armed_.store(0, std::memory_order_release);
	}

	void NoteAwake()  noexcept { awake_.fetch_add(1, std::memory_order_release); }
	void NoteAsleep() noexcept { awake_.fetch_sub(1, std::memory_order_release); }
	int  Awake() const noexcept { return awake_.load(std::memory_order_acquire); }
	bool MayReap() const noexcept { return awake_.load(std::memory_order_acquire) > kMinSeats; }

	void Wait(const std::atomic<bool>& alive) noexcept {
		for (;;) {
			int c = count_.load(std::memory_order_acquire);
			while (c > 0) {
				if (count_.compare_exchange_weak(c, c - 1, std::memory_order_acquire)) return;
			}
			if (!alive.load(std::memory_order_relaxed)) return;
			
			sleepers_.fetch_add(1, std::memory_order_release);
			c = count_.load(std::memory_order_acquire);
			if (c <= 0 && alive.load(std::memory_order_relaxed)) Block();
			sleepers_.fetch_sub(1, std::memory_order_release);
		}
	}

	int Release(int k) noexcept {
		if (k < 1) k = 1;
		count_.fetch_add(k, std::memory_order_release);
		const int parked = sleepers_.load(std::memory_order_acquire);
		const int n = (parked < k) ? parked : k;
		for (int i = 0; i < n; ++i) { WakeOne(); wakes_.fetch_add(1, std::memory_order_relaxed); }
		return k;
	}

	bool TryGrow(int k) noexcept {
		if (awake_.load(std::memory_order_acquire) > 0 && !SpendInterval()) return false;
		Release(k);
		return true;
	}

	void ReleaseAll(int n) noexcept {
		count_.fetch_add(n, std::memory_order_release);
		WakeAll();
	}

	int  Sleepers() const noexcept { return sleepers_.load(std::memory_order_acquire); }
	unsigned long long Wakes() const noexcept { return wakes_.load(std::memory_order_relaxed); }

private:
	static int TrailingZero(uint64_t v) noexcept {
#if defined(_MSC_VER)
		unsigned long b = 0; _BitScanForward64(&b, v); return (int)b;
#else
		return (int)__builtin_ctzll(v);
#endif
	}

	bool SpendInterval() noexcept {
		const long long iv = intervalNs_.load(std::memory_order_relaxed);
		if (iv <= 0) return true;
		const long long now = (long long)std::chrono::duration_cast<std::chrono::nanoseconds>(
			std::chrono::steady_clock::now().time_since_epoch()).count();
		long long last = lastGrowNs_.load(std::memory_order_acquire);
		if (now - last < iv) return false;
		return lastGrowNs_.compare_exchange_strong(last, now, std::memory_order_acq_rel,
		                                           std::memory_order_relaxed);
	}

#if defined(_WIN32)
	void Block()   noexcept { LONG e = 0; ::WaitOnAddress(&count_, &e, sizeof(LONG), INFINITE); }
	void WakeOne() noexcept { ::WakeByAddressSingle(&count_); }
	void WakeAll() noexcept { ::WakeByAddressAll(&count_); }
#elif defined(__linux__)
	void Block()   noexcept { ::syscall(SYS_futex, (int*)&count_, FUTEX_WAIT_PRIVATE, 0, nullptr, nullptr, 0); }
	void WakeOne() noexcept { ::syscall(SYS_futex, (int*)&count_, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0); }
	void WakeAll() noexcept { ::syscall(SYS_futex, (int*)&count_, FUTEX_WAKE_PRIVATE, INT32_MAX, nullptr, nullptr, 0); }
#else
	void Block()   noexcept { std::unique_lock<std::mutex> lk(mtx_); cv_.wait(lk, [&]{ return count_.load(std::memory_order_acquire) > 0; }); }
	void WakeOne() noexcept { std::lock_guard<std::mutex> lk(mtx_); cv_.notify_one(); }
	void WakeAll() noexcept { std::lock_guard<std::mutex> lk(mtx_); cv_.notify_all(); }
	std::mutex              mtx_;
	std::condition_variable cv_;
#endif

	std::atomic<int>       count_{ 0 };        
	std::atomic<int>       sleepers_{ 0 };
	std::atomic<int>       awake_{ 0 };
	std::atomic<uint64_t>  destMask_{ 0 };     
	std::atomic<uint64_t>  seatMask_{ 0 };     
	std::atomic<int>       armed_{ 0 };
	std::atomic<int>       ready_{ 0 };
	std::atomic<int>       wideInFlight_{ 0 };
	std::atomic<int>       handoffFrom_{ -1 };
	std::atomic<uint64_t>  advertised_{ 0 };
	std::atomic<unsigned long long> handoffs_{ 0 };
	std::atomic<long long> lastGrowNs_{ 0 };
	std::atomic<long long> intervalNs_{ 0 };
	std::atomic<unsigned>  cursor_{ 0 };
	std::atomic<unsigned long long> wakes_{ 0 };   
};

}   
