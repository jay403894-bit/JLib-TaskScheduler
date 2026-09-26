
#pragma once

#include <atomic>
#include <cassert> 
#include <cerrno> 
#include <cstddef> 
#include <cstdint> 
#include <ctime> 
#include <type_traits> 

#if defined(_WIN32)

extern "C" {
	struct _SECURITY_ATTRIBUTES;
	__declspec(dllimport) void* __stdcall CreateSemaphoreW(_SECURITY_ATTRIBUTES* lpSemaphoreAttributes, long lInitialCount, long lMaximumCount, const wchar_t* lpName);
	__declspec(dllimport) int __stdcall CloseHandle(void* hObject);
	__declspec(dllimport) unsigned long __stdcall WaitForSingleObject(void* hHandle, unsigned long dwMilliseconds);
	__declspec(dllimport) int __stdcall ReleaseSemaphore(void* hSemaphore, long lReleaseCount, long* lpPreviousCount);
}
#elif defined(__MACH__)
#include <mach/mach.h>
#elif defined(__MVS__)
#include <zos-semaphore.h>
#elif defined(__unix__)
#include <semaphore.h>

#if defined(__GLIBC_PREREQ) && defined(_GNU_SOURCE)
#if __GLIBC_PREREQ(2,30)
#define MOODYCAMEL_LIGHTWEIGHTSEMAPHORE_MONOTONIC
#endif
#endif
#endif

#ifndef MOODYCAMEL_DELETE_FUNCTION
#if __cplusplus >= 201103L || _MSC_VER >= 1900
#define MOODYCAMEL_DELETE_FUNCTION = delete
#else
#define MOODYCAMEL_DELETE_FUNCTION
#endif
#endif

namespace moodycamel
{
namespace details
{

// Copyright (c) 2015 Jeff Preshing

#if defined(_WIN32)
class Semaphore
{
private:
	void* m_hSema;
	
	Semaphore(const Semaphore& other) MOODYCAMEL_DELETE_FUNCTION;
	Semaphore& operator=(const Semaphore& other) MOODYCAMEL_DELETE_FUNCTION;

public:
	Semaphore(int initialCount = 0)
	{
		assert(initialCount >= 0);
		const long maxLong = 0x7fffffff;
		m_hSema = CreateSemaphoreW(nullptr, initialCount, maxLong, nullptr);
		assert(m_hSema);
	}

	~Semaphore()
	{
		CloseHandle(m_hSema);
	}

	bool wait()
	{
		const unsigned long infinite = 0xffffffff;
		return WaitForSingleObject(m_hSema, infinite) == 0;
	}
	
	bool try_wait()
	{
		return WaitForSingleObject(m_hSema, 0) == 0;
	}
	
	bool timed_wait(std::uint64_t usecs)
	{
		return WaitForSingleObject(m_hSema, (unsigned long)(usecs / 1000)) == 0;
	}

	void signal(int count = 1)
	{
		while (!ReleaseSemaphore(m_hSema, count, nullptr));
	}
};
#elif defined(__MACH__)

class Semaphore
{
private:
	semaphore_t m_sema;

	Semaphore(const Semaphore& other) MOODYCAMEL_DELETE_FUNCTION;
	Semaphore& operator=(const Semaphore& other) MOODYCAMEL_DELETE_FUNCTION;

public:
	Semaphore(int initialCount = 0)
	{
		assert(initialCount >= 0);
		kern_return_t rc = semaphore_create(mach_task_self(), &m_sema, SYNC_POLICY_FIFO, initialCount);
		assert(rc == KERN_SUCCESS);
		(void)rc;
	}

	~Semaphore()
	{
		semaphore_destroy(mach_task_self(), m_sema);
	}

	bool wait()
	{
		return semaphore_wait(m_sema) == KERN_SUCCESS;
	}
	
	bool try_wait()
	{
		return timed_wait(0);
	}
	
	bool timed_wait(std::uint64_t timeout_usecs)
	{
		mach_timespec_t ts;
		ts.tv_sec = static_cast<unsigned int>(timeout_usecs / 1000000);
		ts.tv_nsec = static_cast<int>((timeout_usecs % 1000000) * 1000);

		kern_return_t rc = semaphore_timedwait(m_sema, ts);
		return rc == KERN_SUCCESS;
	}

	void signal()
	{
		while (semaphore_signal(m_sema) != KERN_SUCCESS);
	}

	void signal(int count)
	{
		while (count-- > 0)
		{
			while (semaphore_signal(m_sema) != KERN_SUCCESS);
		}
	}
};
#elif defined(__unix__) || defined(__MVS__)

class Semaphore
{
private:
	sem_t m_sema;

	Semaphore(const Semaphore& other) MOODYCAMEL_DELETE_FUNCTION;
	Semaphore& operator=(const Semaphore& other) MOODYCAMEL_DELETE_FUNCTION;

public:
	Semaphore(int initialCount = 0)
	{
		assert(initialCount >= 0);
		int rc = sem_init(&m_sema, 0, static_cast<unsigned int>(initialCount));
		assert(rc == 0);
		(void)rc;
	}

	~Semaphore()
	{
		sem_destroy(&m_sema);
	}

	bool wait()
	{
		
		int rc;
		do {
			rc = sem_wait(&m_sema);
		} while (rc == -1 && errno == EINTR);
		return rc == 0;
	}

	bool try_wait()
	{
		int rc;
		do {
			rc = sem_trywait(&m_sema);
		} while (rc == -1 && errno == EINTR);
		return rc == 0;
	}

	bool timed_wait(std::uint64_t usecs)
	{
		struct timespec ts;
		const int usecs_in_1_sec = 1000000;
		const int nsecs_in_1_sec = 1000000000;
#ifdef MOODYCAMEL_LIGHTWEIGHTSEMAPHORE_MONOTONIC
		clock_gettime(CLOCK_MONOTONIC, &ts);
#else
		clock_gettime(CLOCK_REALTIME, &ts);
#endif
		ts.tv_sec += static_cast<time_t>(usecs / usecs_in_1_sec);
		ts.tv_nsec += static_cast<long>(usecs % usecs_in_1_sec) * 1000;
		
		if (ts.tv_nsec >= nsecs_in_1_sec) {
			ts.tv_nsec -= nsecs_in_1_sec;
			++ts.tv_sec;
		}

		int rc;
		do {
#ifdef MOODYCAMEL_LIGHTWEIGHTSEMAPHORE_MONOTONIC
			rc = sem_clockwait(&m_sema, CLOCK_MONOTONIC, &ts);
#else
			rc = sem_timedwait(&m_sema, &ts);
#endif
		} while (rc == -1 && errno == EINTR);
		return rc == 0;
	}

	void signal()
	{
		while (sem_post(&m_sema) == -1);
	}

	void signal(int count)
	{
		while (count-- > 0)
		{
			while (sem_post(&m_sema) == -1);
		}
	}
};
#else
#error Unsupported platform! (No semaphore wrapper available)
#endif

}	

class LightweightSemaphore
{
public:
	typedef std::make_signed<std::size_t>::type ssize_t;

private:
	std::atomic<ssize_t> m_count;
	details::Semaphore m_sema;
	int m_maxSpins;

	bool waitWithPartialSpinning(std::int64_t timeout_usecs = -1)
	{
		ssize_t oldCount;
		int spin = m_maxSpins;
		while (--spin >= 0)
		{
			oldCount = m_count.load(std::memory_order_relaxed);
			if ((oldCount > 0) && m_count.compare_exchange_strong(oldCount, oldCount - 1, std::memory_order_acquire, std::memory_order_relaxed))
				return true;
			std::atomic_signal_fence(std::memory_order_acquire);	 
		}
		oldCount = m_count.fetch_sub(1, std::memory_order_acquire);
		if (oldCount > 0)
			return true;
		if (timeout_usecs < 0)
		{
			if (m_sema.wait())
				return true;
		}
		if (timeout_usecs > 0 && m_sema.timed_wait(static_cast<std::uint64_t>(timeout_usecs)))
			return true;
		
		while (true)
		{
			oldCount = m_count.load(std::memory_order_acquire);
			if (oldCount >= 0 && m_sema.try_wait())
				return true;
			if (oldCount < 0 && m_count.compare_exchange_strong(oldCount, oldCount + 1, std::memory_order_relaxed, std::memory_order_relaxed))
				return false;
		}
	}

	ssize_t waitManyWithPartialSpinning(ssize_t max, std::int64_t timeout_usecs = -1)
	{
		assert(max > 0);
		ssize_t oldCount;
		int spin = m_maxSpins;
		while (--spin >= 0)
		{
			oldCount = m_count.load(std::memory_order_relaxed);
			if (oldCount > 0)
			{
				ssize_t newCount = oldCount > max ? oldCount - max : 0;
				if (m_count.compare_exchange_strong(oldCount, newCount, std::memory_order_acquire, std::memory_order_relaxed))
					return oldCount - newCount;
			}
			std::atomic_signal_fence(std::memory_order_acquire);
		}
		oldCount = m_count.fetch_sub(1, std::memory_order_acquire);
		if (oldCount <= 0)
		{
			if ((timeout_usecs == 0) || (timeout_usecs < 0 && !m_sema.wait()) || (timeout_usecs > 0 && !m_sema.timed_wait(static_cast<std::uint64_t>(timeout_usecs))))
			{
				while (true)
				{
					oldCount = m_count.load(std::memory_order_acquire);
					if (oldCount >= 0 && m_sema.try_wait())
						break;
					if (oldCount < 0 && m_count.compare_exchange_strong(oldCount, oldCount + 1, std::memory_order_relaxed, std::memory_order_relaxed))
						return 0;
				}
			}
		}
		if (max > 1)
			return 1 + tryWaitMany(max - 1);
		return 1;
	}

public:
	LightweightSemaphore(ssize_t initialCount = 0, int maxSpins = 10000) : m_count(initialCount), m_maxSpins(maxSpins)
	{
		assert(initialCount >= 0);
		assert(maxSpins >= 0);
	}

	bool tryWait()
	{
		ssize_t oldCount = m_count.load(std::memory_order_relaxed);
		while (oldCount > 0)
		{
			if (m_count.compare_exchange_weak(oldCount, oldCount - 1, std::memory_order_acquire, std::memory_order_relaxed))
				return true;
		}
		return false;
	}

	bool wait()
	{
		return tryWait() || waitWithPartialSpinning();
	}

	bool wait(std::int64_t timeout_usecs)
	{
		return tryWait() || waitWithPartialSpinning(timeout_usecs);
	}

	ssize_t tryWaitMany(ssize_t max)
	{
		assert(max >= 0);
		ssize_t oldCount = m_count.load(std::memory_order_relaxed);
		while (oldCount > 0)
		{
			ssize_t newCount = oldCount > max ? oldCount - max : 0;
			if (m_count.compare_exchange_weak(oldCount, newCount, std::memory_order_acquire, std::memory_order_relaxed))
				return oldCount - newCount;
		}
		return 0;
	}

	ssize_t waitMany(ssize_t max, std::int64_t timeout_usecs)
	{
		assert(max >= 0);
		ssize_t result = tryWaitMany(max);
		if (result == 0 && max > 0)
			result = waitManyWithPartialSpinning(max, timeout_usecs);
		return result;
	}
	
	ssize_t waitMany(ssize_t max)
	{
		ssize_t result = waitMany(max, -1);
		assert(result > 0);
		return result;
	}

	void signal(ssize_t count = 1)
	{
		assert(count >= 0);
		ssize_t oldCount = m_count.fetch_add(count, std::memory_order_release);
		ssize_t toRelease = -oldCount < count ? -oldCount : count;
		if (toRelease > 0)
		{
			m_sema.signal(static_cast<int>(toRelease));
		}
	}
	
	std::size_t availableApprox() const
	{
		ssize_t count = m_count.load(std::memory_order_relaxed);
		return count > 0 ? static_cast<std::size_t>(count) : 0;
	}
};

}   
