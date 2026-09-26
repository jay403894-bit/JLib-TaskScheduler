
#pragma once

#include "concurrentqueue.h"
#include "lightweightsemaphore.h"

#include <type_traits>
#include <cerrno>
#include <memory>
#include <chrono>
#include <ctime>

namespace moodycamel
{

template<typename T, typename Traits = ConcurrentQueueDefaultTraits>
class BlockingConcurrentQueue
{
private:
	typedef ::moodycamel::ConcurrentQueue<T, Traits> ConcurrentQueue;
	typedef ::moodycamel::LightweightSemaphore LightweightSemaphore;

public:
	typedef typename ConcurrentQueue::producer_token_t producer_token_t;
	typedef typename ConcurrentQueue::consumer_token_t consumer_token_t;
	
	typedef typename ConcurrentQueue::index_t index_t;
	typedef typename ConcurrentQueue::size_t size_t;
	typedef typename std::make_signed<size_t>::type ssize_t;
	
	static const size_t BLOCK_SIZE = ConcurrentQueue::BLOCK_SIZE;
	static const size_t EXPLICIT_BLOCK_EMPTY_COUNTER_THRESHOLD = ConcurrentQueue::EXPLICIT_BLOCK_EMPTY_COUNTER_THRESHOLD;
	static const size_t EXPLICIT_INITIAL_INDEX_SIZE = ConcurrentQueue::EXPLICIT_INITIAL_INDEX_SIZE;
	static const size_t IMPLICIT_INITIAL_INDEX_SIZE = ConcurrentQueue::IMPLICIT_INITIAL_INDEX_SIZE;
	static const size_t INITIAL_IMPLICIT_PRODUCER_HASH_SIZE = ConcurrentQueue::INITIAL_IMPLICIT_PRODUCER_HASH_SIZE;
	static const std::uint32_t EXPLICIT_CONSUMER_CONSUMPTION_QUOTA_BEFORE_ROTATE = ConcurrentQueue::EXPLICIT_CONSUMER_CONSUMPTION_QUOTA_BEFORE_ROTATE;
	static const size_t MAX_SUBQUEUE_SIZE = ConcurrentQueue::MAX_SUBQUEUE_SIZE;
	
public:
	
	explicit BlockingConcurrentQueue(size_t capacity = 6 * BLOCK_SIZE)
		: inner(capacity), sema(create<LightweightSemaphore, ssize_t, int>(0, static_cast<int>(Traits::MAX_SEMA_SPINS)), &BlockingConcurrentQueue::template destroy<LightweightSemaphore>)
	{
		assert(reinterpret_cast<ConcurrentQueue*>(reinterpret_cast<BlockingConcurrentQueue*>(1)) == &(reinterpret_cast<BlockingConcurrentQueue*>(1))->inner && "BlockingConcurrentQueue must have ConcurrentQueue as its first member");
		if (!sema) {
			MOODYCAMEL_THROW(std::bad_alloc());
		}
	}
	
	BlockingConcurrentQueue(size_t minCapacity, size_t maxExplicitProducers, size_t maxImplicitProducers)
		: inner(minCapacity, maxExplicitProducers, maxImplicitProducers), sema(create<LightweightSemaphore, ssize_t, int>(0, static_cast<int>(Traits::MAX_SEMA_SPINS)), &BlockingConcurrentQueue::template destroy<LightweightSemaphore>)
	{
		assert(reinterpret_cast<ConcurrentQueue*>(reinterpret_cast<BlockingConcurrentQueue*>(1)) == &(reinterpret_cast<BlockingConcurrentQueue*>(1))->inner && "BlockingConcurrentQueue must have ConcurrentQueue as its first member");
		if (!sema) {
			MOODYCAMEL_THROW(std::bad_alloc());
		}
	}
	
	BlockingConcurrentQueue(BlockingConcurrentQueue const&) MOODYCAMEL_DELETE_FUNCTION;
	BlockingConcurrentQueue& operator=(BlockingConcurrentQueue const&) MOODYCAMEL_DELETE_FUNCTION;
	
	BlockingConcurrentQueue(BlockingConcurrentQueue&& other) MOODYCAMEL_NOEXCEPT
		: inner(std::move(other.inner)), sema(std::move(other.sema))
	{ }
	
	inline BlockingConcurrentQueue& operator=(BlockingConcurrentQueue&& other) MOODYCAMEL_NOEXCEPT
	{
		return swap_internal(other);
	}
	
	inline void swap(BlockingConcurrentQueue& other) MOODYCAMEL_NOEXCEPT
	{
		swap_internal(other);
	}
	
private:
	BlockingConcurrentQueue& swap_internal(BlockingConcurrentQueue& other)
	{
		if (this == &other) {
			return *this;
		}
		
		inner.swap(other.inner);
		sema.swap(other.sema);
		return *this;
	}
	
public:
	
	inline bool enqueue(T const& item)
	{
		if ((details::likely)(inner.enqueue(item))) {
			sema->signal();
			return true;
		}
		return false;
	}
	
	inline bool enqueue(T&& item)
	{
		if ((details::likely)(inner.enqueue(std::move(item)))) {
			sema->signal();
			return true;
		}
		return false;
	}
	
	inline bool enqueue(producer_token_t const& token, T const& item)
	{
		if ((details::likely)(inner.enqueue(token, item))) {
			sema->signal();
			return true;
		}
		return false;
	}
	
	inline bool enqueue(producer_token_t const& token, T&& item)
	{
		if ((details::likely)(inner.enqueue(token, std::move(item)))) {
			sema->signal();
			return true;
		}
		return false;
	}
	
	template<typename It>
	inline bool enqueue_bulk(It itemFirst, size_t count)
	{
		if ((details::likely)(inner.enqueue_bulk(std::forward<It>(itemFirst), count))) {
			assert(static_cast<ssize_t>(count) >= 0);
			sema->signal(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(count)));
			return true;
		}
		return false;
	}

	template<typename It>
	inline bool enqueue_bulk(producer_token_t const& token, It itemFirst, size_t count)
	{
		if ((details::likely)(inner.enqueue_bulk(token, std::forward<It>(itemFirst), count))) {
			assert(static_cast<ssize_t>(count) >= 0);
			sema->signal(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(count)));
			return true;
		}
		return false;
	}
	
	inline bool try_enqueue(T const& item)
	{
		if (inner.try_enqueue(item)) {
			sema->signal();
			return true;
		}
		return false;
	}
	
	inline bool try_enqueue(T&& item)
	{
		if (inner.try_enqueue(std::move(item))) {
			sema->signal();
			return true;
		}
		return false;
	}
	
	inline bool try_enqueue(producer_token_t const& token, T const& item)
	{
		if (inner.try_enqueue(token, item)) {
			sema->signal();
			return true;
		}
		return false;
	}
	
	inline bool try_enqueue(producer_token_t const& token, T&& item)
	{
		if (inner.try_enqueue(token, std::move(item))) {
			sema->signal();
			return true;
		}
		return false;
	}
	
	template<typename It>
	inline bool try_enqueue_bulk(It itemFirst, size_t count)
	{
		if (inner.try_enqueue_bulk(std::forward<It>(itemFirst), count)) {
			assert(static_cast<ssize_t>(count) >= 0);
			sema->signal(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(count)));
			return true;
		}
		return false;
	}

	template<typename It>
	inline bool try_enqueue_bulk(producer_token_t const& token, It itemFirst, size_t count)
	{
		if (inner.try_enqueue_bulk(token, std::forward<It>(itemFirst), count)) {
			assert(static_cast<ssize_t>(count) >= 0);
			sema->signal(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(count)));
			return true;
		}
		return false;
	}
	
	template<typename U>
	inline bool try_dequeue(U& item)
	{
		if (sema->tryWait()) {
			while (!inner.try_dequeue(item)) {
				continue;
			}
			return true;
		}
		return false;
	}
	
	template<typename U>
	inline bool try_dequeue(consumer_token_t& token, U& item)
	{
		if (sema->tryWait()) {
			while (!inner.try_dequeue(token, item)) {
				continue;
			}
			return true;
		}
		return false;
	}
	
	template<typename It>
	inline size_t try_dequeue_bulk(It itemFirst, size_t max)
	{
		size_t count = 0;
		assert(static_cast<ssize_t>(max) >= 0);
		max = static_cast<size_t>(sema->tryWaitMany(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(max))));
		while (count != max) {
			count += inner.template try_dequeue_bulk<It&>(itemFirst, max - count);
		}
		return count;
	}

	template<typename It>
	inline size_t try_dequeue_bulk(consumer_token_t& token, It itemFirst, size_t max)
	{
		size_t count = 0;
		assert(static_cast<ssize_t>(max) >= 0);
		max = static_cast<size_t>(sema->tryWaitMany(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(max))));
		while (count != max) {
			count += inner.template try_dequeue_bulk<It&>(token, itemFirst, max - count);
		}
		return count;
	}
	
	template<typename U>
	inline void wait_dequeue(U& item)
	{
		while (!sema->wait()) {
			continue;
		}
		while (!inner.try_dequeue(item)) {
			continue;
		}
	}

	template<typename U>
	inline bool wait_dequeue_timed(U& item, std::int64_t timeout_usecs)
	{
		if (!sema->wait(timeout_usecs)) {
			return false;
		}
		while (!inner.try_dequeue(item)) {
			continue;
		}
		return true;
	}
    
	template<typename U, typename Rep, typename Period>
	inline bool wait_dequeue_timed(U& item, std::chrono::duration<Rep, Period> const& timeout)
    {
        return wait_dequeue_timed(item, std::chrono::duration_cast<std::chrono::microseconds>(timeout).count());
    }
	
	template<typename U>
	inline void wait_dequeue(consumer_token_t& token, U& item)
	{
		while (!sema->wait()) {
			continue;
		}
		while (!inner.try_dequeue(token, item)) {
			continue;
		}
	}
	
	template<typename U>
	inline bool wait_dequeue_timed(consumer_token_t& token, U& item, std::int64_t timeout_usecs)
	{
		if (!sema->wait(timeout_usecs)) {
			return false;
		}
		while (!inner.try_dequeue(token, item)) {
			continue;
		}
		return true;
	}
    
	template<typename U, typename Rep, typename Period>
	inline bool wait_dequeue_timed(consumer_token_t& token, U& item, std::chrono::duration<Rep, Period> const& timeout)
    {
        return wait_dequeue_timed(token, item, std::chrono::duration_cast<std::chrono::microseconds>(timeout).count());
    }
	
	template<typename It>
	inline size_t wait_dequeue_bulk(It itemFirst, size_t max)
	{
		size_t count = 0;
		assert(static_cast<ssize_t>(max) >= 0);
		max = static_cast<size_t>(sema->waitMany(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(max))));
		while (count != max) {
			count += inner.template try_dequeue_bulk<It&>(itemFirst, max - count);
		}
		return count;
	}

	template<typename It>
	inline size_t wait_dequeue_bulk_timed(It itemFirst, size_t max, std::int64_t timeout_usecs)
	{
		size_t count = 0;
		assert(static_cast<ssize_t>(max) >= 0);
		max = static_cast<size_t>(sema->waitMany(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(max)), timeout_usecs));
		while (count != max) {
			count += inner.template try_dequeue_bulk<It&>(itemFirst, max - count);
		}
		return count;
	}
    
	template<typename It, typename Rep, typename Period>
	inline size_t wait_dequeue_bulk_timed(It itemFirst, size_t max, std::chrono::duration<Rep, Period> const& timeout)
    {
        return wait_dequeue_bulk_timed<It&>(itemFirst, max, std::chrono::duration_cast<std::chrono::microseconds>(timeout).count());
    }
	
	template<typename It>
	inline size_t wait_dequeue_bulk(consumer_token_t& token, It itemFirst, size_t max)
	{
		size_t count = 0;
		assert(static_cast<ssize_t>(max) >= 0);
		max = static_cast<size_t>(sema->waitMany(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(max))));
		while (count != max) {
			count += inner.template try_dequeue_bulk<It&>(token, itemFirst, max - count);
		}
		return count;
	}
	
	template<typename It>
	inline size_t wait_dequeue_bulk_timed(consumer_token_t& token, It itemFirst, size_t max, std::int64_t timeout_usecs)
	{
		size_t count = 0;
		assert(static_cast<ssize_t>(max) >= 0);
		max = static_cast<size_t>(sema->waitMany(static_cast<LightweightSemaphore::ssize_t>(static_cast<ssize_t>(max)), timeout_usecs));
		while (count != max) {
			count += inner.template try_dequeue_bulk<It&>(token, itemFirst, max - count);
		}
		return count;
	}
	
	template<typename It, typename Rep, typename Period>
	inline size_t wait_dequeue_bulk_timed(consumer_token_t& token, It itemFirst, size_t max, std::chrono::duration<Rep, Period> const& timeout)
    {
        return wait_dequeue_bulk_timed<It&>(token, itemFirst, max, std::chrono::duration_cast<std::chrono::microseconds>(timeout).count());
    }
	
	inline size_t size_approx() const
	{
		return static_cast<size_t>(sema->availableApprox());
	}
	
	static constexpr bool is_lock_free()
	{
		return ConcurrentQueue::is_lock_free();
	}
	
private:
	template<typename U, typename A1, typename A2>
	static inline U* create(A1&& a1, A2&& a2)
	{
		void* p = (Traits::malloc)(sizeof(U));
		return p != nullptr ? new (p) U(std::forward<A1>(a1), std::forward<A2>(a2)) : nullptr;
	}
	
	template<typename U>
	static inline void destroy(U* p)
	{
		if (p != nullptr) {
			p->~U();
		}
		(Traits::free)(p);
	}
	
private:
	ConcurrentQueue inner;
	std::unique_ptr<LightweightSemaphore, void (*)(LightweightSemaphore*)> sema;
};

template<typename T, typename Traits>
inline void swap(BlockingConcurrentQueue<T, Traits>& a, BlockingConcurrentQueue<T, Traits>& b) MOODYCAMEL_NOEXCEPT
{
	a.swap(b);
}

}	
