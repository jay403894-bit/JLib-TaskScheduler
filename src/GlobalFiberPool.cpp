// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/GlobalFiberPool.h"
#include "../include/Thread.h"

#include <algorithm>
#include <cstdio>

using namespace JLib;

namespace {
	constexpr size_t kMinBlock[3]  = { 64, 64, 8 };      // indexed by StackClass
	constexpr size_t kMaxBlockBytes = 64u * 1024 * 1024;

	const char* ClassName(StackClass c) {
		return c == StackClass::Tiny ? "tiny" : c == StackClass::Deep ? "deep" : "standard";
	}
}

GlobalFiberPool::GlobalFiberPool(size_t tinyCount, size_t standardCount, size_t deepCount,
                                 size_t limit)
	: memoryLimit(limit)
{
	const StackClass order[kClassCount] =
		{ StackClass::Standard, StackClass::Tiny, StackClass::Deep };
	const size_t counts[kClassCount] = { standardCount, tinyCount, deepCount };

	std::lock_guard<std::mutex> lock(poolMutex);
	for (size_t k = 0; k < kClassCount; ++k) {
		const StackClass cls = order[k];
		const size_t ci      = (size_t)cls;
		const size_t maxBlock = std::max(kMinBlock[ci], kMaxBlockBytes / RegionFor(cls));
		blockSize[ci] = std::min(std::max(counts[k], kMinBlock[ci]), maxBlock);

		if (counts[k] != 0 && !AddBlock(cls, counts[k]))
			throw std::runtime_error("Failed to allocate stack");
	}
}

static Fiber* AllocFiberStorage(size_t n) {
	return static_cast<Fiber*>(::operator new(n * sizeof(Fiber), std::align_val_t(alignof(Fiber))));
}
static void FreeFiberStorage(Fiber* p, size_t constructed) {
	for (size_t i = 0; i < constructed; ++i) p[i].~Fiber();
	::operator delete(p, std::align_val_t(alignof(Fiber)));
}

GlobalFiberPool::~GlobalFiberPool() {
	for (auto& list : blocks)
		for (Block& b : list) {
			FreeFiberStorage(b.fibers, b.count);
			delete b.arena;
		}
}

GlobalFiberPool* GlobalFiberPool::Create(size_t standardCount, size_t tinyCount, size_t deepCount,
                                         size_t memoryLimit)
{
	return new GlobalFiberPool(tinyCount, standardCount, deepCount, memoryLimit);
}

bool GlobalFiberPool::AddBlock(StackClass c, size_t count) {
	const size_t ci     = (size_t)c;
	const size_t region = RegionFor(c);

	Block b;
	try {
		b.arena  = new FiberStackArena(count * region);
		b.fibers = AllocFiberStorage(count);
		blocks[ci].reserve(blocks[ci].size() + 1);
	}
	catch (...) {
		if (b.fibers) ::operator delete(b.fibers, std::align_val_t(alignof(Fiber)));
		delete b.arena;
		return false;
	}

	size_t made = 0;
	for (; made < count; ++made) {
		void* stackMem = b.arena->AllocateStack(region);
		if (!stackMem) break;
		Fiber& f = *::new (&b.fibers[made]) Fiber();
		f.stackBase  = stackMem;
		f.stackSize  = region;
		f.stackClass = c;
		f.poolIndex  = nextIndex++;
		f.tsanFiber  = tsan::CreateFiber();
	}
	if (made == 0) {
		FreeFiberStorage(b.fibers, 0);
		delete b.arena;
		return false;
	}
	b.count = made;

	Fiber* first = b.fibers;
	blocks[ci].push_back(b);

	committedBytes.fetch_add(made * region, std::memory_order_relaxed);
	classCount[ci].fetch_add(made, std::memory_order_relaxed);
	size.fetch_add(made, std::memory_order_relaxed);

	Fiber* batch[64];
	for (size_t i = 0; i < made; ) {
		const size_t n = std::min<size_t>(64, made - i);
		for (size_t j = 0; j < n; ++j) batch[j] = first + i + j;
		availableFibers[ci].enqueue_bulk(batch, n);
		i += n;
	}
	return true;
}

bool GlobalFiberPool::Grow(StackClass c) {
	std::lock_guard<std::mutex> lock(poolMutex);
	const size_t ci = (size_t)c;
	if (availableFibers[ci].size_approx() != 0) return true;   // someone returned or grew meanwhile

	const size_t region = RegionFor(c);
	size_t n = blockSize[ci];
	if (memoryLimit) {
		const size_t used = committedBytes.load(std::memory_order_relaxed);
		const size_t room = used < memoryLimit ? (memoryLimit - used) / region : 0;
		n = std::min(n, room);
	}
	if (n == 0 || !AddBlock(c, n)) {
		JLIB_STAT(FiberPoolLimitHits);
		if (!warnedLimit[ci]) {
			warnedLimit[ci] = true;
			std::fprintf(stderr,
				"[JLib::Scheduler] %s fiber pool cannot grow: %zu fibers, %zu of %zu MB of fiber "
				"stack memory in use%s. Tasks that need a fiber are re-queued until a suspended task "
				"finishes. Raise Config::fiberMemoryLimit at Init, or block fewer "
				"tasks at once. This warning prints once per class.\n",
				ClassName(c), CountOf(c), CommittedBytes() >> 20, memoryLimit >> 20,
				n == 0 ? "" : " (the allocation failed)");
			std::fflush(stderr);
		}
		return false;
	}
	JLIB_STAT(FiberPoolGrowths);
	if (!notedGrowth[ci]) {
		notedGrowth[ci] = true;
		std::fprintf(stderr,
			"[JLib::Scheduler] NOTE: %s fiber pool grew by %zu fibers (now %zu). This is safe but "
			"costs an allocation; raise Config::fibers at Init to avoid it. Printed once "
			"per class.\n", ClassName(c), n, CountOf(c));
		std::fflush(stderr);
	}
	return true;
}

size_t GlobalFiberPool::StealInto(Fiber** dest, size_t maxCount, StackClass c) {
	if (maxCount == 0) return 0;
	auto& q = availableFibers[(size_t)c];
	if (size_t got = q.try_dequeue_bulk(dest, maxCount)) return got;
	Grow(c);
	return q.try_dequeue_bulk(dest, maxCount);
}

std::vector<Fiber*> GlobalFiberPool::StealBatch(size_t count, StackClass c)
{
	std::vector<Fiber*> batch(count);
	const size_t got = StealInto(batch.data(), count, c);
	batch.resize(got);
	return batch;
}

void GlobalFiberPool::ReturnBatch(Fiber** fibers, size_t count) {
	if (count == 0) return;

	Fiber* grouped[kClassCount][64];
	size_t n[kClassCount] = {};
	for (size_t i = 0; i < count; ++i) {
		Fiber* f = fibers[i];
		f->ResetForReuse();
		const size_t ci = (size_t)f->stackClass;
		grouped[ci][n[ci]++] = f;
		if (n[ci] == 64) { availableFibers[ci].enqueue_bulk(grouped[ci], 64); n[ci] = 0; }
	}
	for (size_t ci = 0; ci < kClassCount; ++ci)
		if (n[ci]) availableFibers[ci].enqueue_bulk(grouped[ci], n[ci]);
}

void GlobalFiberPool::FiberEntryWrapper()
{
	Fiber* self = Thread::GetCurrent()->currentFiber;
	Task*  task = Thread::GetCurrent()->currentRunningTask;

	if (self && task) {
		task->Execute();
	}

	self->status.store(FiberStatus::DEAD, std::memory_order_release);

	JLIB_EPOCH_CHECK_NO_GUARD_AT_EXIT("fiber exit (task returned)");

	Thread::TsanSwitchToScheduler();
	ContextSwitch(&self->ctx, self->homeCtx);
}

size_t GlobalFiberPool::AvailableCount() const
{
	size_t n = 0;
	for (size_t ci = 0; ci < kClassCount; ++ci) n += availableFibers[ci].size_approx();
	return n;
}
