// Counts heap allocations inside TaskScheduler::ParallelFor, with the counter from
// waitalloc_test. The question is what the count scales WITH: the number of chunks, the number of
// items, the number of lanes, or just the call. Same counter discipline -- a control phase that is
// known to allocate runs first, so a low number cannot mean a dead hook.
#include <TaskScheduler.h>
#include <Thread.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <new>
#include <vector>

using namespace JLib;

static std::atomic<bool>      g_on{ false };
static std::atomic<long long> g_allocs{ 0 };
static std::atomic<long long> g_bytes{ 0 };

// Which sizes, not just how many: that is what says whether an allocator would have anything to do.
static constexpr int kSizeSlots = 24;
static std::atomic<size_t>    g_sizeKey[kSizeSlots];
static std::atomic<long long> g_sizeCnt[kSizeSlots];

static void RecordSize(size_t n) {
	for (int i = 0; i < kSizeSlots; ++i) {
		size_t k = g_sizeKey[i].load(std::memory_order_relaxed);
		if (k == n) { g_sizeCnt[i].fetch_add(1, std::memory_order_relaxed); return; }
		if (k == 0) {
			size_t expect = 0;
			if (g_sizeKey[i].compare_exchange_strong(expect, n, std::memory_order_relaxed)) {
				g_sizeCnt[i].fetch_add(1, std::memory_order_relaxed);
				return;
			}
			--i;   // someone claimed it first; re-read this slot
		}
	}
}

void* operator new(size_t n) {
	if (g_on.load(std::memory_order_relaxed)) {
		g_allocs.fetch_add(1, std::memory_order_relaxed);
		g_bytes.fetch_add((long long)n, std::memory_order_relaxed);
		RecordSize(n);
	}
	void* p = std::malloc(n ? n : 1);
	if (!p) throw std::bad_alloc();
	return p;
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }
void* operator new[](size_t n) { return operator new(n); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }

static std::vector<unsigned> g_data;
static std::atomic<long long> g_chunks{ 0 };

// Enough work per item that the width probe does not fold the range back to serial.
static inline void Body(int lo, int hi) {
	unsigned acc = 0;
	for (int i = lo; i < hi; ++i) {
		unsigned x = g_data[(size_t)i];
		for (int k = 0; k < 64; ++k) x = x * 1664525u + 1013904223u;
		acc ^= x;
	}
	g_data[(size_t)lo] ^= acc;
	g_chunks.fetch_add(1, std::memory_order_relaxed);
}

static volatile void* g_sink;
static void DoNotElide(void* p) { g_sink = p; }

struct Row { const char* label; long long calls, chunks, allocs, bytes; };
static std::vector<Row> g_rows;

static void Measure(const char* label, int calls, int n, int grain) {
	auto& s = TaskScheduler::Instance();
	std::printf("  [%s]\n", label);
	g_chunks = 0; g_allocs = 0; g_bytes = 0;
	g_on = true;
	for (int c = 0; c < calls; ++c) {
		if (grain > 0) s.ParallelFor(0, n, grain, Body);
		else           s.ParallelFor(0, n, Body);
	}
	g_on = false;
	g_rows.push_back(Row{ label, calls, g_chunks.load(), g_allocs.load(), g_bytes.load() });
}

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IONBF, 0);
	TaskScheduler::Init(Mode::Migrate, MainMode::OutOfPool, 8);
	auto& s = TaskScheduler::Instance();
	const size_t W = s.GetWorkerCount();
	constexpr int kN = 65536;
	g_data.assign((size_t)kN * 4, 12345u);   // the "4x items" case indexes past kN
	std::printf("pforalloc_test workers=%zu items=%d\n", W, kN);

	// Warm the slab, the body-cost slot and the width probe before anything is counted.
	for (int i = 0; i < 8; ++i) s.ParallelFor(0, kN, 256, Body);

	{
		g_allocs = 0; g_bytes = 0; g_on = true;
		// A direct call, not a new-expression: an unobserved new/delete pair may be removed.
		static void* volatile sink = nullptr;
		sink = ::operator new(1000);
		::operator delete(sink);
		g_on = false;
		std::printf("control: %lld allocations, %lld bytes (counter is live: %s)\n\n",
			g_allocs.load(), g_bytes.load(), g_allocs.load() > 0 ? "yes" : "NO");
	}

	// Same items, same call count, 32x the chunks: does the count follow the chunks?
	Measure("grain 4096  (16 chunks)",  100, kN, 4096);
	Measure("grain 1024  (64 chunks)",  100, kN, 1024);
	Measure("grain 256   (256 chunks)", 100, kN, 256);
	Measure("grain 64    (1024 chunks)",100, kN, 64);
	Measure("grain 16    (4096 chunks)",100, kN, 16);
	Measure("default grain",            100, kN, 0);
	// Same chunk size, 4x the items.
	Measure("grain 256, 4x items",      100, kN * 4, 256);
	// 10x the calls at a fixed shape.
	Measure("grain 256, 10x calls",    1000, kN, 256);

	std::printf("  %-26s %6s %8s %8s %10s %10s %10s\n",
		"case", "calls", "chunks", "allocs", "per call", "per chunk", "bytes/call");
	for (const Row& r : g_rows) {
		std::printf("  %-26s %6lld %8lld %8lld %10.2f %10.4f %10.1f\n",
			r.label, r.calls, r.chunks, r.allocs,
			(double)r.allocs / (double)r.calls,
			(double)r.allocs / (double)(r.chunks ? r.chunks : 1),
			(double)r.bytes / (double)r.calls);
	}

	// The lanes are what the count should track, if anything does.
	std::printf("\n  lanes are capped at workers = %zu\n", W);

	// --- what is the per-call allocation, exactly? ---------------------------------------------
	// Three suspects, each isolated. ParallelFor(0,0,...) returns before any pool work, so it costs
	// only the std::function parameter. A bare WaitGroup costs whatever its members cost. A lambda
	// task costs whatever CreateInternalTask does when the slab is warm.
	{
		g_allocs = 0; g_bytes = 0; g_on = true;
		for (int i = 0; i < 100; ++i) s.ParallelFor(0, 0, Body);   // returns at once
		g_on = false;
		std::printf("\n  the std::function parameter alone, x100: %lld allocations, %lld bytes\n",
			g_allocs.load(), g_bytes.load());
	}
	{
		g_allocs = 0; g_bytes = 0; g_on = true;
		for (int i = 0; i < 100; ++i) { WaitGroup wg; DoNotElide(&wg); }
		g_on = false;
		std::printf("  a bare WaitGroup, x100:                  %lld allocations, %lld bytes\n",
			g_allocs.load(), g_bytes.load());
	}
	{
		std::atomic<int> cursor{ 0 };
		std::function<void(int, int)> f = Body;
		g_allocs = 0; g_bytes = 0; g_on = true;
		for (int i = 0; i < 100; ++i) {
			Task* t = s.CreateInternalTask([cur = &cursor, ff = &f, end = 0, grain = 1]() {
				(void)cur; (void)ff; (void)end; (void)grain;
			});
			if (t) s.FreeTask(t);
		}
		g_on = false;
		std::printf("  the lane lambda task, x100:              %lld allocations, %lld bytes\n",
			g_allocs.load(), g_bytes.load());
	}

	std::printf("\n  allocation sizes seen across every measured phase:\n");
	for (int i = 0; i < kSizeSlots; ++i) {
		const size_t k = g_sizeKey[i].load();
		if (!k) break;
		std::printf("    %6zu bytes  x %lld\n", k, g_sizeCnt[i].load());
	}
	std::_Exit(0);
}
