// A coroutine frame that outlives its pool is destroyed and freed safely.
//
// Frames come from the pool's slab. A Coro that was never spawned still owns its frame, and here it
// is destroyed only after the pool itself is gone. Destroying it reads the frame; freeing it must
// not hand the slot to anyone. Run under AddressSanitizer this is the real check: with the slab's
// pages kept mapped it is clean, and with -DJLIB_SLAB_CTL_UNMAP_ON_DESTROY it is a use-after-free.
#include <TaskScheduler.h>
#include <Coroutine.h>
#include <cstdio>
#include <optional>
#include <vector>

using namespace JLib;

static int g_bodies = 0;

// Big enough to land in the 512 class once its header is added: the case a restart exposes.
static Coro Unspawned(int k) {
	volatile char pad[200] = {};
	pad[k % 200] = 1;
	++g_bodies;   // never runs: the coroutine is never resumed
	co_return;
}

static Coro Small() { ++g_bodies; co_return; }

int main() {
	std::setvbuf(stdout, nullptr, _IONBF, 0);

	std::vector<Coro> survivors;
	{
		TaskScheduler::Config cfg;
		cfg.workers = 2;
		cfg.main    = MainMode::OutOfPool;
		TaskScheduler::Init(cfg);
		for (int i = 0; i < 64; ++i) survivors.push_back(Unspawned(i));
		for (int i = 0; i < 64; ++i) survivors.push_back(Small());
		detail::DestroyForTesting();   // the pool, and its allocator, are gone
	}

	survivors.clear();   // 128 frames destroyed and freed with no pool running
	std::printf("  frames destroyed after their pool: 128, bodies run: %d\n", g_bodies);

	// And a new pool is unaffected by the old frames.
	{
		TaskScheduler::Config cfg;
		cfg.workers = 2;
		cfg.main    = MainMode::OutOfPool;
		TaskScheduler::Init(cfg);
		std::vector<Coro> v;
		for (int i = 0; i < 64; ++i) v.push_back(Unspawned(i));
		v.clear();
	}

	const bool ok = (g_bodies == 0);
	std::printf("  %s late frees are no-ops and nothing ran that should not have\n", ok ? "ok  " : "FAIL");
	std::printf(ok ? "RESULT: all checks passed\n" : "RESULT: 1 FAILURE(S)\n");
	std::fflush(stdout);
	std::_Exit(ok ? 0 : 1);
}
