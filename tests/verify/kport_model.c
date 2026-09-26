// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// K is a PURE PUMP: it parks in the completion port and moves every completion into the injector
// (the userspace ring). The pool finds that work with no wake and NO SYSCALL -- workers never touch
// the port. Design model for the I/O overhaul. Checked with --check-liveness.
//
//   - K waits in the port (GetQueuedCompletionStatusEx / io_uring_enter). A completion wakes K --
//     the ONE OS wake -- and K injects it and goes straight back to the port. K runs nothing.
//   - The injector wakes nobody. The last hunter never parks while the pool is live, and every
//     pass takes from the injector, so someone always looks.
//   - Workers never poll the port. On Windows that poll is a kernel call per look, and under a
//     steady I/O load it fires often; the injector is a load. So the port has ONE reader, and K
//     draining it completely is load-bearing -- see K_STOPS_AFTER_ONE below.
//   - Nobody posts to the port either: only the device puts packets there.
//
// Scenario: the device posts two completions C1, C2 (unpinned resumes). A pusher injects task L.
// All three must run, each exactly once (run() asserts it), whoever takes them.
//
// Switches (each "must fail" is a negative control):
//   (none)                the design (must pass)
//   -DHUNTER_PARKS        the hunter parks when it finds nothing; nothing wakes it (must fail)
//   -DNO_HUNTER_INJECTOR  the hunter does not read the injector (must fail)
//   -DK_DROPS             K takes a packet from the port and never injects it (must fail)
//   -DK_STOPS_AFTER_ONE   K takes one packet, then leaves the port (must fail). THIS is the control
//                         that shows what changed: nobody else reads the port any more, so a second
//                         packet is stranded. Add -DHUNTER_POLLS_PORT and it passes again, because
//                         the old design's hunter port poll rescues it.
//
// The previous design, kept for comparison (both must pass):
//   -DK_RUNS_FIRST        K runs the first completion itself and injects the rest
//   -DHUNTER_POLLS_PORT   every hunter pass also polls the port without blocking (the syscall path)

#include <stdatomic.h>
#include <pthread.h>
#include <assert.h>

// Tasks are bits so the injector can hold several at once, the way the real queue does.
enum { T_C1 = 1, T_C2 = 2, T_L = 4 };

atomic_int portC1, portC2;             // the port: two completion packets
atomic_int injector;                   // a set of task bits: inject = fetch_or, take = exchange(0)
atomic_int stop;
atomic_int ranC1, ranC2, ranL;

static void run(int task) {
    if (task == T_C1)      assert(atomic_fetch_add_explicit(&ranC1, 1, memory_order_relaxed) == 0);
    else if (task == T_C2) assert(atomic_fetch_add_explicit(&ranC2, 1, memory_order_relaxed) == 0);
    else if (task == T_L)  assert(atomic_fetch_add_explicit(&ranL, 1, memory_order_relaxed) == 0);
}

static void run_all(int bits) {
    if (bits & T_C1) run(T_C1);
    if (bits & T_C2) run(T_C2);
    if (bits & T_L)  run(T_L);
}

static int take_injector(void) {
    // Read first; write only when there is something (GenMC counts an effect-free pass as a spin).
    if (!atomic_load_explicit(&injector, memory_order_seq_cst)) return 0;
    return atomic_exchange_explicit(&injector, 0, memory_order_seq_cst);
}

// A non-blocking port take: one completion if there is one. A packet is claimed by exchange --
// exactly one taker gets it. In the design only K calls this; HUNTER_POLLS_PORT adds the hunter.
static int take_port(void) {
    if (atomic_load_explicit(&portC1, memory_order_seq_cst)) {
        const int c = atomic_exchange_explicit(&portC1, 0, memory_order_seq_cst);
        if (c) return c;
    }
    if (atomic_load_explicit(&portC2, memory_order_seq_cst)) {
        const int c = atomic_exchange_explicit(&portC2, 0, memory_order_seq_cst);
        if (c) return c;
    }
    return 0;
}

static void inject(int task) {
#ifndef K_DROPS
    atomic_fetch_or_explicit(&injector, task, memory_order_seq_cst);   // no wake
#else
    (void)task;
#endif
}

static void* kworker(void* arg) {
    (void)arg;
    for (;;) {
        // Wait in the port. The wait DEQUEUES (GetQueuedCompletionStatusEx returns packets it has
        // already removed), so a packet taken by someone else simply isn't returned to K.
        int first;
        while (!(first = take_port()))
            if (atomic_load_explicit(&stop, memory_order_acquire)) return 0;
#ifdef K_RUNS_FIRST
        // Previous design: run the first completion here, inject any second one.
        const int second = take_port();
        if (second) inject(second);
        run(first);
#else
        // Pump: inject it and go straight back to the port. K never runs a continuation, so it is
        // never away from the port while completions arrive.
        inject(first);
#endif
#ifdef K_STOPS_AFTER_ONE
        return 0;
#endif
    }
}

// The last hunter: never parks while the pool is live. Each pass takes from the injector -- and
// in the design, ONLY the injector.
static void* hunter(void* arg) {
    (void)arg;
    for (;;) {
        if (atomic_load_explicit(&stop, memory_order_acquire)) break;
#ifndef NO_HUNTER_INJECTOR
        int t = take_injector();
        if (t) { run_all(t); continue; }
#endif
#ifdef HUNTER_POLLS_PORT
        const int c = take_port();
        if (c) { run(c); continue; }
#endif
#ifdef HUNTER_PARKS
        // Parks on a token nobody sets (the injector and K wake nobody).
        while (!atomic_load_explicit(&stop, memory_order_acquire)) {}
        break;
#endif
    }
    return 0;
}

static void* device(void* arg) {
    (void)arg;
    atomic_store_explicit(&portC1, T_C1, memory_order_seq_cst);
    atomic_store_explicit(&portC2, T_C2, memory_order_seq_cst);
    return 0;
}

static void* pusher(void* arg) {
    (void)arg;
    atomic_fetch_or_explicit(&injector, T_L, memory_order_seq_cst);   // PushInjector: no wake
    return 0;
}

static void* shutdown(void* arg) {
    (void)arg;
    while (!atomic_load_explicit(&ranC1, memory_order_acquire)
           || !atomic_load_explicit(&ranC2, memory_order_acquire)
           || !atomic_load_explicit(&ranL, memory_order_acquire)) {}
    atomic_store_explicit(&stop, 1, memory_order_release);
    return 0;
}

int main(void) {
    pthread_t k, h, d, p, s;
    pthread_create(&k, 0, kworker, 0);
    pthread_create(&h, 0, hunter, 0);
    pthread_create(&d, 0, device, 0);
    pthread_create(&p, 0, pusher, 0);
    pthread_create(&s, 0, shutdown, 0);
    pthread_join(k, 0); pthread_join(h, 0);
    pthread_join(d, 0); pthread_join(p, 0); pthread_join(s, 0);
    assert(atomic_load(&ranC1) == 1 && atomic_load(&ranC2) == 1 && atomic_load(&ranL) == 1);
    return 0;
}
