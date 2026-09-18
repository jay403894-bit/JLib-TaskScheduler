// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// The hunt protocol: the `searching` counter, the last-hunter rule, and the 1->0 handoff wake.
// Mirrors TaskScheduler::EnterHunt / LeaveHunt / TryLeaveHuntForPark and the park path in
// Thread.cpp. Checked with --check-liveness.
//
// What it protects: work pushed onto a deque with NO wake. A resume or a yield goes onto the
// worker's own deque and notifies nobody; if that worker stays busy, someone must be looking.
//
// Scenario (default, deep): W0 runs task T. T pushes X onto W0's deque unannounced and waits for
// X. Whoever runs X pushes Y onto its own deque unannounced and waits for Y. Progress needs X and
// Y to be stolen. S waits for X (which implies Y) and then stops the pool. If the protocol ever
// leaves nobody looking, some wait never ends and GenMC reports a liveness violation.
// -DONE_LEVEL: two workers, T pushes X only. Much cheaper; enough for the last-hunter rule.
//
// FOUND BY THIS MODEL (2026-09-18): the park path left the hunt (decrement `searching`) BEFORE
// arming on the idle stack. A LeaveHunt taking `searching` 1->0 in that gap found nobody to hand
// off to; the parker then armed, rechecked its own (empty) queues and slept with searching == 0,
// while Y sat on a busy worker's deque. Fixed in Thread.cpp: after arming, a parker that sees
// searching == 0 unparks and hunts (Dekker pair with LeaveHunt; all four operations seq_cst).
//
// Switches (each "must fail" is a negative control):
//   (none)            shipping, with the fix
//   -DOLD_PARK        the pre-fix park: no searching recheck after arming (must fail, deep)
//   -DNO_LAST_HUNTER  every hunter may park (must fail)
//   -DNO_HANDOFF      LeaveHunt 1->0 wakes nobody (must fail, deep)
//   -DMETRONOME       a ticking thread wakes a parked worker whenever a deque has work -- the
//                     "everyone may park" alternative; pair with NO_LAST_HUNTER
// NOT a switch: removing the own-queue recheck. In this scenario nothing ever lands on a parked
// worker's own deque, so that mutant cannot fail here; it needs a resume/PushTo onto a sleeper.

#include <stdatomic.h>
#include <pthread.h>
#include <assert.h>

#ifdef ONE_LEVEL
  #define NW 2
#else
  #define NW 3
#endif
#define ST_EMPTY    0
#define ST_NOTIFIED 1
#define ST_PARKED   2

enum { T_T = 1, T_X = 2, T_Y = 3 };

atomic_int searching;
atomic_int permit[NW];
atomic_int armed[NW];          // registered on the idle stack
atomic_int deque[NW];          // one item slot per deque; 0 = empty
atomic_int ranX, ranY;
atomic_int stop;

static void wake(int j) {
    // Thread::Wake: exchange to NOTIFIED; the OS wake is the parked loop seeing it.
    atomic_exchange_explicit(&permit[j], ST_NOTIFIED, memory_order_seq_cst);
}

static void pop_idle_and_wake(void) {
    for (int j = 0; j < NW; ++j)
        if (atomic_exchange_explicit(&armed[j], 0, memory_order_seq_cst)) {
            if (atomic_load_explicit(&permit[j], memory_order_seq_cst) == ST_PARKED) { wake(j); return; }
        }
}

static void enter_hunt(void) { atomic_fetch_add_explicit(&searching, 1, memory_order_seq_cst); }

static void leave_hunt(void) {
    const int old = atomic_fetch_sub_explicit(&searching, 1, memory_order_seq_cst);
#ifndef NO_HANDOFF
    if (old == 1) pop_idle_and_wake();
#else
    (void)old;
#endif
}

// The real code is a CAS loop that refuses when searching <= 1. Modeled with RMWs instead: GenMC
// treats a failed-CAS retry as an effect-free spin and would flag it as non-terminating. The
// refusal dips the count to 0 for an instant, which can only cause an extra handoff wake, never a
// missing one -- it cannot make the shipping build pass wrongly.
static int try_leave_for_park(void) {
    const int old = atomic_fetch_sub_explicit(&searching, 1, memory_order_seq_cst);
#ifndef NO_LAST_HUNTER
    if (old <= 1) {                      // the last hunter stays up
        atomic_fetch_add_explicit(&searching, 1, memory_order_seq_cst);
        return 0;
    }
#else
    (void)old;
#endif
    return 1;
}

// Flat on purpose: used inside spin-wait conditions, and GenMC's spin detection does not see a
// wait end when the condition itself contains a loop.
static int any_work(void) {
    return atomic_load_explicit(&deque[0], memory_order_seq_cst)
        || atomic_load_explicit(&deque[1], memory_order_seq_cst)
#if NW > 2
        || atomic_load_explicit(&deque[2], memory_order_seq_cst)
#endif
        ;
}

// A steal writes only when it takes something, as the Chase-Lev steal does: an empty deque is a
// pure read. (GenMC treats a loop pass with no effective write as a spin.)
static int steal_any(void) {
    for (int j = 0; j < NW; ++j)
        if (atomic_load_explicit(&deque[j], memory_order_seq_cst)) {
            const int t = atomic_exchange_explicit(&deque[j], 0, memory_order_seq_cst);
            if (t) return t;
        }
    return 0;
}

static void run(int task, int self) {
    if (task == T_T) {
        atomic_store_explicit(&deque[self], T_X, memory_order_seq_cst);   // unannounced push
        while (!atomic_load_explicit(&ranX, memory_order_acquire)) {}
    } else if (task == T_X) {
#ifdef ONE_LEVEL
        atomic_store_explicit(&ranY, 1, memory_order_release);
        atomic_store_explicit(&ranX, 1, memory_order_release);
#else
        atomic_store_explicit(&deque[self], T_Y, memory_order_seq_cst);   // unannounced push
        while (!atomic_load_explicit(&ranY, memory_order_acquire)) {}
        atomic_store_explicit(&ranX, 1, memory_order_release);
#endif
    } else if (task == T_Y) {
        atomic_store_explicit(&ranY, 1, memory_order_release);
    }
}

static void unpark(int self) {
    atomic_store_explicit(&armed[self], 0, memory_order_seq_cst);
    atomic_store_explicit(&permit[self], ST_EMPTY, memory_order_seq_cst);
    enter_hunt();
}

static void* worker(void* arg) {
    const int self = (int)(long)arg;
    enter_hunt();
    if (self == 0) {
        leave_hunt();
        run(T_T, 0);
#ifndef FULL_W0
        // W0 stops after T instead of hunting on. Conservative: fewer hunters can only make a lost
        // wakeup easier to reach, never hide one -- and it cuts the deep search from ~1.4e9
        // executions to something that finishes. -DFULL_W0 restores it.
        return 0;
#endif
        enter_hunt();
    }

    for (;;) {
        if (atomic_load_explicit(&stop, memory_order_acquire)) break;

        int got = steal_any();
        if (got) {
            leave_hunt(); run(got, self);
#ifndef FULL_WORKERS
            // Stop after running a stolen task, as W0 stops after T. Conservative (fewer hunters
            // only make a lost wakeup easier), and after Y runs there is no work left to explore.
            return 0;
#endif
            enter_hunt(); continue;
        }

        if (!try_leave_for_park()) {
            // The last hunter keeps scanning until some deque has work, then steals in the same
            // pass -- not by looping back first, which GenMC would see as an effect-free spin.
            while (!any_work() && !atomic_load_explicit(&stop, memory_order_acquire)) {}
            if (atomic_load_explicit(&stop, memory_order_acquire)) break;
            got = steal_any();
            if (got) {
                leave_hunt(); run(got, self);
#ifndef FULL_WORKERS
                return 0;
#endif
                enter_hunt();
            }
            continue;
        }

        int e = ST_EMPTY;
        if (!atomic_compare_exchange_strong_explicit(&permit[self], &e, ST_PARKED,
                memory_order_seq_cst, memory_order_seq_cst)) {
            atomic_store_explicit(&permit[self], ST_EMPTY, memory_order_seq_cst);   // consume NOTIFIED
            enter_hunt();
            continue;
        }
        atomic_store_explicit(&armed[self], 1, memory_order_seq_cst);

        // Recheck: the worker's OWN queues only, as in Thread.cpp (a recheck of every deque would
        // act as a spare hunter), plus -- the fix -- whether anybody is still hunting.
        if (atomic_load_explicit(&deque[self], memory_order_seq_cst)
            || atomic_load_explicit(&stop, memory_order_seq_cst)
#ifndef OLD_PARK
            || atomic_load_explicit(&searching, memory_order_seq_cst) == 0
#endif
            ) {
            unpark(self);
            continue;
        }

        while (atomic_load_explicit(&permit[self], memory_order_seq_cst) == ST_PARKED
               && !atomic_load_explicit(&stop, memory_order_acquire)) {}
        unpark(self);
    }
    return 0;
}

static void* shutdown(void* arg) {
    (void)arg;
    while (!atomic_load_explicit(&ranX, memory_order_acquire)) {}
    atomic_store_explicit(&stop, 1, memory_order_release);
    for (int j = 0; j < NW; ++j) wake(j);
    return 0;
}

#ifdef METRONOME
static void* metronome(void* arg) {
    (void)arg;
    while (!atomic_load_explicit(&stop, memory_order_acquire)) {
        if (any_work()) pop_idle_and_wake();
    }
    return 0;
}
#endif

int main(void) {
    pthread_t w[NW], s;
#ifdef METRONOME
    pthread_t m;
    pthread_create(&m, 0, metronome, 0);
#endif
    for (long i = 0; i < NW; ++i) pthread_create(&w[i], 0, worker, (void*)i);
    pthread_create(&s, 0, shutdown, 0);
    for (int i = 0; i < NW; ++i) pthread_join(w[i], 0);
    pthread_join(s, 0);
#ifdef METRONOME
    pthread_join(m, 0);
#endif
    assert(atomic_load(&ranX) && atomic_load(&ranY));
    return 0;
}
