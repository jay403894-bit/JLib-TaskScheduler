// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// GenMC model of inbox adoption (BlockInPlace for any pool thread). One int per thread:
//   -1  normal
//   >=0 I have adopted that thread's normal inbox (I drain it alongside my own)
//   -2  I am away (blocked in code that cannot suspend); someone has adopted my inbox
//
//   blocker B:  CAS slot[A] -1 -> B ; slot[B] = -2 ; fence ; kick A                   (claim)
//               ... blocked ...
//               CAS slot[A] B -> -1 ; slot[B] = -1 ; wait draining[A] != B ; resume draining own
//   adopter A:  q = slot[A] ; if q >= 0: draining[A] = q ; fence ; if slot[A] == q: pop q ;
//               draining[A] = -1.  Park: CAS state RUNNING -> PARKED, sleep until kicked.
//   pusher:     push inbox[B] ; kick B ; fence ; if slot[B] == -2: kick every w with slot[w] == B
//
// -DSPARE: the adopter runs what it pops (a spare thread has no deque); draining must be released
// before the run, so a returning owner waits out at most a pop, never a task.
//
// Checked: an inbox never has two consumers at once (assert); nothing waits while every consumer
// sleeps forever (--check-liveness reports the sleep loop if it can spin forever).
// Run: --disable-estimation --check-liveness.

#include <stdatomic.h>
#include <stdbool.h>
#include <pthread.h>
#include <assert.h>

#define SC memory_order_seq_cst
#define NORMAL (-1)
#define AWAY   (-2)
enum { RUNNING = 0, PARKED = 1, KICKED = 2 };

#define A  0
#define B1 1
#define B2 2
#define NT 3
#ifndef PUSHES
#define PUSHES 1
#endif

static _Atomic int g_slot[NT]     = { NORMAL, NORMAL, NORMAL };
static _Atomic int g_draining[NT] = { NORMAL, NORMAL, NORMAL };
static _Atomic int g_inbox[NT];       // tasks waiting in each normal inbox
static _Atomic int g_consumers[NT];   // threads inside a pop, per inbox
static _Atomic int g_state[NT];       // park words
static _Atomic int g_taken;           // tasks consumed from B1's inbox
static _Atomic int g_b1Back;          // B1 returned and owns its inbox again
static _Atomic int g_running;         // SPARE: the adopter is running a task it popped

static void kick(int w) { atomic_exchange_explicit(&g_state[w], KICKED, SC); }

static bool pop_one(int q) {
    bool got = false;
    int c = atomic_fetch_add_explicit(&g_consumers[q], 1, SC);
    assert(c == 0);                                  // one consumer at a time
    atomic_thread_fence(SC);
    if (atomic_load_explicit(&g_inbox[q], memory_order_acquire) > 0) {
        atomic_fetch_sub_explicit(&g_inbox[q], 1, memory_order_relaxed);   // pushers only add
        if (q == B1) atomic_fetch_add_explicit(&g_taken, 1, SC);
        got = true;
    }
    atomic_fetch_sub_explicit(&g_consumers[q], 1, SC);
    return got;
}

// A spare has no deque: it runs what it pops itself.
// It must have released the inbox first: an owner returning now waits out at most a pop.
static void run_task(void) {
    assert(atomic_load_explicit(&g_draining[A], SC) == NORMAL);
    atomic_store_explicit(&g_running, 1, SC);
    atomic_store_explicit(&g_running, 0, SC);
}

static bool claim(int me) {
    int exp = NORMAL;
#ifdef EXCHANGE_CLAIM
    (void)exp;
    atomic_exchange_explicit(&g_slot[A], me, SC);
#else
    if (!atomic_compare_exchange_strong_explicit(&g_slot[A], &exp, me, SC, SC)) return false;
#endif
    atomic_store_explicit(&g_slot[me], AWAY, SC);
    atomic_thread_fence(SC);                         // Dekker with the pusher's redirect
    kick(A);
    return true;
}

// Every push consumed, or B1 is back: what is left in its inbox is then B1's, live and in its loop.
static bool done(void) {
    return atomic_load_explicit(&g_taken, SC) == PUSHES || atomic_load_explicit(&g_b1Back, SC);
}

static void *blocker1(void *arg) {
    (void)arg;
    claim(B1);
    // ... blocked in foreign code ...
#ifdef RETURNS
    int exp = B1;
    atomic_compare_exchange_strong_explicit(&g_slot[A], &exp, NORMAL, SC, SC);
    atomic_store_explicit(&g_slot[B1], NORMAL, SC);
    while (atomic_load_explicit(&g_draining[A], SC) == B1) {}   // adopter mid-pop: wait it out
    pop_one(B1);                                                // B1 consumes its own inbox again
    atomic_store_explicit(&g_b1Back, 1, SC);
#endif
    return NULL;
}

// Claims after B1 holds A: a CAS fails (the fallback path, not modelled); a plain exchange
// steals A from B1 and leaves B1's inbox with no consumer.
static void *blocker2(void *arg) {
    (void)arg;
    while (atomic_load_explicit(&g_slot[B1], SC) != AWAY) {}
    claim(B2);
    return NULL;
}

static void *pusher(void *arg) {
    (void)arg;
    for (int i = 0; i < PUSHES; ++i) {
        atomic_fetch_add_explicit(&g_inbox[B1], 1, memory_order_release);
        kick(B1);
#ifndef NO_WAKE_REDIRECT
  #ifndef NO_REDIRECT_FENCE
        atomic_thread_fence(SC);                     // the push is ordered before this read
  #endif
        if (atomic_load_explicit(&g_slot[B1], SC) == AWAY)
            for (int w = 0; w < NT; ++w)
                if (w != B1 && atomic_load_explicit(&g_slot[w], SC) == B1) kick(w);
#endif
    }
    return NULL;
}

static void drain_adopted(void) {
    int q = atomic_load_explicit(&g_slot[A], SC);
    if (q < 0) return;
    atomic_store_explicit(&g_draining[A], q, SC);
    atomic_thread_fence(SC);
    bool got = false;
#ifndef NO_DRAIN_RECHECK
    if (atomic_load_explicit(&g_slot[A], SC) == q)
#endif
        got = pop_one(q);
#if defined(SPARE) && defined(DRAIN_ACROSS_RUN)
    if (got) run_task();                     // wrong: the owner may wait out the whole task
#endif
    atomic_store_explicit(&g_draining[A], NORMAL, SC);
#if defined(SPARE) && !defined(DRAIN_ACROSS_RUN)
    if (got) run_task();                     // a spare runs it only after releasing the inbox
#endif
    (void)got;
}

static void *adopter(void *arg) {
    (void)arg;
    for (;;) {
        drain_adopted();
        if (done()) return NULL;
        int r = RUNNING;
        if (!atomic_compare_exchange_strong_explicit(&g_state[A], &r, PARKED, SC, SC)) {
            atomic_exchange_explicit(&g_state[A], RUNNING, SC);   // consume the kick
            continue;
        }
        // Park recheck covers the adopted inbox too: while adopted it is one of A's queues.
        // Load-bearing: without it two pushes lose a wake (-DNO_PARK_RECHECK -DPUSHES=2).
#ifndef NO_PARK_RECHECK
        {
            const int q = atomic_load_explicit(&g_slot[A], SC);
            if (q >= 0 && atomic_load_explicit(&g_inbox[q], SC) > 0) {
                atomic_exchange_explicit(&g_state[A], RUNNING, SC);
                continue;
            }
        }
#endif
        while (atomic_load_explicit(&g_state[A], SC) == PARKED && !done()) {}   // asleep
        if (done()) return NULL;
        atomic_exchange_explicit(&g_state[A], RUNNING, SC);
    }
}

int main(void) {
    pthread_t a, b1, b2, p;
    pthread_create(&a, NULL, adopter, NULL);
    pthread_create(&b1, NULL, blocker1, NULL);
#ifdef TWO_BLOCKERS
    pthread_create(&b2, NULL, blocker2, NULL);
#endif
    pthread_create(&p, NULL, pusher, NULL);
    pthread_join(p, NULL);
    pthread_join(b1, NULL);
#ifdef TWO_BLOCKERS
    pthread_join(b2, NULL);
#endif
    pthread_join(a, NULL);
    return 0;
}
