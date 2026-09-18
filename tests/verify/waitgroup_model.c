// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// WaitGroup completion: Done() against waiters that may not suspend (Settle) and then free the
// group. Mirrors include/WaitGroup.h + src/WaitGroup.cpp.
//
// Two properties, checked with --check-liveness:
//   safety   -- nothing touches the group after the last waiter frees it (it is really freed)
//   liveness -- no Settle and no wait spins forever
//
// Threads: C completes the one outstanding task (Done). W0 waits as a task. W1 waits as a task,
// or with -DBLOCK_WAITER through BlockThread. The last waiter to finish frees the group, so any
// use-after-free found is a waker's, not a waiter's.
//
// Switches:
//   (none)          the shipping protocol: a `waking` counter bracketing every Done
//   -DPROPOSED      no `waking`: Done is one fetch_sub, and Settle waits while the count is zero
//                   with WAITER_BIT still set (the waker clears it in its locked section)
//   -DFIX_DIRECT    with PROPOSED: WakeAllDirect clears the bit even when the list is empty
//   -DFIX_BLOCK     with PROPOSED: BlockThread clears the bit when it finds the count at zero
//   -DPACKED        `waking` packed into `n` above the count: Done marks and decrements in one RMW
//   -DNO_SETTLE     negative control: waiters free without settling (must fail, any design)
//
// Results (GenMC 0.18, --check-liveness, 2026-09-18): shipping clean; PROPOSED alone is a liveness
// violation (a stale waiter bit); PROPOSED + FIX_DIRECT is a use-after-free (the bit is also cleared
// by WakeAllDirect, so "bit clear" does not mean "the waker left"); PACKED is clean but measured
// slower in the scheduler (fib ~13%, pingpong fiber), so the shipping protocol stays.

#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>

#ifdef PACKED
// The `waking` count lives in the same word as the task count, above the waiter bit, so marking
// "a waker is in flight" and the decrement are one read-modify-write.
#define COUNT_MASK  0x0000FFFF
#define WAITER_BIT  0x00010000
#define WAKE_SHIFT  20
#define WAKE_ONE    (1 << WAKE_SHIFT)
#else
#define WAITER_BIT 0x40000000
#define COUNT_MASK (WAITER_BIT - 1)
#endif

struct wg {
    atomic_int      n;
    atomic_int      waking;
    atomic_int      tasks;     // registered task waiters, one bit each (the Treiber list)
    atomic_int      blocked;   // registered BlockThread waiters, one bit each (guarded by mtx)
    pthread_mutex_t mtx;
};

struct wg* g;
atomic_int woken[2];           // each waiter's own wake flag (not in the group)
atomic_int finished;           // waiters done with the group; the last one frees it

static void wake_mask(int m) {
    for (int i = 0; i < 2; ++i)
        if (m & (1 << i)) atomic_store_explicit(&woken[i], 1, memory_order_release);
}

// Called from Done with releaseMark = true.
static void wake_all(void) {
    pthread_mutex_lock(&g->mtx);
    const int list = atomic_exchange_explicit(&g->tasks, 0, memory_order_acq_rel);
    const int thr  = atomic_exchange_explicit(&g->blocked, 0, memory_order_relaxed);
    atomic_fetch_and_explicit(&g->n, ~WAITER_BIT, memory_order_release);
#if defined(PACKED)
    atomic_fetch_sub_explicit(&g->n, WAKE_ONE, memory_order_acq_rel);
#elif !defined(PROPOSED)
    atomic_fetch_sub_explicit(&g->waking, 1, memory_order_acq_rel);
#endif
    pthread_mutex_unlock(&g->mtx);
    wake_mask(list | thr);     // locals only from here
}

static void done(void) {
#if defined(PACKED)
    const int old = atomic_fetch_add_explicit(&g->n, WAKE_ONE - 1, memory_order_acq_rel);
    if ((old & COUNT_MASK) == 1 && (old & WAITER_BIT)) { wake_all(); return; }
    atomic_fetch_sub_explicit(&g->n, WAKE_ONE, memory_order_acq_rel);
#else
  #ifndef PROPOSED
    atomic_fetch_add_explicit(&g->waking, 1, memory_order_acq_rel);
  #endif
    const int old = atomic_fetch_sub_explicit(&g->n, 1, memory_order_acq_rel);
    if ((old & COUNT_MASK) == 1 && (old & WAITER_BIT)) { wake_all(); return; }
  #ifndef PROPOSED
    atomic_fetch_sub_explicit(&g->waking, 1, memory_order_acq_rel);
  #endif
#endif
}

static void wake_all_direct(void) {
    const int list = atomic_exchange_explicit(&g->tasks, 0, memory_order_acq_rel);
#ifndef FIX_DIRECT
    if (!list) return;
#endif
    atomic_fetch_and_explicit(&g->n, ~WAITER_BIT, memory_order_release);
    wake_mask(list);
}

static void settle(void) {
#ifndef NO_SETTLE
  #if defined(PACKED)
    while ((atomic_load_explicit(&g->n, memory_order_acquire) >> WAKE_SHIFT) != 0) {}
  #elif defined(PROPOSED)
    for (;;) {
        const int v = atomic_load_explicit(&g->n, memory_order_acquire);
        if ((v & COUNT_MASK) != 0 || !(v & WAITER_BIT)) break;
    }
  #else
    while (atomic_load_explicit(&g->waking, memory_order_acquire) != 0) {}
  #endif
    pthread_mutex_lock(&g->mtx);
    pthread_mutex_unlock(&g->mtx);
#endif
}

static void leave(void) {
    if (atomic_fetch_add_explicit(&finished, 1, memory_order_acq_rel) == 1) free(g);
}

// A fiber/coroutine waiter: WaitFor / co_await WaitAsync.
static void task_wait(int i) {
    if ((atomic_load_explicit(&g->n, memory_order_acquire) & COUNT_MASK) == 0) { settle(); leave(); return; }
    atomic_fetch_or_explicit(&g->tasks, 1 << i, memory_order_acq_rel);
    const int old = atomic_fetch_or_explicit(&g->n, WAITER_BIT, memory_order_acq_rel);
    if ((old & COUNT_MASK) == 0) wake_all_direct();
    while (!atomic_load_explicit(&woken[i], memory_order_acquire)) {}
    settle();
    leave();
}

// A thread that cannot suspend: BlockThread.
static void block_wait(int i) {
    for (;;) {
        pthread_mutex_lock(&g->mtx);
        const int old = atomic_fetch_or_explicit(&g->n, WAITER_BIT, memory_order_acq_rel);
        if ((old & COUNT_MASK) == 0) {
#ifdef FIX_BLOCK
            atomic_fetch_and_explicit(&g->n, ~WAITER_BIT, memory_order_release);
#endif
            pthread_mutex_unlock(&g->mtx);
            break;
        }
        atomic_fetch_or_explicit(&g->blocked, 1 << i, memory_order_relaxed);
        pthread_mutex_unlock(&g->mtx);
        while (!atomic_load_explicit(&woken[i], memory_order_acquire)) {}
        if ((atomic_load_explicit(&g->n, memory_order_acquire) & COUNT_MASK) == 0) break;
    }
    settle();
    leave();
}

static void* thr_c(void* a)  { (void)a; done(); return NULL; }
static void* thr_w0(void* a) { (void)a; task_wait(0); return NULL; }
static void* thr_w1(void* a) {
    (void)a;
#ifdef BLOCK_WAITER
    block_wait(1);
#else
    task_wait(1);
#endif
    return NULL;
}

int main(void) {
    g = malloc(sizeof *g);
    atomic_init(&g->n, 1);          // one task outstanding
    atomic_init(&g->waking, 0);
    atomic_init(&g->tasks, 0);
    atomic_init(&g->blocked, 0);
    pthread_mutex_init(&g->mtx, NULL);

    pthread_t c, w0, w1;
    pthread_create(&w0, NULL, thr_w0, NULL);
    pthread_create(&w1, NULL, thr_w1, NULL);
    pthread_create(&c,  NULL, thr_c,  NULL);
    pthread_join(c, NULL);
    pthread_join(w0, NULL);
    pthread_join(w1, NULL);
    return 0;
}
