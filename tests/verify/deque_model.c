// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stdbool.h>
#include <pthread.h>
#include <assert.h>

#define CAPACITY 4          
#define MASK     (CAPACITY - 1)
#define NITEMS   2          

#ifdef STEAL_CAS_SEQ_CST
  #define STEAL_CAS_SUCCESS memory_order_seq_cst
#else
  #define STEAL_CAS_SUCCESS memory_order_acq_rel   
#endif

static _Atomic size_t g_top;
static _Atomic size_t g_bottom;
static int            g_buffer[CAPACITY];

static _Atomic int g_claims[NITEMS + 1];

#define EMPTY (-1)
#define ABORT (-2)

static void push_bottom(int item) {
    size_t b = atomic_load_explicit(&g_bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&g_top,    memory_order_acquire);
    if (b - t >= CAPACITY) return;                       
    g_buffer[b & MASK] = item;
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&g_bottom, b + 1, memory_order_release);
}

static int pop_bottom(void) {
    size_t b = atomic_load_explicit(&g_bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&g_top,    memory_order_acquire);
    if (t >= b) return EMPTY;

    b -= 1;
    atomic_store_explicit(&g_bottom, b, memory_order_release);

#ifndef NO_POP_FENCE
    atomic_thread_fence(memory_order_seq_cst);           
#endif

    t = atomic_load_explicit(&g_top, memory_order_acquire);

    if (t <= b) {
        int item = g_buffer[b & MASK];
        if (t == b) {
            
            size_t expected = t;
            if (!atomic_compare_exchange_strong_explicit(
                    &g_top, &expected, t + 1,
                    memory_order_acq_rel, memory_order_relaxed)) {
                atomic_store_explicit(&g_bottom, b + 1, memory_order_relaxed);
                return EMPTY;                            
            }
            atomic_store_explicit(&g_bottom, b + 1, memory_order_relaxed);
        }
        return item;
    }
    atomic_store_explicit(&g_bottom, t, memory_order_relaxed);
    return EMPTY;
}

static int steal(void) {
    size_t t = atomic_load_explicit(&g_top, memory_order_acquire);
    atomic_thread_fence(memory_order_seq_cst);
    size_t b = atomic_load_explicit(&g_bottom, memory_order_acquire);

    if (t < b) {
        int item = g_buffer[t & MASK];
        size_t expected = t;
        if (!atomic_compare_exchange_strong_explicit(
                &g_top, &expected, t + 1,
                STEAL_CAS_SUCCESS, memory_order_relaxed)) {
            return ABORT;                                
        }
        return item;
    }
    return EMPTY;
}

static void record(int item) {
    if (item >= 1 && item <= NITEMS)
        atomic_fetch_add_explicit(&g_claims[item], 1, memory_order_relaxed);
}

static void *owner_thread(void *arg) {
    (void)arg;
    record(pop_bottom());
    record(pop_bottom());
    return NULL;
}

static void *thief_thread(void *arg) {
    (void)arg;
    record(steal());
    return NULL;
}

#ifndef SINGLE_THIEF
static void *thief_thread2(void *arg) {
    (void)arg;
    record(steal());
    return NULL;
}
#endif

int main(void) {
    atomic_init(&g_top, 0);
    atomic_init(&g_bottom, 0);
    for (int i = 0; i <= NITEMS; ++i) atomic_init(&g_claims[i], 0);

    for (int i = 1; i <= NITEMS; ++i) push_bottom(i);

    pthread_t owner, thief;
    pthread_create(&owner, NULL, owner_thread, NULL);
    pthread_create(&thief, NULL, thief_thread, NULL);
#ifndef SINGLE_THIEF
    pthread_t thief2;
    pthread_create(&thief2, NULL, thief_thread2, NULL);
#endif
    pthread_join(owner, NULL);
    pthread_join(thief, NULL);
#ifndef SINGLE_THIEF
    pthread_join(thief2, NULL);
#endif

    for (int i = 1; i <= NITEMS; ++i)
        assert(atomic_load(&g_claims[i]) <= 1);

    int total = 0;
    for (int i = 1; i <= NITEMS; ++i) total += atomic_load(&g_claims[i]);
    assert(total >= 1);

    return 0;
}
