// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define CAP0    2                 
#define CAP1    (CAP0 * 2)
#define NITEMS  3                 

#define EMPTY   (-1)
#define ABORT   (-2)
#define POISON  (-99)             
#define BASE    2                 

static _Atomic size_t g_top;
static _Atomic size_t g_bottom;

static _Atomic int g_bufA[CAP0];
static _Atomic int g_bufB[CAP1];

#ifdef NO_PUBLISH_RELEASE
  #define PUBLISH_ORDER memory_order_relaxed
  #define ACQUIRE_ORDER memory_order_relaxed
#else
  #define PUBLISH_ORDER memory_order_release   
  #define ACQUIRE_ORDER memory_order_acquire
#endif

static _Atomic int *_Atomic g_buf;

#ifdef SPLIT_PTR_MASK

static _Atomic size_t g_mask;
static size_t mask_of(_Atomic int *buf) { (void)buf; return atomic_load_explicit(&g_mask, ACQUIRE_ORDER); }
#else

static size_t mask_of(_Atomic int *buf) { return (buf == g_bufA) ? (CAP0 - 1) : (CAP1 - 1); }
#endif

static _Atomic int g_claims[NITEMS + 1];

static void grow(size_t t, size_t b) {
    _Atomic int *old = atomic_load_explicit(&g_buf, memory_order_relaxed);   
    size_t oldMask = mask_of(old);
    _Atomic int *nbuf = g_bufB;
    size_t newMask = CAP1 - 1;

    for (size_t i = t; i != b; ++i)
        atomic_store_explicit(&nbuf[i & newMask],
                              atomic_load_explicit(&old[i & oldMask], memory_order_relaxed),
                              memory_order_relaxed);

#ifdef SPLIT_PTR_MASK
    atomic_store_explicit(&g_mask, newMask, PUBLISH_ORDER);
#endif
    atomic_store_explicit(&g_buf, nbuf, PUBLISH_ORDER);

#ifdef NO_RETIRE
    
    for (size_t i = 0; i < CAP0; ++i) atomic_store_explicit(&old[i], POISON, memory_order_relaxed);
#endif
    
}

static void push_bottom(int item) {
    size_t b = atomic_load_explicit(&g_bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&g_top,    memory_order_acquire);

    _Atomic int *buf = atomic_load_explicit(&g_buf, memory_order_relaxed);
    size_t mask = mask_of(buf);

    if (b - t >= mask + 1) {
        grow(t, b);                                        
        buf  = atomic_load_explicit(&g_buf, memory_order_relaxed);
        mask = mask_of(buf);
    }

    atomic_store_explicit(&buf[b & mask], item, memory_order_relaxed);
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&g_bottom, b + 1, memory_order_release);
}

static int pop_bottom(void) {
    size_t b = atomic_load_explicit(&g_bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&g_top,    memory_order_acquire);
    if (t >= b) return EMPTY;

    b -= 1;
    atomic_store_explicit(&g_bottom, b, memory_order_release);
    atomic_thread_fence(memory_order_seq_cst);
    t = atomic_load_explicit(&g_top, memory_order_acquire);

    if (t <= b) {
        _Atomic int *buf = atomic_load_explicit(&g_buf, ACQUIRE_ORDER);
        size_t mask = mask_of(buf);
        int    item = atomic_load_explicit(&buf[b & mask], memory_order_relaxed);
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
        
        _Atomic int *buf = atomic_load_explicit(&g_buf, ACQUIRE_ORDER);
        size_t mask = mask_of(buf);
        int    item = atomic_load_explicit(&buf[t & mask], memory_order_relaxed);

        size_t expected = t;
        if (!atomic_compare_exchange_strong_explicit(
                &g_top, &expected, t + 1,
                memory_order_acq_rel, memory_order_relaxed)) {
            return ABORT;                                  
        }
        return item;
    }
    return EMPTY;
}

static void record(int item) {
    
    assert(item != POISON);
    if (item >= 1 && item <= NITEMS)
        atomic_fetch_add_explicit(&g_claims[item], 1, memory_order_relaxed);
}

static void *owner_thread(void *arg) {
    (void)arg;
    push_bottom(NITEMS);        
    record(pop_bottom());
    return NULL;
}

static void *thief_thread(void *arg) {
    (void)arg;
    record(steal());
    return NULL;
}

static void *thief_thread2(void *arg) {
    (void)arg;
    record(steal());
    return NULL;
}

int main(void) {
    
    atomic_init(&g_top, BASE);
    atomic_init(&g_bottom, BASE);
    atomic_init(&g_buf, g_bufA);
#ifdef SPLIT_PTR_MASK
    atomic_init(&g_mask, CAP0 - 1);
#endif
    for (int i = 0; i <= NITEMS; ++i) atomic_init(&g_claims[i], 0);
    for (int i = 0; i < CAP1; ++i) atomic_init(&g_bufB[i], POISON);   

    for (int i = 1; i < NITEMS; ++i) push_bottom(i);

    pthread_t owner, thief, thief2;
    pthread_create(&owner,  NULL, owner_thread,  NULL);
    pthread_create(&thief,  NULL, thief_thread,  NULL);
    pthread_create(&thief2, NULL, thief_thread2, NULL);
    pthread_join(owner,  NULL);
    pthread_join(thief,  NULL);
    pthread_join(thief2, NULL);

    for (int i = 1; i <= NITEMS; ++i)
        assert(atomic_load(&g_claims[i]) <= 1);

    int total = 0;
    for (int i = 1; i <= NITEMS; ++i) total += atomic_load(&g_claims[i]);
    assert(total >= 1);

    return 0;
}
