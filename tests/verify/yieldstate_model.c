// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define ST_EMPTY     0
#define ST_NOTIFIED  1
#define ST_PARKED    2
#define ST_YIELD     3

static _Atomic int g_permit;
static _Atomic int g_oncore;    
static _Atomic int g_work;      
static _Atomic int g_oswake;    
static _Atomic int g_aimed;     

static _Atomic int g_consumed;

#define PUBLISH     memory_order_seq_cst
#define OBSERVE     memory_order_seq_cst
#define TRANSITION  memory_order_seq_cst

static void *worker(void *arg) {
    (void)arg;

    atomic_store_explicit(&g_oncore, 1, PUBLISH);

#ifndef NO_YIELD_HANDSHAKE
    
    {
        int e = ST_EMPTY;
        if (!atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_YIELD,
                                                     TRANSITION, memory_order_relaxed)) {
            
            if (e == ST_NOTIFIED) {
                int e2 = ST_NOTIFIED;
                if (atomic_compare_exchange_strong_explicit(&g_permit, &e2, ST_EMPTY,
                                                            TRANSITION, memory_order_relaxed))
                    atomic_store_explicit(&g_consumed, 1, PUBLISH);
            }
            return NULL;                     
        }
    }
#else
    
#endif

    atomic_store_explicit(&g_oncore, 0, PUBLISH);
#ifdef PROBE_OFFCORE
    assert(0);                               
#endif
    atomic_store_explicit(&g_oncore, 1, PUBLISH);

#ifndef NO_YIELD_HANDSHAKE
    
# ifdef YIELD_STORE_BACK
    
    atomic_store_explicit(&g_permit, ST_EMPTY, PUBLISH);
# else
    {
        int e = ST_YIELD;
        if (!atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_EMPTY,
                                                     TRANSITION, memory_order_relaxed)) {
            
            int e2 = ST_NOTIFIED;
            if (atomic_compare_exchange_strong_explicit(&g_permit, &e2, ST_EMPTY,
                                                        TRANSITION, memory_order_relaxed))
                atomic_store_explicit(&g_consumed, 1, PUBLISH);
        }
    }
# endif
#endif
    return NULL;
}

static void *producer(void *arg) {
    (void)arg;

    const int st = atomic_load_explicit(&g_permit, OBSERVE);
#ifndef TARGET_YIELDED
    if (st == ST_YIELD)
        return NULL;                         
#else
    
    (void)st;
#endif

    atomic_store_explicit(&g_aimed, 1, PUBLISH);
    atomic_store_explicit(&g_work, 1, PUBLISH);

    const int prev = atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    if (prev == ST_PARKED) {
        atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
        return NULL;
    }

    if (prev == ST_EMPTY)
        assert(atomic_load_explicit(&g_oncore, OBSERVE) == 1);

    return NULL;
}

int main(void) {
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_oncore, 1);
    atomic_init(&g_work, 0);
    atomic_init(&g_oswake, 0);
    atomic_init(&g_aimed, 0);
    atomic_init(&g_consumed, 0);

    pthread_t w, p;
    pthread_create(&w, NULL, worker,   NULL);
    pthread_create(&p, NULL, producer, NULL);
    pthread_join(w, NULL);
    pthread_join(p, NULL);

    const int work   = atomic_load(&g_work);
    const int permit = atomic_load(&g_permit);
    const int oswake = atomic_load(&g_oswake);
    const int consumed = atomic_load(&g_consumed);

    assert(!(work > 0 && oswake == 0 && consumed == 0 && permit != ST_NOTIFIED));

    return 0;
}
