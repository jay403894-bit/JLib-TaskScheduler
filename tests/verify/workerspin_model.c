// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define ST_EMPTY     0
#define ST_NOTIFIED  1
#define ST_PARKED    2

static _Atomic int g_lopri;    
static _Atomic int g_flag;     
static _Atomic int g_permit;   

static _Atomic int g_hint;     
static _Atomic int g_found;    
static _Atomic int g_parked;   

#define PUBLISH     memory_order_seq_cst
#define OBSERVE     memory_order_seq_cst
#define TRANSITION  memory_order_seq_cst

#ifdef PLACE_ON_RESERVED
  #ifndef SEARCH_MISS
    #define SEARCH_MISS 1
  #endif
  
  #define RECHECK_HINTS_ONLY 1
#endif

#ifdef SEARCH_MISS
  #define SEARCH_SEES_LOPRI 0
#else
  #define SEARCH_SEES_LOPRI 1
#endif

static int search_drain(void) {
#if SEARCH_SEES_LOPRI
    return atomic_exchange_explicit(&g_lopri, 0, TRANSITION) != 0;
#else
    return 0;
#endif
}

static int search_would_find(void) {
#if SEARCH_SEES_LOPRI
    return atomic_load_explicit(&g_lopri, OBSERVE) != 0;
#else
    return 0;
#endif
}

static int recheck_hint(void) {
    int h = atomic_load_explicit(&g_flag, OBSERVE) != 0;          
#ifndef RECHECK_HINTS_ONLY
    h = h || (atomic_load_explicit(&g_lopri, OBSERVE) != 0);      
#endif
    return h;
}

static void *worker(void *arg) {
    (void)arg;

    atomic_exchange_explicit(&g_flag, 0, TRANSITION);

    if (search_drain()) {
        atomic_store_explicit(&g_found, 1, PUBLISH);
        return NULL;                                  
    }

    if (recheck_hint()) {
        atomic_store_explicit(&g_hint, 1, PUBLISH);

#ifdef PROBE_HINT
        assert(0);                                    
#endif
        assert(search_would_find());
        return NULL;
    }

    atomic_exchange_explicit(&g_permit, ST_PARKED, TRANSITION);
    atomic_store_explicit(&g_parked, 1, PUBLISH);
    return NULL;
}

static void *producer(void *arg) {
    (void)arg;
    atomic_exchange_explicit(&g_lopri, 1, TRANSITION);
    atomic_exchange_explicit(&g_flag,  1, TRANSITION);
    atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    return NULL;
}

int main(void) {
    atomic_init(&g_lopri, 0);
    atomic_init(&g_flag, 0);
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_hint, 0);
    atomic_init(&g_found, 0);
    atomic_init(&g_parked, 0);

    pthread_t w, p;
    pthread_create(&w, NULL, worker,   NULL);
    pthread_create(&p, NULL, producer, NULL);
    pthread_join(w, NULL);
    pthread_join(p, NULL);

    (void)atomic_load(&g_hint);
    (void)atomic_load(&g_found);
    (void)atomic_load(&g_parked);
    return 0;
}
