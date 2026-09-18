// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define ST_AWAKE     0
#define ST_GOING     1     
#define ST_SLEEPING  2

static _Atomic int g_state;      
static _Atomic int g_work;       
static _Atomic int g_lanewake;   
static _Atomic int g_notified;   

#ifdef ACQ_REL_ONLY
  
  #define PUBLISH      memory_order_release
  #define OBSERVE      memory_order_acquire
  #define TRANSITION   memory_order_acq_rel
#else
  #define PUBLISH      memory_order_seq_cst
  #define OBSERVE      memory_order_seq_cst
  #define TRANSITION   memory_order_seq_cst
#endif

#ifdef WEAK_LANEWAKE
  #define LANE_PUBLISH  memory_order_release
  #define LANE_OBSERVE  memory_order_acquire
#else
  #define LANE_PUBLISH  PUBLISH
  #define LANE_OBSERVE  OBSERVE
#endif

static void *worker(void *arg) {
    (void)arg;

    int expected = ST_AWAKE;
    atomic_compare_exchange_strong_explicit(&g_state, &expected, ST_GOING,
                                            TRANSITION, memory_order_relaxed);

    const int work = atomic_load_explicit(&g_work,     OBSERVE);
    const int lane = atomic_load_explicit(&g_lanewake, LANE_OBSERVE);

    if (work == 0 && lane == 0) {
        int e2 = ST_GOING;
        if (atomic_compare_exchange_strong_explicit(&g_state, &e2, ST_SLEEPING,
                                                    TRANSITION, memory_order_relaxed)) {
            
        }
    } else {
        
        atomic_store_explicit(&g_state, ST_AWAKE, memory_order_relaxed);
    }
    return NULL;
}

static void *pusher(void *arg) {
    (void)arg;

    atomic_fetch_add_explicit(&g_work, 1, PUBLISH);

    int s = atomic_load_explicit(&g_state, OBSERVE);
    if (s == ST_GOING || s == ST_SLEEPING) {
        
        atomic_fetch_add_explicit(&g_notified, 1, memory_order_relaxed);
    }
    return NULL;
}

static void *lane_setter(void *arg) {
    (void)arg;

    atomic_store_explicit(&g_lanewake, 1, LANE_PUBLISH);

    int s = atomic_load_explicit(&g_state, OBSERVE);
    if (s == ST_GOING || s == ST_SLEEPING) {
        atomic_fetch_add_explicit(&g_notified, 1, memory_order_relaxed);
    }
    return NULL;
}

int main(void) {
    atomic_init(&g_state, ST_AWAKE);
    atomic_init(&g_work, 0);
    atomic_init(&g_lanewake, 0);
    atomic_init(&g_notified, 0);

    pthread_t w;
    pthread_create(&w,  NULL, worker,           NULL);

    pthread_t ln;
    pthread_create(&ln, NULL, lane_setter, NULL);

#ifndef LANE_ONLY
    pthread_t p;
    pthread_create(&p, NULL, pusher, NULL);
#endif

    pthread_join(w,  NULL);
    pthread_join(ln, NULL);
#ifndef LANE_ONLY
    pthread_join(p, NULL);
#endif

    const int work      = atomic_load(&g_work);
    const int lanewake  = atomic_load(&g_lanewake);
    const int state     = atomic_load(&g_state);
    const int notified  = atomic_load(&g_notified);

    assert(!((work > 0 || lanewake > 0) && state == ST_SLEEPING && notified == 0));

    return 0;
}
