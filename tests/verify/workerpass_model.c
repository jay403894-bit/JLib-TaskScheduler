// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define ST_EMPTY     0
#define ST_NOTIFIED  1
#define ST_PARKED    2

#define PASSES 2

static _Atomic int g_permit;    
static _Atomic int g_flag;      
static _Atomic int g_inbox;     
static _Atomic int g_ran;       
static _Atomic int g_blocked;   

static _Atomic int g_waiting;   
static _Atomic int g_delivered; 
static _Atomic int g_oswake;    
static _Atomic int g_spurious;  

#ifdef ACQ_REL_ONLY
  #define PUBLISH     memory_order_release
  #define OBSERVE     memory_order_acquire
  #define TRANSITION  memory_order_acq_rel
#else
  #define PUBLISH     memory_order_seq_cst
  #define OBSERVE     memory_order_seq_cst
  #define TRANSITION  memory_order_seq_cst
#endif

#if defined(SEARCH_MISS) && !defined(RECHECK_FLAG_ONLY) && !defined(RECHECK_FULL)
  #define RECHECK_FLAG_ONLY 1
#endif

#ifdef RECHECK_FULL
  #undef RECHECK_FLAG_ONLY
#endif

static void *os_noise(void *arg) {
    (void)arg;
    atomic_store_explicit(&g_spurious, 1, PUBLISH);
    return NULL;
}

static void leave_wait_for_search(void) {
    assert(atomic_load_explicit(&g_permit, OBSERVE) != ST_PARKED);
    atomic_store_explicit(&g_waiting, 0, PUBLISH);
}

static void *worker(void *arg) {
    (void)arg;

    for (int pass = 0; pass < PASSES; ++pass) {

        // -DCLEAR_TOP_OF_PASS: the PRE-2026-09-18 shape, kept only as a control -- it cost a
        // seq_cst store per task, and Thread.cpp:1003-1006 records why it went.
#if defined(CLEAR_TOP_OF_PASS)
        atomic_exchange_explicit(&g_flag, 0, TRANSITION);
#endif

        int got = 0;
#ifndef SEARCH_MISS
        got = atomic_exchange_explicit(&g_inbox, 0, TRANSITION) != 0;
#else
        
#endif

#ifdef CLEAR_AFTER_SEARCH
        
        atomic_exchange_explicit(&g_flag, 0, TRANSITION);
#endif

        if (got) {
            atomic_fetch_add_explicit(&g_ran, 1, PUBLISH);
            return NULL;
        }

        // THE SHIPPING SHAPE, and now the DEFAULT (Thread.cpp:1003-1010): the flag is not cleared
        // at the top of a pass, only on the way to idle and only when it is already set.
        //
        // LOAD THEN STORE, not an exchange: the code is two separate seq_cst operations, so a
        // MarkQueuedWork landing between them is erased. That is safe only because the pusher's
        // Wake() also leaves NOTIFIED, which the park CAS below consumes -- an exchange here would
        // hide that interleaving from the model. -DCLEAR_EXCHANGE restores the exchange.
#if !defined(CLEAR_TOP_OF_PASS) && !defined(CLEAR_AFTER_SEARCH)
        if (atomic_load_explicit(&g_flag, OBSERVE) != 0) {
  #ifdef CLEAR_EXCHANGE
            atomic_exchange_explicit(&g_flag, 0, TRANSITION);
  #else
            atomic_store_explicit(&g_flag, 0, TRANSITION);
  #endif
            continue;
        }
#endif

        int e = ST_NOTIFIED;
        if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed))
            continue;                       

        e = ST_EMPTY;
        if (!atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_PARKED,
                                                     TRANSITION, memory_order_relaxed)) {
            if (e == ST_NOTIFIED) {
                int e2 = ST_NOTIFIED;
                atomic_compare_exchange_strong_explicit(&g_permit, &e2, ST_EMPTY,
                                                        TRANSITION, memory_order_relaxed);
            }
            continue;
        }

        {
#ifdef RECHECK_FLAG_ONLY
            
            const int live = atomic_load_explicit(&g_flag, OBSERVE) != 0;
#else
            
            const int live = (atomic_load_explicit(&g_flag,  OBSERVE) != 0)
                          || (atomic_load_explicit(&g_inbox, OBSERVE) != 0);
#endif
            if (live) {
                int e3 = ST_PARKED;
                if (atomic_compare_exchange_strong_explicit(&g_permit, &e3, ST_EMPTY,
                                                            TRANSITION, memory_order_relaxed))
                    continue;               
                int e4 = ST_NOTIFIED;
                atomic_compare_exchange_strong_explicit(&g_permit, &e4, ST_EMPTY,
                                                        TRANSITION, memory_order_relaxed);
                continue;
            }
        }

        atomic_store_explicit(&g_waiting, 1, PUBLISH);
        if (atomic_load_explicit(&g_permit, OBSERVE) != ST_PARKED) {
            leave_wait_for_search();
            continue;
        }

        {
            const int delivered_to_me = atomic_load_explicit(&g_delivered, OBSERVE) != 0;
            const int spurious        = atomic_load_explicit(&g_spurious,  OBSERVE) != 0;

            if (!delivered_to_me && !spurious) {
#ifdef PROBE_BLOCKED
                assert(0);                  
#endif
                atomic_store_explicit(&g_blocked, 1, PUBLISH);
                return NULL;                
            }

            if (atomic_load_explicit(&g_permit, OBSERVE) == ST_PARKED) {
                
#ifdef PROBE_BLOCKED
                assert(0);
#endif
                atomic_store_explicit(&g_blocked, 1, PUBLISH);
                return NULL;
            }

            int e5 = ST_NOTIFIED;
            atomic_compare_exchange_strong_explicit(&g_permit, &e5, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed);
            leave_wait_for_search();
            
        }
    }

    return NULL;                            
}

static void *producer(void *arg) {
    (void)arg;
    atomic_exchange_explicit(&g_inbox, 1, TRANSITION);
    atomic_exchange_explicit(&g_flag,  1, TRANSITION);

#ifdef WAKE_CAS_ONLY
    
    {
        int e = ST_PARKED;
        if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_NOTIFIED,
                                                    TRANSITION, memory_order_relaxed)) {
            atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
            if (atomic_load_explicit(&g_waiting, OBSERVE) == 1)
                atomic_fetch_add_explicit(&g_delivered, 1, memory_order_relaxed);
        }
    }
#else
    {
        const int prev = atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
        if (prev == ST_PARKED) {
            atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
            
            if (atomic_load_explicit(&g_waiting, OBSERVE) == 1)
                atomic_fetch_add_explicit(&g_delivered, 1, memory_order_relaxed);
        }
    }
#endif
    return NULL;
}

int main(void) {
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_flag, 0);
    atomic_init(&g_inbox, 0);
    atomic_init(&g_ran, 0);
    atomic_init(&g_blocked, 0);
    atomic_init(&g_waiting, 0);
    atomic_init(&g_delivered, 0);
    atomic_init(&g_oswake, 0);
    atomic_init(&g_spurious, 0);

    pthread_t w, p, os;
    pthread_create(&w,  NULL, worker,   NULL);
    pthread_create(&p,  NULL, producer, NULL);
    pthread_create(&os, NULL, os_noise, NULL);
    pthread_join(w,  NULL);
    pthread_join(p,  NULL);
    pthread_join(os, NULL);

    const int inbox   = atomic_load(&g_inbox);
    const int blocked = atomic_load(&g_blocked);
    const int permit  = atomic_load(&g_permit);

    assert(!(blocked == 1 && inbox > 0 && permit != ST_NOTIFIED));

    return 0;
}
