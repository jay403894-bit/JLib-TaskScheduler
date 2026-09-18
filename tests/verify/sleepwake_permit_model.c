// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define ST_EMPTY     0
#define ST_NOTIFIED  1
#define ST_PARKED    2

static _Atomic int g_permit;   
static _Atomic int g_work;     
static _Atomic int g_oswake;   

static _Atomic int g_waiting;

static _Atomic int g_delivered;

static _Atomic int g_left_wait;

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

static void leave_wait_for_search(void) {
    assert(atomic_load_explicit(&g_permit, OBSERVE) != ST_PARKED);
    atomic_store_explicit(&g_waiting,   0, PUBLISH);
    atomic_store_explicit(&g_left_wait, 1, PUBLISH);
}

static void *worker(void *arg) {
    (void)arg;

    int e = ST_NOTIFIED;
    if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_EMPTY,
                                                TRANSITION, memory_order_relaxed))
        return NULL;                       

    e = ST_EMPTY;
    if (!atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_PARKED,
                                                 TRANSITION, memory_order_relaxed)) {
        
        if (e == ST_NOTIFIED) {
            int e2 = ST_NOTIFIED;
            atomic_compare_exchange_strong_explicit(&g_permit, &e2, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed);
        }
        return NULL;                       
    }

#ifndef NO_RECHECK
    
    if (atomic_load_explicit(&g_work, OBSERVE) != 0) {
        int e3 = ST_PARKED;
        if (atomic_compare_exchange_strong_explicit(&g_permit, &e3, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed))
            return NULL;                   
        
        int e4 = ST_NOTIFIED;
        atomic_compare_exchange_strong_explicit(&g_permit, &e4, ST_EMPTY,
                                                TRANSITION, memory_order_relaxed);
        return NULL;
    }
#endif

    atomic_store_explicit(&g_waiting, 1, PUBLISH);

#ifndef NO_PREWAIT_REREAD
    
    if (atomic_load_explicit(&g_permit, OBSERVE) != ST_PARKED) {
        leave_wait_for_search();
        return NULL;                       
    }
#endif

    {
        
        const int delivered_to_me = atomic_load_explicit(&g_delivered, OBSERVE) != 0;
        const int spurious        = atomic_load_explicit(&g_spurious,  OBSERVE) != 0;
        if (!delivered_to_me && !spurious)
            return NULL;                   

        const int p = atomic_load_explicit(&g_permit, OBSERVE);

        if (p == ST_PARKED) {
#ifdef NO_SPURIOUS_REBLOCK
            
            leave_wait_for_search();
            return NULL;
#else
            
            return NULL;
#endif
        }

#ifdef POSTWAIT_STORE
        
        atomic_store_explicit(&g_permit, ST_EMPTY, PUBLISH);
#else
        {
            
            int e5 = ST_NOTIFIED;
            atomic_compare_exchange_strong_explicit(&g_permit, &e5, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed);
        }
#endif
        leave_wait_for_search();
        return NULL;
    }
}

static void note_wake_performed(void) {
    atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
    
    if (atomic_load_explicit(&g_waiting, OBSERVE) == 1)
        atomic_fetch_add_explicit(&g_delivered, 1, memory_order_relaxed);
}

static void *os_noise(void *arg) {
    (void)arg;
    atomic_store_explicit(&g_spurious, 1, PUBLISH);
    return NULL;
}

static void *waker(void *arg) {
    (void)arg;
    atomic_fetch_add_explicit(&g_work, 1, PUBLISH);

#ifdef WAKE_ALWAYS_SYSCALL
    
    atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    note_wake_performed();
#elif defined(WAKE_CAS_ONLY)
    
    int e = ST_PARKED;
    if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_NOTIFIED,
                                                TRANSITION, memory_order_relaxed))
        note_wake_performed();
#else
    
    const int prev = atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    if (prev == ST_PARKED)
        note_wake_performed();
    
#endif
    return NULL;
}

int main(void) {
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_work, 0);
    atomic_init(&g_oswake, 0);
    atomic_init(&g_waiting, 0);
    atomic_init(&g_delivered, 0);
    atomic_init(&g_left_wait, 0);
    atomic_init(&g_spurious, 0);

    pthread_t w, p1, p2, os;
    pthread_create(&w,  NULL, worker, NULL);
    pthread_create(&p1, NULL, waker,  NULL);
    pthread_create(&p2, NULL, waker,  NULL);
    pthread_create(&os, NULL, os_noise, NULL);
    pthread_join(w,  NULL);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(os, NULL);

    const int work    = atomic_load(&g_work);
    const int permit  = atomic_load(&g_permit);
    const int oswake  = atomic_load(&g_oswake);
    const int waiting   = atomic_load(&g_waiting);
    const int delivered = atomic_load(&g_delivered);
    const int left_wait = atomic_load(&g_left_wait);

    assert(!(work > 0 && permit == ST_PARKED && oswake == 0));

    assert(!(work > 0 && waiting == 1 && permit == ST_NOTIFIED && delivered == 0));

    assert(oswake <= 1);

    (void)left_wait;

    return 0;
}
