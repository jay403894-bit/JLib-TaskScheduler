// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define FS_RUNNING          0
#define FS_WANTS_SUSPEND    1
#define FS_SUSPEND_SIGNALED 2
#define FS_SUSPENDED        3
#define FS_READY            4

#ifdef SEQ_CST
  #define PUB  memory_order_seq_cst
  #define OBS  memory_order_seq_cst
  #define TRAN memory_order_seq_cst
#else
  
  #define PUB  memory_order_release
  #define OBS  memory_order_acquire
  #define TRAN memory_order_acq_rel
#endif

static _Atomic int g_status;    
static _Atomic int g_queued;    
static _Atomic int g_requeued;  
static _Atomic int g_owner;     

static pthread_mutex_t g_spin;

static void *waiter(void *arg) {
    (void)arg;

    pthread_mutex_lock(&g_spin);
#ifdef OLD_ORDERING
    
    atomic_store_explicit(&g_queued, 1, PUB);
#else
    
    atomic_store_explicit(&g_status, FS_WANTS_SUSPEND, PUB);
    atomic_store_explicit(&g_queued, 1, PUB);
#endif
    pthread_mutex_unlock(&g_spin);

#ifdef OLD_ORDERING
    atomic_store_explicit(&g_status, FS_WANTS_SUSPEND, PUB);   
#endif

#ifdef CLOBBER_SUSPEND
    
    atomic_store_explicit(&g_status, FS_WANTS_SUSPEND, PUB);
#endif

    int exp = FS_WANTS_SUSPEND;
    if (atomic_compare_exchange_strong_explicit(&g_status, &exp, FS_SUSPENDED,
                                                TRAN, memory_order_relaxed)) {
        
    } else if (exp == FS_SUSPEND_SIGNALED) {
        
        atomic_store_explicit(&g_status, FS_READY, PUB);
        atomic_fetch_add_explicit(&g_requeued, 1, memory_order_relaxed);
    }
    return NULL;
}

static void *unlocker(void *arg) {
    (void)arg;

    int popped = 0;
    pthread_mutex_lock(&g_spin);
    if (atomic_load_explicit(&g_queued, OBS) == 1) {
        atomic_store_explicit(&g_queued, 0, PUB);
        popped = 1;
        atomic_store_explicit(&g_owner, 1, PUB);   
    }
    pthread_mutex_unlock(&g_spin);

    if (!popped) return NULL;

    for (;;) {
        int s = atomic_load_explicit(&g_status, OBS);
        if (s == FS_SUSPENDED) {
            int e = FS_SUSPENDED;
            if (atomic_compare_exchange_strong_explicit(&g_status, &e, FS_READY,
                                                        TRAN, memory_order_relaxed))
                atomic_fetch_add_explicit(&g_requeued, 1, memory_order_relaxed);
            return NULL;
        } else if (s == FS_WANTS_SUSPEND) {
            int e = FS_WANTS_SUSPEND;
            if (atomic_compare_exchange_strong_explicit(&g_status, &e, FS_SUSPEND_SIGNALED,
                                                        TRAN, memory_order_relaxed))
                return NULL;               
            
        } else {
            
            return NULL;
        }
    }
}

int main(void) {
    atomic_init(&g_status, FS_RUNNING);
    atomic_init(&g_queued, 0);
    atomic_init(&g_requeued, 0);
    atomic_init(&g_owner, 0);
    pthread_mutex_init(&g_spin, NULL);

    pthread_t w, u;
    pthread_create(&w, NULL, waiter,   NULL);
    pthread_create(&u, NULL, unlocker, NULL);
    pthread_join(w, NULL);
    pthread_join(u, NULL);

    const int owner    = atomic_load(&g_owner);
    const int status   = atomic_load(&g_status);
    const int requeued = atomic_load(&g_requeued);

    assert(!(owner == 1 && status == FS_SUSPENDED && requeued == 0));

    assert(requeued <= 1);

    assert(!(owner == 0 && requeued > 0));

    return 0;
}
