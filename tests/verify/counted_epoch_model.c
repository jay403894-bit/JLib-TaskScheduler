// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stdint.h>
#include <pthread.h>
#include <assert.h>

#define NSLOTS   2                       
#define SLOT(e)  ((e) & (NSLOTS - 1))    
#define NSHARDS  2                       
#define MAX_TRY  2                        
#define MAX_ADV  2                        

static _Atomic(unsigned) g_epoch = 0;
static _Atomic(int)      g_counters[NSLOTS][NSHARDS];

static int ring_total(unsigned ring) {
    int t = 0;
    for (unsigned s = 0; s < NSHARDS; ++s)
        t += atomic_load_explicit(&g_counters[ring][s], memory_order_seq_cst);
    return t;
}

typedef struct { int v; } Obj;
static Obj                g_obj      = { 7 };
static _Atomic(Obj *)     g_ptr      = &g_obj;
static _Atomic(int)       g_freed    = 0;

static unsigned enter_epoch(unsigned shard) {
    for (int attempt = 0; attempt < MAX_TRY; ++attempt) {
        unsigned e = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
        atomic_fetch_add_explicit(&g_counters[SLOT(e)][shard], 1, memory_order_seq_cst);

#ifndef NO_REVALIDATE
        
        unsigned again = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
        if (again != e) {
            atomic_fetch_sub_explicit(&g_counters[SLOT(e)][shard], 1, memory_order_seq_cst);
            continue;
        }
#endif
        return e;
    }
    return (unsigned)-1;
}

static void leave_epoch(unsigned token, unsigned shard) {
    atomic_fetch_sub_explicit(&g_counters[SLOT(token)][shard], 1, memory_order_seq_cst);
}

static void *reader(void *arg) {
    (void)arg;
    const unsigned enter_shard = 0, leave_shard = NSHARDS - 1;   
    unsigned tok = enter_epoch(enter_shard);
    if (tok != (unsigned)-1) {
        
        Obj *p = atomic_load_explicit(&g_ptr, memory_order_seq_cst);
        if (p) {
            
            assert(atomic_load_explicit(&g_freed, memory_order_seq_cst) == 0);
        }
        leave_epoch(tok, leave_shard);
    }
    return 0;
}

static int try_advance(void) {
    unsigned e = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
    unsigned next = e + 1;

#ifndef NO_ADVANCE_GATE
    
    if (ring_total(SLOT(next)) != 0) return 0;
#endif

    return atomic_compare_exchange_strong_explicit(
        &g_epoch, &e, next, memory_order_seq_cst, memory_order_seq_cst);
}

static void *reclaimer(void *arg) {
    (void)arg;

    atomic_store_explicit(&g_ptr, (Obj *)0, memory_order_seq_cst);
    unsigned r = atomic_load_explicit(&g_epoch, memory_order_seq_cst);

    for (int i = 0; i < MAX_ADV; ++i) try_advance();

    unsigned cur = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
    int blocked = 0;
    for (unsigned k = 0; k < NSLOTS; ++k) {
        
        unsigned e = cur - ((cur - k) & (NSLOTS - 1));
        if (e <= r && ring_total(k) != 0) blocked = 1;
    }
    if (!blocked)
        atomic_store_explicit(&g_freed, 1, memory_order_seq_cst);

    return 0;
}

int main(void) {
    pthread_t tr, tr2, tc;
    pthread_create(&tr,  0, reader,    0);
#ifdef TWO_READERS
    pthread_create(&tr2, 0, reader,    0);
#endif
    pthread_create(&tc,  0, reclaimer, 0);
    pthread_join(tr, 0);
#ifdef TWO_READERS
    pthread_join(tr2, 0);
#endif
    pthread_join(tc, 0);
    return 0;
}
