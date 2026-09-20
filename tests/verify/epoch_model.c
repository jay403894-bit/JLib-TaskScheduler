// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Epoch-based reclamation as it is actually built (include/Epochs.h). Replaces the old
// counted_epoch_model.c, which modelled a ring of sharded reference counters -- a design this
// library does not use and never did.
//
// What ships, and what this mirrors:
//   reader   SlotEpochGuard: publish this thread's slot = CurrentEpoch() (seq_cst), read, then
//            unpin with slot = SIZE_MAX (release).                          Epochs.h:384-400
//   writer   unlink the object (release), then RetirePtr stamps it with CurrentEpoch().
//                                                                          Epochs.h:270-281
//   advance  AdvanceEpoch is an UNCONDITIONAL compare_exchange on globalEpoch -- there is no
//            gate that waits for readers.                                  Epochs.h:306-310
//   reclaim  safeEpoch = MinActiveEpoch() (the global epoch, lowered by every pinned slot);
//            an item is freed only when item.epoch < safeEpoch.            Epochs.h:213-227, 254-263
//
// The property: a reader that pinned before an object was unlinked never sees freed memory. The
// pin is what holds safeEpoch down, so the object stays in the bag until that reader unpins.
//
// ASSUMED, and true of every structure here that hands a pointer to a guarded reader: the UNLINK
// and the reader's LOAD of that pointer are seq_cst (they are CASes or exchanges -- TaskDeque's
// steal CAS, Event's head exchange, the skiplist's link CASes). Model them weaker and the model
// reports a violation the code cannot have: an acquire load may return the pre-unlink pointer even
// though the reader pinned an epoch later than the one the object was retired in, which breaks the
// ordering the whole argument rests on.
//
// Switches (each "must fail" is a negative control):
//   (none)              the design (must pass)
//   -DPIN_AFTER_READ    read the pointer, then publish the pin: the gap lets a reclaim pass see
//                       no pin and free what the reader is about to use (must fail)
//   -DNO_MIN_SCAN       reclaim with globalEpoch instead of the minimum over slots, i.e. forget
//                       that a pinned reader holds the epoch down (must fail)
//   -DFREE_AT_EQUAL     free on `epoch <= safeEpoch` instead of `<`: frees an object retired in
//                       the very epoch a reader is still pinned to (must fail)
//   -DTWO_READERS       a second reader (scope, not a control)

#include <stdatomic.h>
#include <stdint.h>
#include <pthread.h>
#include <assert.h>

#define NSLOTS 2
#define UNPINNED SIZE_MAX

static _Atomic(size_t) g_epoch = 1;          // globalEpoch; starts past 0 so `epoch < safe` can hold
static _Atomic(size_t) g_slot[NSLOTS];       // one per reader thread; UNPINNED when not in a guard

typedef struct { int v; } Obj;
static Obj  g_a = { 1 };
static _Atomic(Obj *) g_ptr;                 // the structure's one pointer
static _Atomic(int)   g_freed;               // set when the reclaimer frees the old object
static _Atomic(size_t) g_retiredEpoch;       // the stamp RetirePtr put on it
static _Atomic(int)    g_retired;            // an item is in the bag

static size_t current_epoch(void) { return atomic_load_explicit(&g_epoch, memory_order_seq_cst); }

// MinActiveEpoch: the global epoch, lowered by any slot that is pinned (Epochs.h:254-263).
static size_t min_active_epoch(void) {
    size_t m = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
    for (int i = 0; i < NSLOTS; ++i) {
        const size_t e = atomic_load_explicit(&g_slot[i], memory_order_seq_cst);
        if (e != UNPINNED && e < m) m = e;
    }
    return m;
}

// SlotEpochGuard + a read of whatever the pointer names.
static void reader(int id) {
#ifdef PIN_AFTER_READ
    Obj *p = atomic_load_explicit(&g_ptr, memory_order_seq_cst);
    atomic_store_explicit(&g_slot[id], current_epoch(), memory_order_seq_cst);
#else
    atomic_store_explicit(&g_slot[id], current_epoch(), memory_order_seq_cst);
    Obj *p = atomic_load_explicit(&g_ptr, memory_order_seq_cst);
#endif
    if (p) {
        // Touching it must be safe: the guard is open, so nothing it can reach may be freed.
        assert(!atomic_load_explicit(&g_freed, memory_order_seq_cst) || p != &g_a);
        (void)p->v;
    }
    atomic_store_explicit(&g_slot[id], UNPINNED, memory_order_release);
}

static void *t_reader0(void *a) { (void)a; reader(0); return 0; }
#ifdef TWO_READERS
static void *t_reader1(void *a) { (void)a; reader(1); return 0; }
#endif

// The writer: unlink, then retire with the epoch of the moment (Epochs.h:270-281).
static void *t_writer(void *a) {
    (void)a;
    atomic_exchange_explicit(&g_ptr, NULL, memory_order_seq_cst);   // an unlink is a CAS/exchange
    atomic_store_explicit(&g_retiredEpoch, current_epoch(), memory_order_seq_cst);
    atomic_store_explicit(&g_retired, 1, memory_order_release);
    return 0;
}

// Any thread may advance; the advance is unconditional (Epochs.h:306-310).
static void *t_advancer(void *a) {
    (void)a;
    size_t e = atomic_load_explicit(&g_epoch, memory_order_acquire);
    atomic_compare_exchange_strong_explicit(&g_epoch, &e, e + 1,
                                            memory_order_seq_cst, memory_order_relaxed);
    return 0;
}

// TryReclaim: free an item only when its stamp is strictly below the minimum active epoch.
static void *t_reclaimer(void *a) {
    (void)a;
    if (!atomic_load_explicit(&g_retired, memory_order_acquire)) return 0;
#ifdef NO_MIN_SCAN
    const size_t safe = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
#else
    const size_t safe = min_active_epoch();
#endif
    const size_t stamp = atomic_load_explicit(&g_retiredEpoch, memory_order_seq_cst);
#ifdef FREE_AT_EQUAL
    if (stamp <= safe)
#else
    if (stamp < safe)
#endif
        atomic_store_explicit(&g_freed, 1, memory_order_seq_cst);
    return 0;
}

int main(void) {
    for (int i = 0; i < NSLOTS; ++i) atomic_store_explicit(&g_slot[i], UNPINNED, memory_order_relaxed);
    atomic_store_explicit(&g_ptr, &g_a, memory_order_relaxed);

    pthread_t r0, w, adv, rec;
    pthread_create(&r0, 0, t_reader0, 0);
    pthread_create(&w, 0, t_writer, 0);
    pthread_create(&adv, 0, t_advancer, 0);
    pthread_create(&rec, 0, t_reclaimer, 0);
#ifdef TWO_READERS
    pthread_t r1;
    pthread_create(&r1, 0, t_reader1, 0);
#endif
    pthread_join(r0, 0);
    pthread_join(w, 0);
    pthread_join(adv, 0);
    pthread_join(rec, 0);
#ifdef TWO_READERS
    pthread_join(r1, 0);
#endif
    return 0;
}
