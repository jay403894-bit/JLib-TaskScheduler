// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// GenMC model of the Linden-Jonsson skiplist priority queue (OPODIS 2013; UU tech report 2018-003),
// Algorithms 2-5. The delete flag of a node lives in the low bit of its PREDECESSOR's next[0];
// deleted nodes form a prefix; DeleteMin claims with one fetch_or and cuts the prefix in batches.
//
// Checked: every value popped at most once; conservation (popped + live = inserted); no recycled
// node reachable from head at any level; a node is never recycled while `inserting`; the ordering
// allowed by a priority queue for the configuration.
//
// Run (GenMC 0.18, --disable-estimation -unroll=8). Clean:
//   -DCONFIG_TWO_DEL; -DINS_KEY=1UL / 3UL, each with and without -DPRE_DELETED; -DBOUND=2;
//   -DCONFIG_INS_TWO_DEL with INS_KEY 1UL / 3UL, +/- PRE_DELETED, and BOUND=2 (up to 1,556 execs).
// Must fail:
//   -DPAPER_OBSHEAD  -DCONFIG_INS_TWO_DEL -DINS_KEY=1UL   access past the list (head moved back)
//   -DNO_INSERTING   -DCONFIG_INS_TWO_DEL -DINS_KEY=1UL   recycled node reachable
//   -DNO_SKEW_BREAK  -DCONFIG_INS_TWO_DEL -DINS_KEY=1UL   recycled node reachable
//   -DHEAD_UNMARKED  -DCONFIG_INS_TWO_DEL -DINS_KEY=3UL   popped node live again
//   -DRELAXED_LINK   -DINS_KEY=1UL                        non-atomic race on key/value
// NO_INSERTING and NO_SKEW_BREAK do not fail with -DPRE_DELETED: that setup cannot reach them.

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>
#include <assert.h>

#define LEVELS   2
#define POOL     8
#define KEY_HEAD 0UL
#define KEY_TAIL 99UL
#ifndef BOUND
#define BOUND    1          // BoundOffset: 1 = every DeleteMin tries the batch cut
#endif

#ifdef RELAXED_LINK
  #define LINK_ORD memory_order_relaxed
#else
  #define LINK_ORD memory_order_acq_rel
#endif
#define LD memory_order_acquire

typedef struct Node {
    unsigned long     key;          // plain: published by the level-0 link CAS
    int               value;        // plain: read by the DeleteMin that wins the node
    int               height;
    _Atomic int       inserting;
    _Atomic uintptr_t next[LEVELS]; // next[0] low bit = delete flag of the SUCCESSOR
} Node;

static uintptr_t pack(Node *p, int m) { return (uintptr_t)p | (uintptr_t)m; }
static Node *ptr_of(uintptr_t v)      { return (Node *)(v & ~(uintptr_t)1); }
static int   mark_of(uintptr_t v)     { return (int)(v & 1); }

static Node        g_pool[POOL];
static _Atomic int g_poolNext;
static Node       *g_head, *g_tail;
static _Atomic int g_recycled[POOL];
static _Atomic int g_popped[POOL];

static int slot_of(const Node *n) { return (int)(n - g_pool); }

static Node *alloc_node(unsigned long key, int height) {
    int i = atomic_fetch_add_explicit(&g_poolNext, 1, memory_order_relaxed);
    assert(i < POOL);
    Node *n = &g_pool[i];
    n->key = key;
    n->value = (int)key;
    n->height = height;
    atomic_store_explicit(&n->inserting, 0, memory_order_relaxed);
    for (int l = 0; l < LEVELS; ++l)
        atomic_store_explicit(&n->next[l], 0, memory_order_relaxed);
    return n;
}

static int deleted_flag(Node *n) {   // n.d: is n's successor deleted
    return mark_of(atomic_load_explicit(&n->next[0], LD));
}

static void recycle(Node *n) {
#ifndef NO_INSERTING
    assert(atomic_load_explicit(&n->inserting, LD) == 0);   // invariant 4a
#endif
    int prev = atomic_fetch_add_explicit(&g_recycled[slot_of(n)], 1, memory_order_relaxed);
    assert(prev == 0);
}

// Algorithm 5.
static Node *locate_preds(unsigned long k, Node **preds, Node **succs) {
    Node *pred = g_head, *del = NULL;
    for (int i = LEVELS - 1; i >= 0; --i) {
        // <cur, d> <- <pred.next[i], pred.d>: at level 0 one load gives both.
        uintptr_t raw = atomic_load_explicit(&pred->next[i], LD);
        Node *cur = ptr_of(raw);
        int d = (i == 0) ? mark_of(raw) : deleted_flag(pred);
        while (cur->key < k || deleted_flag(cur) || (d && i == 0)) {
            if (d && i == 0) del = cur;
            pred = cur;
            raw  = atomic_load_explicit(&pred->next[i], LD);
            cur  = ptr_of(raw);
            d    = (i == 0) ? mark_of(raw) : deleted_flag(pred);
        }
        preds[i] = pred;
        succs[i] = cur;
    }
    return del;
}

// Algorithm 4.
static void insert(unsigned long k, int height) {
    Node *preds[LEVELS], *succs[LEVELS], *del;
    Node *n = alloc_node(k, height);
    atomic_store_explicit(&n->inserting, 1, memory_order_relaxed);
    for (;;) {
        del = locate_preds(k, preds, succs);
        atomic_store_explicit(&n->next[0], pack(succs[0], 0), memory_order_relaxed);
        uintptr_t exp = pack(succs[0], 0);
        if (atomic_compare_exchange_strong_explicit(&preds[0]->next[0], &exp, pack(n, 0),
                                                    LINK_ORD, memory_order_relaxed))
            break;
    }
    int i = 1;
    while (i < height) {
        atomic_store_explicit(&n->next[i], pack(succs[i], 0), memory_order_relaxed);
#ifndef NO_SKEW_BREAK
        if (deleted_flag(n) || deleted_flag(succs[i]) || succs[i] == del) break;
#else
        if (deleted_flag(n)) break;
#endif
        uintptr_t exp = pack(succs[i], 0);
        if (atomic_compare_exchange_strong_explicit(&preds[i]->next[i], &exp, pack(n, 0),
                                                    LINK_ORD, memory_order_relaxed)) {
            ++i;
        } else {
            del = locate_preds(k, preds, succs);
            if (succs[0] != n) break;
        }
    }
    atomic_store_explicit(&n->inserting, 0, memory_order_release);
}

// Algorithm 3.
static void restructure(void) {
    int i = LEVELS - 1;
    Node *pred = g_head;
    while (i > 0) {
        uintptr_t hraw = atomic_load_explicit(&g_head->next[i], LD);
        Node *h   = ptr_of(hraw);
        Node *cur = ptr_of(atomic_load_explicit(&pred->next[i], LD));
        if (!deleted_flag(h)) { --i; continue; }
        while (deleted_flag(cur)) {
            pred = cur;
            cur  = ptr_of(atomic_load_explicit(&pred->next[i], LD));
        }
        if (atomic_compare_exchange_strong_explicit(&g_head->next[i], &hraw, pack(cur, 0),
                                                    LINK_ORD, memory_order_relaxed))
            --i;
    }
}

// Algorithm 2. Returns the popped node's value, or -1 for Empty.
static int delete_min(void) {
    Node *x = g_head, *newhead = NULL;
    int offset = 0;
    uintptr_t obshead = atomic_load_explicit(&g_head->next[0], LD);
    uintptr_t nxt;
    do {
        nxt = atomic_load_explicit(&x->next[0], LD);
        if (ptr_of(nxt) == g_tail) return -1;
#ifndef NO_INSERTING
        if (newhead == NULL && atomic_load_explicit(&x->inserting, LD)) newhead = x;
#endif
        if (!mark_of(nxt))   // the reference code skips the RMW when the flag is already set
            nxt = atomic_fetch_or_explicit(&x->next[0], 1, memory_order_acq_rel);
        ++offset;
        x = ptr_of(nxt);
    } while (mark_of(nxt));

    int v = x->value;
    int prev = atomic_fetch_add_explicit(&g_popped[slot_of(x)], 1, memory_order_relaxed);
    assert(prev == 0);

    if (offset < BOUND) return v;
    if (newhead == NULL) newhead = x;
    // Compare against the RAW value read at the start, as the reference code does. The paper's
    // Algorithm 2 line 16 compares against <obshead, 1>: while the head is still unmarked (no
    // delete yet), an insert in front, a claim and another thread's cut can return head.next[0]
    // to <obshead, 1>, and the CAS then moves the head BACKWARD onto a recycled node.
#ifdef PAPER_OBSHEAD
    uintptr_t exp = pack(ptr_of(obshead), 1);
#else
    uintptr_t exp = obshead;
#endif
#ifdef HEAD_UNMARKED
    if (atomic_compare_exchange_strong_explicit(&g_head->next[0], &exp, pack(newhead, 0),
                                                LINK_ORD, memory_order_relaxed)) {
#else
    if (atomic_compare_exchange_strong_explicit(&g_head->next[0], &exp, pack(newhead, 1),
                                                LINK_ORD, memory_order_relaxed)) {
#endif
        restructure();
        Node *cur = ptr_of(obshead);
        while (cur != newhead) {
            Node *n2 = ptr_of(atomic_load_explicit(&cur->next[0], LD));
            recycle(cur);
            cur = n2;
        }
    }
    return v;
}

// ---- harness ------------------------------------------------------------------------------------

#ifndef INS_KEY
#define INS_KEY 1UL          // 1 = the new node is the minimum and races the claim; 3 = mid-list
#endif
#ifndef INS_HEIGHT
#define INS_HEIGHT 2
#endif

static int g_res[3];
static Node *g_A, *g_B;

static void *t_ins(void *a) { (void)a; insert(INS_KEY, INS_HEIGHT); return NULL; }
static void *t_del(void *a) { g_res[(intptr_t)a] = delete_min(); return NULL; }

static void setup(void) {
    g_head = alloc_node(KEY_HEAD, LEVELS);
    g_tail = alloc_node(KEY_TAIL, LEVELS);
    g_A = alloc_node(2UL, 2);
    g_B = alloc_node(4UL, 1);
    atomic_store_explicit(&g_B->next[0], pack(g_tail, 0), memory_order_relaxed);
    atomic_store_explicit(&g_A->next[0], pack(g_B, 0), memory_order_relaxed);
    atomic_store_explicit(&g_A->next[1], pack(g_tail, 0), memory_order_relaxed);
    atomic_store_explicit(&g_head->next[0], pack(g_A, 0), memory_order_relaxed);
    atomic_store_explicit(&g_head->next[1], pack(g_A, 0), memory_order_relaxed);
#ifdef PRE_DELETED
    // A prefix already exists: 2 is deleted (flag in head), so the live minimum is 4.
    atomic_store_explicit(&g_head->next[0], pack(g_A, 1), memory_order_relaxed);
#endif
}

static void check_no_recycled_reachable(void) {
    for (int l = 0; l < LEVELS; ++l) {
        Node *cur = ptr_of(atomic_load_explicit(&g_head->next[l], LD));
        int steps = 0;
        while (cur != g_tail) {
            assert(atomic_load_explicit(&g_recycled[slot_of(cur)], memory_order_relaxed) == 0);
            cur = ptr_of(atomic_load_explicit(&cur->next[l], LD));
            assert(++steps <= POOL);
        }
    }
}

// Level 0, skipping the deleted prefix: keys increase, and each live node is un-popped.
static int count_live(void) {
    Node *pred = g_head;
    Node *cur = ptr_of(atomic_load_explicit(&g_head->next[0], LD));
    unsigned long last = KEY_HEAD;
    int live = 0, steps = 0;
    while (cur != g_tail) {
        if (!deleted_flag(pred)) {
            assert(cur->key > last);
            last = cur->key;
            assert(atomic_load_explicit(&g_popped[slot_of(cur)], memory_order_relaxed) == 0);
            ++live;
        } else {
            assert(live == 0);   // invariant 1b: the deleted nodes are a prefix
        }
        pred = cur;
        cur = ptr_of(atomic_load_explicit(&cur->next[0], LD));
        assert(++steps <= POOL);
    }
    return live;
}

int main(void) {
    setup();
    int ndel = 0;
    pthread_t ti, t1, t2;
#if defined(CONFIG_TWO_DEL)
    pthread_create(&t1, NULL, t_del, (void *)0);
    pthread_create(&t2, NULL, t_del, (void *)1);
    ndel = 2;
    pthread_join(t1, NULL); pthread_join(t2, NULL);
#elif defined(CONFIG_INS_TWO_DEL)
    pthread_create(&ti, NULL, t_ins, NULL);
    pthread_create(&t1, NULL, t_del, (void *)0);
    pthread_create(&t2, NULL, t_del, (void *)1);
    ndel = 2;
    pthread_join(ti, NULL); pthread_join(t1, NULL); pthread_join(t2, NULL);
#else
    pthread_create(&ti, NULL, t_ins, NULL);
    pthread_create(&t1, NULL, t_del, (void *)0);
    ndel = 1;
    pthread_join(ti, NULL); pthread_join(t1, NULL);
#endif

    check_no_recycled_reachable();

    int inserted = 2;
#if !defined(CONFIG_TWO_DEL)
    inserted = 3;
#endif
#ifdef PRE_DELETED
    inserted -= 1;
#endif
    int popped = 0;
    for (int d = 0; d < ndel; ++d) {
#ifndef PRE_DELETED
        assert(g_res[d] != -1);                   // {2,4} present from the start: never empty
#endif
        if (g_res[d] != -1) ++popped;
    }
#ifdef PRE_DELETED
    // Live {4} plus the insert: two pops can see at most one Empty.
    if (ndel == 2) assert(!(g_res[0] == -1 && g_res[1] == -1));
#endif
    assert(popped + count_live() == inserted);    // conservation

    // Priority order. Initial live keys {2,4} ({4} with PRE_DELETED); the insert adds INS_KEY.
#ifndef PRE_DELETED
    if (ndel == 1)
        assert(g_res[0] == 2 || (INS_KEY < 2 && g_res[0] == (int)INS_KEY));
  #if defined(CONFIG_INS_TWO_DEL) || defined(CONFIG_TWO_DEL)
    assert(!(g_res[0] != 2 && g_res[1] != 2));    // 2 is present from the start: one pop gets it
  #endif
#endif
    return 0;
}
