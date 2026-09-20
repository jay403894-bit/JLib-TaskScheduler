// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>
#include <assert.h>

#define LEVELS   2          
#define POOL     12         
#define KEY_HEAD 0UL
#define KEY_TAIL 99UL

#ifdef RELAXED_LINKS
  #define LOAD_ORD memory_order_relaxed
  #define CAS_ORD  memory_order_relaxed
#else
  #define LOAD_ORD memory_order_acquire   
  #define CAS_ORD  memory_order_acq_rel
#endif

typedef struct Node {
    unsigned long      key;
    int                topLevel;
    _Atomic(int *)     data;              
    _Atomic uintptr_t  next[LEVELS];      
} Node;

_Static_assert(_Alignof(Node) >= 2, "tagged pointers need a spare low bit");

static uintptr_t pack(Node *p, bool mark) {
    return (uintptr_t)p | (mark ? 1u : 0u);
}
static Node *ptr_of(uintptr_t v)  { return (Node *)(v & ~(uintptr_t)1); }
static bool  mark_of(uintptr_t v) { return (v & (uintptr_t)1) != 0; }

static Node        g_pool[POOL];
static int         g_payload[POOL];       
static _Atomic int g_poolNext;

static Node *g_head;
static Node *g_tail;

static _Atomic int g_retired[POOL];      
static _Atomic int g_popped[POOL];       
static _Atomic int g_addWins;            

static _Atomic int g_added3;

static int slot_of(const Node *n) { return (int)(n - g_pool); }

static Node *alloc_node(unsigned long key, int top) {
    int i = atomic_fetch_add_explicit(&g_poolNext, 1, memory_order_relaxed);
    assert(i < POOL);                    
    Node *n = &g_pool[i];
    n->key      = key;
    n->topLevel = top;
    g_payload[i] = (int)key;
    atomic_store_explicit(&n->data, &g_payload[i], memory_order_relaxed);
    for (int l = 0; l < LEVELS; ++l)
        atomic_store_explicit(&n->next[l], 0, memory_order_relaxed);
    return n;
}

static void retire(Node *n) {
    int prev = atomic_fetch_add_explicit(&g_retired[slot_of(n)], 1, memory_order_relaxed);
    assert(prev == 0);
}

static bool find_node(unsigned long key, Node **preds, Node **succs) {
retry:
    for (;;) {
        Node *pred = g_head;
        for (int level = LEVELS - 1; level >= 0; --level) {
            uintptr_t curRaw = atomic_load_explicit(&pred->next[level], LOAD_ORD);
            Node *curr = ptr_of(curRaw);
            for (;;) {
                uintptr_t succRaw = atomic_load_explicit(&curr->next[level], LOAD_ORD);
                Node *succ = ptr_of(succRaw);
                while (mark_of(succRaw)) {
                    uintptr_t expected = pack(curr, false);
                    bool won = atomic_compare_exchange_strong_explicit(
                        &pred->next[level], &expected, pack(succ, false),
                        CAS_ORD, memory_order_relaxed);
#ifdef NO_FIND_RESTART
                    (void)won;   
#else
                    if (!won) goto retry;
#endif
                    curr    = ptr_of(atomic_load_explicit(&pred->next[level], LOAD_ORD));
                    succRaw = atomic_load_explicit(&curr->next[level], LOAD_ORD);
                    succ    = ptr_of(succRaw);
                }
                if (curr->key < key) { pred = curr; curr = succ; }
                else break;
            }
            preds[level] = pred;
            succs[level] = curr;
        }
        return succs[0]->key == key;
    }
}

static void mark_upper(Node *victim) {
#ifndef NO_MARK_UPPER
    for (int level = victim->topLevel; level >= 1; --level) {
        uintptr_t raw = atomic_load_explicit(&victim->next[level], LOAD_ORD);
        while (!mark_of(raw)) {
            if (atomic_compare_exchange_weak_explicit(
                    &victim->next[level], &raw, pack(ptr_of(raw), true),
                    CAS_ORD, memory_order_relaxed))
                break;
            
        }
    }
#else
    (void)victim;   
#endif
}

static bool add_key(unsigned long key, int topLevel) {
    Node *preds[LEVELS];
    Node *succs[LEVELS];

    for (;;) {
        if (find_node(key, preds, succs)) return false;      

        Node *node = alloc_node(key, topLevel);
        for (int level = 0; level <= topLevel; ++level)
            atomic_store_explicit(&node->next[level], pack(succs[level], false),
                                  memory_order_relaxed);

        uintptr_t expected = pack(succs[0], false);
        if (!atomic_compare_exchange_strong_explicit(
                &preds[0]->next[0], &expected, pack(node, false),
                CAS_ORD, memory_order_relaxed))
            continue;        

        for (int level = 1; level <= topLevel; ++level) {
            for (;;) {
                uintptr_t exp = pack(succs[level], false);
                if (atomic_compare_exchange_strong_explicit(
                        &preds[level]->next[level], &exp, pack(node, false),
                        CAS_ORD, memory_order_relaxed))
                    break;
                
                find_node(key, preds, succs);
                if (mark_of(atomic_load_explicit(&node->next[level], LOAD_ORD)))
                    return true;
            }
        }
        return true;
    }
}

static bool remove_key(unsigned long key) {
    Node *preds[LEVELS];
    Node *succs[LEVELS];

    for (;;) {
        if (!find_node(key, preds, succs)) return false;
        Node *victim = succs[0];

        mark_upper(victim);

        uintptr_t raw = atomic_load_explicit(&victim->next[0], LOAD_ORD);
        for (;;) {
#ifndef REMOVE_NO_MARK_CHECK
            if (mark_of(raw)) return false;              
#endif
#ifdef OWNERSHIP_CAS_NOT_RMW
            
            atomic_store_explicit(&victim->next[0], pack(ptr_of(raw), true), CAS_ORD);
            break;
#else
            if (atomic_compare_exchange_weak_explicit(
                    &victim->next[0], &raw, pack(ptr_of(raw), true),
                    CAS_ORD, memory_order_relaxed))
                break;
#endif
        }

        find_node(key, preds, succs);    
        retire(victim);
        return true;
    }
}

static int *pop_min(void) {
    Node *preds[LEVELS];
    Node *succs[LEVELS];

    for (;;) {
        Node *curr = ptr_of(atomic_load_explicit(&g_head->next[0], LOAD_ORD));
        if (curr == g_tail || curr == NULL) return NULL;

        uintptr_t raw = atomic_load_explicit(&curr->next[0], LOAD_ORD);
#ifndef POP_NO_MARK_CHECK
        if (mark_of(raw)) {                  
            curr = ptr_of(raw);
            if (curr == g_tail || curr == NULL) return NULL;
            continue;
        }
#endif
        mark_upper(curr);

        if (!atomic_compare_exchange_strong_explicit(
                &curr->next[0], &raw, pack(ptr_of(raw), true),
                CAS_ORD, memory_order_relaxed))
            continue;                        

        int *value = atomic_exchange_explicit(&curr->data, NULL, memory_order_acq_rel);
        assert(value != NULL);               
        int prev = atomic_fetch_add_explicit(&g_popped[slot_of(curr)], 1,
                                             memory_order_relaxed);
        assert(prev == 0);                   

        find_node(curr->key, preds, succs);  
        retire(curr);
        return value;
    }
}

#define HARNESS_BODY __attribute__((unused))

HARNESS_BODY static void *t_pop(void *arg) {
    (void)arg;
    pop_min();
    return NULL;
}

HARNESS_BODY static void *t_remove(void *arg) {
    (void)arg;
    remove_key(1UL);
    return NULL;
}

HARNESS_BODY static void *t_add(void *arg) {
    (void)arg;
    if (add_key(3UL, 1)) {
        atomic_fetch_add_explicit(&g_addWins, 1, memory_order_relaxed);
        atomic_store_explicit(&g_added3, 1, memory_order_relaxed);
    }
    return NULL;
}

HARNESS_BODY static void *t_remove2(void *arg) {
    (void)arg;
    remove_key(2UL);
    return NULL;
}

static void check_level0_sorted(void) {
    Node *cur = ptr_of(atomic_load_explicit(&g_head->next[0], memory_order_acquire));
    unsigned long last = KEY_HEAD;
    int steps = 0;
    while (cur != NULL && cur != g_tail) {
        assert(cur->key > last);            
        last = cur->key;
        cur  = ptr_of(atomic_load_explicit(&cur->next[0], memory_order_acquire));
        assert(++steps <= POOL);            
    }
    assert(cur == g_tail);                  
}

HARNESS_BODY static bool reachable_level0(unsigned long key) {
    Node *cur = ptr_of(atomic_load_explicit(&g_head->next[0], memory_order_acquire));
    int steps = 0;
    while (cur != NULL && cur != g_tail) {
        if (cur->key == key)
            return !mark_of(atomic_load_explicit(&cur->next[0], memory_order_acquire));
        cur = ptr_of(atomic_load_explicit(&cur->next[0], memory_order_acquire));
        if (++steps > POOL) return false;
    }
    return false;
}

#if !defined(CONFIG_ADD_RACE) && !defined(CONFIG_ADD_POP)
static void check_no_retired_reachable(void) {
    for (int level = 0; level < LEVELS; ++level) {
        Node *cur = ptr_of(atomic_load_explicit(&g_head->next[level], memory_order_acquire));
        int steps = 0;
        while (cur != NULL && cur != g_tail) {
            assert(atomic_load_explicit(&g_retired[slot_of(cur)], memory_order_relaxed) == 0);
            cur = ptr_of(atomic_load_explicit(&cur->next[level], memory_order_acquire));
            assert(++steps <= POOL);
        }
    }
}
#endif

static void setup(void) {
    g_head = alloc_node(KEY_HEAD, LEVELS - 1);
    g_tail = alloc_node(KEY_TAIL, LEVELS - 1);
    for (int l = 0; l < LEVELS; ++l)
        atomic_store_explicit(&g_tail->next[l], 0, memory_order_relaxed);

    Node *a = alloc_node(1UL, 1);    
    Node *b = alloc_node(2UL, 0);    

    atomic_store_explicit(&b->next[0], pack(g_tail, false), memory_order_relaxed);
    atomic_store_explicit(&a->next[0], pack(b, false), memory_order_relaxed);
    atomic_store_explicit(&a->next[1], pack(g_tail, false), memory_order_relaxed);
    for (int l = 0; l < LEVELS; ++l)
        atomic_store_explicit(&g_head->next[l], pack(a, false), memory_order_relaxed);
}

int main(void) {
    setup();

    pthread_t t1, t2;
#ifdef CONFIG_THREE
    
    pthread_t t3;
    pthread_create(&t1, NULL, t_pop,     NULL);
    pthread_create(&t2, NULL, t_remove2, NULL);
    pthread_create(&t3, NULL, t_add,     NULL);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    check_level0_sorted();
    if (atomic_load_explicit(&g_added3, memory_order_relaxed))
        assert(reachable_level0(3UL));
    for (int i = 0; i < POOL; ++i)
        assert(atomic_load_explicit(&g_retired[i], memory_order_relaxed) <= 1);
    return 0;
#endif

#if defined(CONFIG_TWO_POP)
    pthread_create(&t1, NULL, t_pop, NULL);
    pthread_create(&t2, NULL, t_pop, NULL);
#elif defined(CONFIG_ADD_RACE)
    pthread_create(&t1, NULL, t_add, NULL);
    pthread_create(&t2, NULL, t_add, NULL);
#elif defined(CONFIG_ADD_POP)
    pthread_create(&t1, NULL, t_add, NULL);
    pthread_create(&t2, NULL, t_pop, NULL);
#elif defined(CONFIG_POP_VS_REMOVE2)
    
    pthread_create(&t1, NULL, t_pop, NULL);
    pthread_create(&t2, NULL, t_remove2, NULL);
#else
    
    pthread_create(&t1, NULL, t_pop, NULL);
    pthread_create(&t2, NULL, t_remove, NULL);
#endif

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    check_level0_sorted();
#if !defined(CONFIG_ADD_RACE) && !defined(CONFIG_ADD_POP)
    check_no_retired_reachable();
#endif

#ifdef CONFIG_ADD_RACE
    
    assert(atomic_load_explicit(&g_addWins, memory_order_relaxed) == 1);
#endif

#if defined(CONFIG_ADD_RACE) || defined(CONFIG_ADD_POP)
    
    if (atomic_load_explicit(&g_added3, memory_order_relaxed))
        assert(reachable_level0(3UL));
#endif

    for (int i = 0; i < POOL; ++i)
        assert(atomic_load_explicit(&g_retired[i], memory_order_relaxed) <= 1);

    return 0;
}
