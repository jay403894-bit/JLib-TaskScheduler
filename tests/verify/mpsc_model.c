// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define NPROD      2
#define PER_PROD   2
#define NITEMS     (NPROD * PER_PROD)

#ifdef NO_APPEND_RELEASE
  #define LINK_ORDER memory_order_relaxed
#else
  #define LINK_ORDER memory_order_release   
#endif

#ifdef NO_POP_ACQUIRE
  #define NEXT_ORDER memory_order_relaxed
#else
  #define NEXT_ORDER memory_order_acquire
#endif

#ifdef RELAXED_XCHG
  #define XCHG_ORDER memory_order_relaxed
#else
  #define XCHG_ORDER memory_order_acq_rel
#endif

struct Node {
    _Atomic(struct Node *) next;
    int                    value;      
};

static struct Node  g_stub;
static struct Node  g_nodes[NITEMS];

static _Atomic(struct Node *) g_head;
static struct Node           *g_tail;   

static _Atomic int g_claims[NITEMS + 1];

static void append(struct Node *n) {
    atomic_store_explicit(&n->next, NULL, memory_order_relaxed);

    struct Node *prev = atomic_exchange_explicit(&g_head, n, XCHG_ORDER);

    atomic_store_explicit(&prev->next, n, LINK_ORDER);
}

static struct Node *pop(void) {
    struct Node *tail = g_tail;
    struct Node *next = atomic_load_explicit(&tail->next, NEXT_ORDER);

    if (tail == &g_stub) {
        if (!next) return NULL;                  
        g_tail = next;
        tail   = next;
        next   = atomic_load_explicit(&next->next, NEXT_ORDER);
    }

    if (next) {
        g_tail = next;
        return tail;
    }

    if (tail != atomic_load_explicit(&g_head, memory_order_acquire))
        return NULL;

    append(&g_stub);

    next = atomic_load_explicit(&tail->next, NEXT_ORDER);
    if (next) {
        g_tail = next;
        return tail;
    }
    return NULL;
}

static void *producer(void *arg) {
    const long id = (long)arg;                   
    for (int i = 0; i < PER_PROD; ++i) {
        struct Node *n = &g_nodes[id * PER_PROD + i];
        
        n->value = (int)(id * PER_PROD + i) + 1;
        append(n);
    }
    return NULL;
}

static void *consumer(void *arg) {
    (void)arg;
    
    for (int i = 0; i < NITEMS; ++i) {
        struct Node *n = pop();
        if (!n) continue;

        assert(n->value >= 1 && n->value <= NITEMS);
        atomic_fetch_add_explicit(&g_claims[n->value], 1, memory_order_relaxed);
    }
    return NULL;
}

int main(void) {
    atomic_init(&g_stub.next, NULL);
    g_stub.value = -1;
    for (int i = 0; i < NITEMS; ++i) {
        atomic_init(&g_nodes[i].next, NULL);
        g_nodes[i].value = 0;                    
    }
    for (int i = 0; i <= NITEMS; ++i) atomic_init(&g_claims[i], 0);

    atomic_init(&g_head, &g_stub);
    g_tail = &g_stub;

    pthread_t p0, p1, c;
    pthread_create(&p0, NULL, producer, (void *)0L);
    pthread_create(&p1, NULL, producer, (void *)1L);
    pthread_create(&c,  NULL, consumer, NULL);
    pthread_join(p0, NULL);
    pthread_join(p1, NULL);
    pthread_join(c,  NULL);

    for (;;) {
        struct Node *n = pop();
        if (!n) break;
        assert(n->value >= 1 && n->value <= NITEMS);
        atomic_fetch_add_explicit(&g_claims[n->value], 1, memory_order_relaxed);
    }

    for (int i = 1; i <= NITEMS; ++i)
        assert(atomic_load(&g_claims[i]) <= 1);

    int total = 0;
    for (int i = 1; i <= NITEMS; ++i) total += atomic_load(&g_claims[i]);
    assert(total == NITEMS);

    return 0;
}
