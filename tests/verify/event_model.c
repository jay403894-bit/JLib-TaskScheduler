// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define NNODES 2      

typedef struct Node {
    int          id;
    struct Node *nextWaiter;
} Node;

static Node              g_nodes[NNODES];
static _Atomic(Node *)   g_head;
static _Atomic int       g_drained[NNODES + 1];

#ifdef NO_RELEASE
  
  #define PUBLISH_ORDER memory_order_relaxed
#else
  #define PUBLISH_ORDER memory_order_release
#endif

static void add_waiter(Node *t) {
    Node *h = atomic_load_explicit(&g_head, memory_order_relaxed);
    do {
        t->nextWaiter = h;                       
    } while (!atomic_compare_exchange_weak_explicit(
                 &g_head, &h, t, PUBLISH_ORDER, memory_order_relaxed));
}

static void signal_all(void) {
    Node *t = atomic_exchange_explicit(&g_head, NULL, memory_order_acq_rel);
    while (t) {
        Node *next = t->nextWaiter;              
        t->nextWaiter = NULL;
        atomic_fetch_add_explicit(&g_drained[t->id], 1, memory_order_relaxed);
        t = next;
    }
}

static void *pusher0(void *arg) { (void)arg; add_waiter(&g_nodes[0]); return NULL; }
static void *pusher1(void *arg) { (void)arg; add_waiter(&g_nodes[1]); return NULL; }
static void *drainer(void *arg) { (void)arg; signal_all();            return NULL; }

int main(void) {
    atomic_init(&g_head, NULL);
    for (int i = 0; i < NNODES; ++i) {
        g_nodes[i].id = i + 1;
        g_nodes[i].nextWaiter = NULL;
        atomic_init(&g_drained[i + 1], 0);
    }

    pthread_t p0, p1, d;
    pthread_create(&p0, NULL, pusher0, NULL);
    pthread_create(&p1, NULL, pusher1, NULL);
    pthread_create(&d,  NULL, drainer, NULL);
    pthread_join(p0, NULL);
    pthread_join(p1, NULL);
    pthread_join(d,  NULL);

    signal_all();

    for (int i = 1; i <= NNODES; ++i)
        assert(atomic_load(&g_drained[i]) == 1);

    return 0;
}
