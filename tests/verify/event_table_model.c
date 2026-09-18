// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define NFIBERS 2     
#define NWORDS  1     

typedef struct Node { int id; } Node;

static Node               g_nodes[NFIBERS];
static _Atomic(Node *)    g_slots[NFIBERS];
static _Atomic(uint64_t)  g_occupied[NWORDS];
static _Atomic int        g_claimed[NFIBERS + 1];

#ifdef NO_RELEASE
  
  #define PUBLISH_ORDER memory_order_relaxed
#else
  #define PUBLISH_ORDER memory_order_release
#endif

static void add_waiter(int i) {
    atomic_store_explicit(&g_slots[i], &g_nodes[i], memory_order_release);
    atomic_fetch_or_explicit(&g_occupied[i >> 6], (uint64_t)1 << (i & 63), PUBLISH_ORDER);
}

static void take(int i) {
    
    Node *t = atomic_exchange_explicit(&g_slots[i], NULL, memory_order_acq_rel);

    assert(t != NULL);

    const int prev = atomic_fetch_add_explicit(&g_claimed[t->id], 1, memory_order_relaxed);

    assert(prev == 0);

}

static void signal_all(void) {
    for (int w = 0; w < NWORDS; ++w) {
        uint64_t bits = atomic_exchange_explicit(&g_occupied[w], 0, memory_order_acq_rel);
        while (bits) {
            const int b = __builtin_ctzll(bits);
            bits &= bits - 1;                       
            take(w * 64 + b);
        }
    }
}

static int signal_one(void) {
    for (int w = 0; w < NWORDS; ++w) {
        uint64_t bits = atomic_load_explicit(&g_occupied[w], memory_order_acquire);
        while (bits) {
            const int b = __builtin_ctzll(bits);
            const uint64_t m = (uint64_t)1 << b;
            int won;
#ifdef CLAIM_ALWAYS_WINS
            
            atomic_fetch_and_explicit(&g_occupied[w], ~m, memory_order_acq_rel);
            won = 1;
#elif defined(CLAIM_NOT_RMW)
            
            const uint64_t cur = atomic_load_explicit(&g_occupied[w], memory_order_acquire);
            won = (cur & m) != 0;
            if (won)
                atomic_store_explicit(&g_occupied[w], cur & ~m, memory_order_release);
#else
            const uint64_t old = atomic_fetch_and_explicit(&g_occupied[w], ~m,
                                                           memory_order_acq_rel);
            won = (old & m) != 0;
#endif
            if (won) { take(w * 64 + b); return 1; }
            bits &= ~m;                             
        }
    }
    return 0;
}

static void *pusher0(void *arg) { (void)arg; add_waiter(0); return NULL; }
static void *pusher1(void *arg) { (void)arg; add_waiter(1); return NULL; }
static void *waker_all(void *arg) { (void)arg; signal_all();  return NULL; }
static void *waker_one(void *arg) { (void)arg; signal_one();  return NULL; }

int main(void) {
    for (int i = 0; i < NFIBERS; ++i) {
        g_nodes[i].id = i + 1;
        atomic_init(&g_slots[i], NULL);
        atomic_init(&g_claimed[i + 1], 0);
    }
    for (int w = 0; w < NWORDS; ++w) atomic_init(&g_occupied[w], 0);

    pthread_t p0, p1, wa, wo;
    pthread_create(&p0, NULL, pusher0,   NULL);
    pthread_create(&p1, NULL, pusher1,   NULL);
    pthread_create(&wa, NULL, waker_all, NULL);
    pthread_create(&wo, NULL, waker_one, NULL);
    pthread_join(p0, NULL);
    pthread_join(p1, NULL);
    pthread_join(wa, NULL);
    pthread_join(wo, NULL);

    signal_all();

    for (int i = 1; i <= NFIBERS; ++i)
        assert(atomic_load(&g_claimed[i]) == 1);

    return 0;
}
