#include "node.h"

#include <immintrin.h>
#include <unistd.h>

#define MAX_RETRIES (5)

int __try_fast_path(struct node_ctx *ctx, uint32_t target_slot) {
#ifdef TRACK_SLOTS
    uint64_t ballot = gen_ballot(ctx->id);
    uint64_t start = ts_us();
    int ret = rdma_bcas(&ctx->r, target_slot, ballot);
    uint64_t dif = ts_us() - start;
    __sync_bool_compare_and_swap(&ctx->s[target_slot].lat, 0, dif);
    __sync_bool_compare_and_swap(&ctx->s[target_slot].path, 0, 1);
    if (!ret) {
        __sync_bool_compare_and_swap(&ctx->s[target_slot].won, 0, 1);
        fprintf(stdout, "%hu,%u,%lu,FAST\n", ctx->id, target_slot, dif);
        fflush(stdout);
    }
    return ret;
#else
    uint64_t ballot = gen_ballot(ctx->id);
    return rdma_bcas(&ctx->r, target_slot, ballot);
#endif
}

int __try_slow_path(struct node_ctx *ctx, uint32_t target_slot) {
#ifdef TRACK_SLOTS
    uint64_t ballot = gen_ballot(ctx->id);
    uint64_t start = ts_us();
    int ret = rdma_slow_path(&ctx->r, target_slot, ballot, ballot);
    uint64_t dif = ts_us() - start;
    __sync_bool_compare_and_swap(&ctx->s[target_slot].lat, 0, dif);
    __sync_bool_compare_and_swap(&ctx->s[target_slot].path, 0, 2);
    if (!ret) {
        __sync_bool_compare_and_swap(&ctx->s[target_slot].won, 0, 1);
        fprintf(stdout, "%hu,%u,%lu,SLOW\n", ctx->id, target_slot, dif);
        fflush(stdout);
    }
    return ret;
#else
    uint64_t ballot = gen_ballot(ctx->id);
    return rdma_slow_path(&ctx->r, target_slot, ballot, ballot);
#endif
}

int64_t fetch_and_add(struct node_ctx *ctx) {
    struct rdma_ctx *r = &ctx->r;
#ifdef DEBUG
    static __thread uint64_t call_count = 0;
    uint64_t this_call = ++call_count;
#endif
    while (1) {
        /* Get assigned slot */
        uint64_t my_slot = rdma_get_next_slot(r);
        if (my_slot == (uint64_t)-1) {
            usleep(100);
            continue;
        } else if (my_slot >= MAX_SLOTS)
            return -ENOMEM;

        /* 1. Try fast path */
        int fast_res = __try_fast_path(ctx, my_slot);
        if (fast_res == 0)
            return my_slot;  // this thread won
        else if (fast_res == 1)
            continue;  // slot commited by another thread

        /* 2. Fast path failed. Try slow path */
        int slow_res = __try_slow_path(ctx, my_slot);
        if (slow_res == 0)
            return my_slot;  // this thread won
        else if (slow_res == 1)
            continue;  // slot commited by another thread

        /* 3. Both paths failed. Retry the same slot. */
        for (int retry_count = 0; retry_count < MAX_RETRIES; ++retry_count) {
            uint64_t val = *(volatile uint64_t *)&r->shared_mem->slots[my_slot];
            if (val != 0 || __try_slow_path(ctx, my_slot) >= 0)
                break;  // slot filled
            usleep(1);
        }
    }
}

int64_t test_and_set(struct node_ctx *ctx, uint32_t slot) {
    struct rdma_ctx *r = &ctx->r;
    for (int retry_count = 0; retry_count < MAX_RETRIES; ++retry_count) {
        // 1. Try fast path
        int fast_res = rdma_btas(r, slot);
        if (fast_res == 0)
            return 0;  // this thread won
        else if (fast_res == 1)
            return 1;  // another thread won

        // 2. Fast path failed. Try slow path
        uint64_t ballot = gen_ballot(ctx->id);
        int slow_res = rdma_slow_path(r, slot, ballot, 1);
        if (slow_res == 0)
            return 0;  // this thread won
        else if (slow_res >= 0)
            return 1;  // another thread won

        // 3. Both paths failed. Check and retry
        uint64_t val = *(volatile uint64_t *)&r->shared_mem->slots[slot];
        if (val != 0) return 1;
        if (retry_count < 3)
            _mm_pause();
        else
            usleep(1);
    }
    return -1;
}

int node_init(struct node_ctx *ctx, struct config *c) {
    ctx->id = c->host_id;
    ctx->seed = (uint32_t)time(0) ^ (uint32_t)ctx->id;
#ifdef TRACK_SLOTS
    ctx->s = calloc(MAX_SLOTS, sizeof(*ctx->s));
    if (!ctx->s) {
        perror("calloc");
        return -ENOMEM;
    }
#endif
    return rdma_init(&ctx->r, c);
}

void node_destroy(struct node_ctx *ctx) {
#ifdef TRACK_SLOTS
    free(ctx->s);
#endif
    rdma_destroy(&ctx->r);
}
