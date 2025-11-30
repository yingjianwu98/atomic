#include "node.h"

#include <unistd.h>
#include "arch.h"

#define MAX_RETRIES (5)

static inline int __try_fast_path(struct node_ctx *ctx, uint32_t target_slot) {
    uint64_t ballot = gen_ballot(ctx->id);
    return rdma_bcas(&ctx->r, target_slot, ballot);
}

static inline int __try_slow_path(struct node_ctx *ctx, uint32_t target_slot) {
    uint64_t ballot = gen_ballot(ctx->id);
    return rdma_slow_path(&ctx->r, target_slot, ballot, ballot);
}

int64_t fetch_and_add(struct node_ctx *ctx) {
    struct rdma_ctx *r = &ctx->r;
    uint64_t slot = 0;
    int64_t ret = 0;
    pthread_mutex_lock(&ctx->lock);
    while (1) {
        /* Get assigned slot */
        slot = rdma_get_next_slot(r);
        if (slot == (uint64_t)-1) {  // failed. try again
            usleep(100);
            continue;
        } else if (slot >= MAX_SLOTS) {
            slot = -ENOMEM;
            goto done;
        }

        /* 1. Try fast path */
        ret = __try_fast_path(ctx, slot);
        if (!ret)
            goto done;  // this thread won
        else if (ret == 1)
            continue;  // slot commited by another thread

        /* 2. Fast path failed. Try slow path */
        ret = __try_slow_path(ctx, slot);
        if (!ret)
            goto done;  // this thread won
        else if (ret == 1)
            continue;  // slot commited by another thread

        /* 3. Both paths failed. Retry slow path on the same slot. */
        for (int retry_count = 0; retry_count < MAX_RETRIES; ++retry_count) {
            uint64_t val = *(volatile uint64_t *)&r->shared_mem->slots[ret];
            if (val != 0 || __try_slow_path(ctx, slot) >= 0)
                break;  // slot filled
            usleep(1);
        }
    }
done:
    pthread_mutex_unlock(&ctx->lock);
    return slot;
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
            cpu_relax();
        else
            usleep(1);
    }
    return -1;
}

/* LL/SC: Load-Link operation */
int load_link(struct node_ctx *ctx, uint64_t *out_value) {
    struct rdma_ctx *r = &ctx->r;
    int ret;

    pthread_mutex_lock(&ctx->lock);
    ret = rdma_load_link(r, &ctx->my_index, &ctx->my_value);
    if (ret == 0 && out_value) {
        *out_value = ctx->my_value;
    }
    pthread_mutex_unlock(&ctx->lock);

    return ret;
}

/* LL/SC: Store-Conditional operation */
int store_conditional(struct node_ctx *ctx, uint64_t value) {
    struct rdma_ctx *r = &ctx->r;
    int ret;

    pthread_mutex_lock(&ctx->lock);
    ret = rdma_store_conditional(r, ctx->my_index, value);
    pthread_mutex_unlock(&ctx->lock);

    return ret;
}

int node_init(struct node_ctx *ctx, struct config *c) {
    ctx->id = c->host_id;
    ctx->seed = (uint32_t)time(0) ^ (uint32_t)ctx->id;
    ctx->my_index = 0;
    ctx->my_value = 0;
    pthread_mutex_init(&ctx->lock, 0);
    return rdma_init(&ctx->r, c);
}

void node_destroy(struct node_ctx *ctx) {
    pthread_mutex_destroy(&ctx->lock);
    rdma_destroy(&ctx->r);
}
