#include <immintrin.h>

#include "rdma.h"

#define FAST_QUORUM(c) ((c->n * 3 + 3) / 4)
#define CLASSIC_QUORUM(c) (((c)->n / 2) + 1)

/* Get per-thread results buffer */
static inline uint64_t *get_thread_results(struct rdma_ctx *r) {
    static __thread int thread_id = -1;
    static int next_thread_id = 0;
    static pthread_mutex_t id_mutex = PTHREAD_MUTEX_INITIALIZER;
    if (thread_id == -1) {
        pthread_mutex_lock(&id_mutex);
        thread_id = next_thread_id++;
        pthread_mutex_unlock(&id_mutex);
        if (thread_id >= MAX_CONCURRENT_REQ) {
            FAA_LOG("ERROR: Too many threads. Max %d supported",
                    MAX_CONCURRENT_REQ);
            return NULL;
        }
    }
    return r->results + thread_id * (r->c->n + 1);
}

/* Broadcast atomic RDMA CAS */
int rdma_bcas(struct rdma_ctx *r, uint32_t slot, uint64_t swp) {
    struct config *c = r->c;

    uint64_t local_result =
        __sync_val_compare_and_swap(&r->shared_mem->slots[slot], 0, swp);
    int local_won = (local_result == 0);
    int successes = local_won;

    uint64_t *thread_results = get_thread_results(r);
    if (!thread_results) return -1;

    for (int i = 0; i < c->n; ++i) {
        if (i != c->host_id) {
            struct ibv_send_wr *bad_wr = NULL, wr = {};
            struct remote_attr *a = r->ra + i;
            uint64_t remote_slot_addr =
                a->addr + offsetof(typeof(*r->shared_mem), slots) +
                (slot * sizeof(uint64_t));

            struct ibv_sge list = {};
            list.addr = (uint64_t)(thread_results + i);
            list.length = sizeof(uint64_t);
            list.lkey = r->mr[1]->lkey;

            wr.wr_id = i;
            wr.sg_list = &list;
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.num_sge = 1;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.atomic.remote_addr = remote_slot_addr;
            wr.wr.atomic.rkey = a->rkey;
            wr.wr.atomic.compare_add = 0;
            wr.wr.atomic.swap = swp;

            ibv_post_send(r->qp[i], &wr, &bad_wr);
        }
    }

    struct ibv_wc wc[c->n];
    int left = c->n - 1;
    while (left > 0) {
        int n = ibv_poll_cq(r->cq, left, wc);
        if (n > 0) {
            for (int i = 0; i < n; ++i) {
                int remote_idx = wc[i].wr_id;
                if (wc[i].status == IBV_WC_SUCCESS)
                    successes += (thread_results[remote_idx] == 0);
            }
            left -= n;
        }
    }

    return (successes < FAST_QUORUM(c)) ? -1 : !local_won;
}

/* Slow path: paxos recovery */
int rdma_slow_path(struct rdma_ctx *r, uint32_t slot, uint64_t ballot,
                   uint64_t proposed_value) {
    struct config *c = r->c;
    struct prep_res *results = r->prepares;
    memset(results, 0, sizeof(struct prep_res) * c->n);

    // Get result buffer for this thread
    uint64_t *thread_results = get_thread_results(r);
    if (!thread_results) return -1;

    // Phase 2a (Prepare): Read current values
    results[c->host_id].ballot =
        *(volatile uint64_t *)&r->shared_mem->slots[slot];
    results[c->host_id].success = 1;
    for (int i = 0; i < c->n; ++i)
        if (i != c->host_id) {
            struct remote_attr *ra = r->ra + i;
            uint64_t remote_slot_addr =
                ra->addr + offsetof(typeof(*r->shared_mem), slots) +
                (slot * sizeof(uint64_t));
            struct ibv_sge sge = {.addr = (uint64_t)(thread_results + i),
                                  .length = sizeof(uint64_t),
                                  .lkey = r->mr[1]->lkey};
            struct ibv_send_wr wr = {
                .wr_id = i,
                .sg_list = &sge,
                .num_sge = 1,
                .opcode = IBV_WR_RDMA_READ,
                .send_flags = IBV_SEND_SIGNALED,
                .wr.rdma = {.remote_addr = remote_slot_addr, .rkey = ra->rkey}};
            struct ibv_send_wr *bad_wr;
            ibv_post_send(r->qp[i], &wr, &bad_wr);
        }

    struct ibv_wc wc[c->n];
    int completed = 0;
    int num_posted = c->n - 1;
    while (completed < num_posted) {
        int n = ibv_poll_cq(r->cq, num_posted - completed, wc);
        if (n > 0) {
            for (int i = 0; i < n; ++i) {
                int remote_idx = wc[i].wr_id;
                if (wc[i].status == IBV_WC_SUCCESS) {
                    results[remote_idx].ballot = thread_results[remote_idx];
                    results[remote_idx].success = 1;
                } else
                    results[remote_idx].success = 0;
            }
            completed += n;
        }
    }

    // Check if fast quorum already exists
    uint64_t ballot_counts[c->n];
    memset(ballot_counts, 0, sizeof(ballot_counts));
    int unique_ballots = 0;
    for (int i = 0; i < c->n; ++i)
        if (results[i].success && results[i].ballot > 0) {
            int found = 0;
            for (int j = 0; j < unique_ballots; ++j)
                if (ballot_counts[j] == results[i].ballot) {
                    found = 1;
                    break;
                }
            if (!found && unique_ballots < c->n)
                ballot_counts[unique_ballots++] = results[i].ballot;
        }
    for (int i = 0; i < unique_ballots; ++i) {
        int count = 0;
        uint16_t owner = ballot_counts[i] & 0xFFFF;
        for (int j = 0; j < c->n; ++j)
            if (results[j].success && results[j].ballot == ballot_counts[i])
                ++count;
        if (count >= FAST_QUORUM(c)) return (owner != c->host_id);
    }

    // Calculate promises
    int promises = 0;
    uint64_t highest_ballot = 0;
    uint64_t highest_value = 0;
    for (int i = 0; i < c->n; ++i)
        if (results[i].success && ballot >= results[i].ballot) {
            ++promises;
            if (results[i].ballot > highest_ballot) {
                highest_ballot = results[i].ballot;
                highest_value = results[i].ballot;
            }
        }
    if (promises < CLASSIC_QUORUM(c)) return -1;

    // Phase 2b (Accept)
    uint64_t proposal = (highest_ballot > 0) ? highest_value : proposed_value;
    uint64_t cmp = results[c->host_id].ballot;
    uint64_t res =
        __sync_val_compare_and_swap(&r->shared_mem->slots[slot], cmp, proposal);
    int accepts = (res == cmp);
    for (int i = 0; i < c->n; ++i)
        if (i != c->host_id) {
            struct remote_attr *ra = r->ra + i;
            uint64_t remote_slot_addr =
                ra->addr + offsetof(typeof(*r->shared_mem), slots) +
                (slot * sizeof(uint64_t));
            uint64_t expected = results[i].ballot;
            struct ibv_sge sge = {.addr = (uint64_t)(thread_results + i),
                                  .length = sizeof(uint64_t),
                                  .lkey = r->mr[1]->lkey};
            struct ibv_send_wr wr = {
                .wr_id = i,
                .sg_list = &sge,
                .num_sge = 1,
                .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
                .send_flags = IBV_SEND_SIGNALED,
                .wr.atomic = {.remote_addr = remote_slot_addr,
                              .rkey = ra->rkey,
                              .compare_add = expected,
                              .swap = proposal}};
            struct ibv_send_wr *bad_wr;
            ibv_post_send(r->qp[i], &wr, &bad_wr);
        }

    completed = 0;
    num_posted = c->n - 1;
    while (completed < num_posted) {
        int n = ibv_poll_cq(r->cq, num_posted - completed, wc);
        if (n > 0) {
            for (int i = 0; i < n; ++i)
                if (wc[i].status == IBV_WC_SUCCESS) {
                    int remote_idx = wc[i].wr_id;
                    uint64_t returned = thread_results[remote_idx];
                    uint64_t expected = results[remote_idx].ballot;
                    if (returned == expected) ++accepts;
                }
            completed += n;
        }
    }

    uint16_t winner = proposal & 0xffff;
    return (accepts >= CLASSIC_QUORUM(c)) ? c->host_id != winner : -1;
}

/* Read next slot from frontier node */
uint64_t rdma_get_next_slot(struct rdma_ctx *r) {
    static pthread_mutex_t frontier_mutex = PTHREAD_MUTEX_INITIALIZER;

    pthread_mutex_lock(&frontier_mutex);

    uint64_t *thread_results = get_thread_results(r);
    if (!thread_results) {
        pthread_mutex_unlock(&frontier_mutex);
        return -1;
    }

    uint64_t *result_ptr = thread_results + r->c->n;
    struct remote_attr *ra = r->ra + FRONTIER_NODE;
    uint64_t remote_frontier_addr =
        ra->addr + offsetof(typeof(*r->shared_mem), frontier);

    struct ibv_sge sge = {.addr = (uint64_t)result_ptr,
                          .length = sizeof(uint64_t),
                          .lkey = r->mr[1]->lkey};

    struct ibv_send_wr wr = {.wr_id = 0,
                             .sg_list = &sge,
                             .num_sge = 1,
                             .opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
                             .send_flags = IBV_SEND_SIGNALED,
                             .wr.atomic = {.remote_addr = remote_frontier_addr,
                                           .rkey = ra->rkey,
                                           .compare_add = 1}};

    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(r->fqp[FRONTIER_NODE], &wr, &bad_wr)) {
        pthread_mutex_unlock(&frontier_mutex);
        FAA_LOG("Failed to post frontier FAA");
        return -1;
    }

    struct ibv_wc wc;
    while (ibv_poll_cq(r->fcq, 1, &wc) < 1) _mm_pause();
    uint64_t result = *result_ptr;

    pthread_mutex_unlock(&frontier_mutex);

    if (wc.status != IBV_WC_SUCCESS) {
        FAA_LOG("Frontier FAA failed: %s", ibv_wc_status_str(wc.status));
        return -1;
    }

    return result;
}
