// Implementation based on "Linearizable Synchronization over RDMA"
// Algorithm 2: Synra-LL/SC (Section 5)

#include "rdma.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "arch.h"

#define FAST_QUORUM(c) ((c->n * 3 + 3) / 4)
#define CLASSIC_QUORUM(c) (((c)->n / 2) + 1)
#define COORDINATOR_NODE (0)

/* Load-Link: Read frontier from replicas and return max
 * Algorithm 2, Lines 1-4 */
int rdma_load_link(struct rdma_ctx *r, uint32_t *out_index, uint64_t *out_value) {
    struct config *c = r->c;
    uint64_t *frontier_results = r->frontier_results;

    // Read local frontier
    uint64_t local_frontier = *(volatile uint64_t *)&r->llsc_mem->frontier;
    frontier_results[c->host_id] = local_frontier;

    // Issue RDMA reads to all replicas for their frontier values
    int num_posted = 0;
    for (int i = 0; i < c->n; ++i) {
        if (i != c->host_id) {
            struct remote_attr *ra = r->ra + i;
            uint64_t remote_frontier_addr =
                ra->addr + offsetof(typeof(*r->llsc_mem), frontier);

            struct ibv_sge sge = {
                .addr = (uint64_t)(frontier_results + i),
                .length = sizeof(uint64_t),
                .lkey = r->llsc_mr[0]->lkey
            };

            struct ibv_send_wr wr = {
                .wr_id = i,
                .sg_list = &sge,
                .num_sge = 1,
                .opcode = IBV_WR_RDMA_READ,
                .send_flags = IBV_SEND_SIGNALED,
                .wr.rdma = {
                    .remote_addr = remote_frontier_addr,
                    .rkey = ra->rkey
                }
            };

            struct ibv_send_wr *bad_wr;
            if (ibv_post_send(r->qp[i], &wr, &bad_wr)) {
                FAA_LOG("Failed to post RDMA read for frontier[%d]", i);
                return -1;
            }
            num_posted++;
        }
    }

    // Wait for a quorum of responses
    struct ibv_wc wc[c->n];
    int completed = 0;
    int quorum = CLASSIC_QUORUM(c);
    int success_count = 1; // Count local read

    while (success_count < quorum && completed < num_posted) {
        int n = ibv_poll_cq(r->cq, num_posted - completed, wc);
        if (n > 0) {
            for (int i = 0; i < n; ++i) {
                if (wc[i].status == IBV_WC_SUCCESS) {
                    success_count++;
                }
            }
            completed += n;
        }
    }

    if (success_count < quorum) {
        FAA_LOG("Failed to get quorum for Load-Link");
        return -1;
    }

    // Find max frontier (Line 4)
    uint64_t max_index = local_frontier;
    for (int i = 0; i < c->n; ++i) {
        if (frontier_results[i] > max_index) {
            max_index = frontier_results[i];
        }
    }

    *out_index = (uint32_t)max_index;

    // Read the value at max_index from local memory if available
    if (max_index < MAX_SLOTS) {
        struct llsc_slot *slot = &r->llsc_mem->slots[max_index];
        uint64_t ballot = *(volatile uint64_t *)&slot->ballot;
        if (ballot != 0) {
            *out_value = *(volatile uint64_t *)&slot->value;
        } else {
            *out_value = 0; // Empty slot
        }
    } else {
        *out_value = 0;
    }

    return 0;
}

/* Store-Conditional: FastPaxos on the slot
 * Algorithm 2, Lines 5-24
 * NOTE: CAS only on ballot field (64-bit), then write value separately */
int rdma_store_conditional(struct rdma_ctx *r, uint32_t index, uint64_t value) {
    struct config *c = r->c;
    uint16_t thread_id = c->host_id;
    uint64_t ballot = gen_ballot(thread_id);

    // Fast Path: Try to CAS the slot ballot field (Lines 8-12)
    // Local CAS on slot.ballot (64-bit atomic)
    uint64_t expected_ballot = 0;
    uint64_t old_ballot = __sync_val_compare_and_swap(
        &r->llsc_mem->slots[index].ballot, expected_ballot, ballot);
    int local_slot_success = (old_ballot == expected_ballot);

    // If local CAS succeeded, write value
    if (local_slot_success) {
        r->llsc_mem->slots[index].value = value;
    }

    // Local CAS on frontier
    uint64_t expected_frontier = index;
    uint64_t new_frontier = index + 1;
    uint64_t old_frontier = __sync_val_compare_and_swap(
        &r->llsc_mem->frontier, expected_frontier, new_frontier);
    int local_frontier_success = (old_frontier == expected_frontier);

    int successes = (local_slot_success && local_frontier_success) ? 1 : 0;
    int failures = (local_slot_success && local_frontier_success) ? 0 : 1;

    // Issue parallel RDMA CAS to all replicas on ballot field only
    for (int i = 0; i < c->n; ++i) {
        if (i != c->host_id) {
            struct remote_attr *ra = r->ra + i;

            // CAS on Mi[index].ballot (Line 9) - 64-bit atomic operation
            uint64_t remote_ballot_addr =
                ra->addr + offsetof(typeof(*r->llsc_mem), slots) +
                (index * sizeof(struct llsc_slot)) +
                offsetof(struct llsc_slot, ballot);

            struct ibv_sge sge_slot = {
                .addr = (uint64_t)(&r->llsc_results[i].ballot),
                .length = sizeof(uint64_t),
                .lkey = r->llsc_mr[1]->lkey
            };

            struct ibv_send_wr wr_slot = {
                .wr_id = ((uint64_t)index << 32) | ((uint64_t)i << 16) | 0,
                .sg_list = &sge_slot,
                .num_sge = 1,
                .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
                .send_flags = IBV_SEND_SIGNALED,
                .wr.atomic = {
                    .remote_addr = remote_ballot_addr,
                    .rkey = ra->rkey,
                    .compare_add = 0,
                    .swap = ballot
                }
            };

            struct ibv_send_wr *bad_wr;
            ibv_post_send(r->qp[i], &wr_slot, &bad_wr);

            // CAS on frontieri (Line 10)
            uint64_t remote_frontier_addr =
                ra->addr + offsetof(typeof(*r->llsc_mem), frontier);

            struct ibv_sge sge_frontier = {
                .addr = (uint64_t)(r->frontier_results + i),
                .length = sizeof(uint64_t),
                .lkey = r->llsc_mr[0]->lkey
            };

            struct ibv_send_wr wr_frontier = {
                .wr_id = ((uint64_t)index << 32) | ((uint64_t)i << 16) | 1,
                .sg_list = &sge_frontier,
                .num_sge = 1,
                .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
                .send_flags = IBV_SEND_SIGNALED,
                .wr.atomic = {
                    .remote_addr = remote_frontier_addr,
                    .rkey = ra->rkey,
                    .compare_add = index,
                    .swap = new_frontier
                }
            };

            ibv_post_send(r->qp[i], &wr_frontier, &bad_wr);
        }
    }

    // Poll for completions
    struct ibv_wc wc[c->n * 2];
    int left = (c->n - 1) * 2;
    int n = 0;
    int *remote_slot_won = calloc(c->n, sizeof(int));

    while (left > 0) {
        if ((n = ibv_poll_cq(r->cq, left, wc)) > 0) {
            for (int i = 0; i < n; ++i) {
                if (wc[i].status == IBV_WC_SUCCESS) {
                    uint32_t completion_index = (uint32_t)(wc[i].wr_id >> 32);
                    int node_id = (wc[i].wr_id >> 16) & 0xFFFF;
                    int is_frontier = wc[i].wr_id & 1;

                    if (completion_index == index) {
                        if (is_frontier) {
                            if (r->frontier_results[node_id] == expected_frontier) {
                                successes++;
                            } else {
                                failures++;
                            }
                        } else {
                            // Ballot CAS - check if it was empty (returned 0)
                            if (r->llsc_results[node_id].ballot == 0) {
                                successes++;
                                remote_slot_won[node_id] = 1;
                            } else {
                                failures++;
                            }
                        }

                        if (successes >= FAST_QUORUM(c)) {
                            break; // Fast path succeeded
                        }
                        if (failures > c->n - FAST_QUORUM(c)) {
                            break; // Fast path definitely failed
                        }
                    }
                }
                left--;
            }
        }
    }

    // If fast quorum achieved, write values to replicas where we won the ballot CAS
    if (successes >= FAST_QUORUM(c)) {
        // Write value to replicas where ballot CAS succeeded
        for (int i = 0; i < c->n; ++i) {
            if (i != c->host_id && remote_slot_won[i]) {
                struct remote_attr *ra = r->ra + i;
                uint64_t remote_value_addr =
                    ra->addr + offsetof(typeof(*r->llsc_mem), slots) +
                    (index * sizeof(struct llsc_slot)) +
                    offsetof(struct llsc_slot, value);

                struct ibv_sge sge = {
                    .addr = (uint64_t)&value,
                    .length = sizeof(uint64_t),
                    .lkey = r->llsc_mr[0]->lkey
                };

                struct ibv_send_wr wr = {
                    .wr_id = i,
                    .sg_list = &sge,
                    .num_sge = 1,
                    .opcode = IBV_WR_RDMA_WRITE,
                    .send_flags = IBV_SEND_SIGNALED,
                    .wr.rdma = {
                        .remote_addr = remote_value_addr,
                        .rkey = ra->rkey
                    }
                };

                struct ibv_send_wr *bad_wr;
                ibv_post_send(r->qp[i], &wr, &bad_wr);
            }
        }

        free(remote_slot_won);
        return 0; // SC succeeded (Line 13)
    }

    // Check if none of the CAS succeeded (Line 15)
    if (successes == 0) {
        free(remote_slot_won);
        return -1; // SC fails
    }

    // Slow path: Coordinated recovery (Lines 17-24)
    free(remote_slot_won);
    return rdma_llsc_slow_path(r, index, value, thread_id, ballot);
}

/* RDMA-based Coordinated Recovery (Section 5.1)
 * Called when fast path partially succeeds */
int rdma_llsc_slow_path(struct rdma_ctx *r, uint32_t slot, uint64_t value,
                        uint16_t thread_id, uint64_t ballot) {
    struct config *c = r->c;

    (void)value;  // Unused - value already written during fast path attempt
    (void)ballot; // Unused - coordinator assigns new ballot

    // Step 1: Remove content in MSj (local spinning area)
    memset(r->recovery_resp, 0, sizeof(struct recovery_resp));

    // Step 2: Notify coordinator about recovery need
    // rdma-write(MRc[j], ⟨threadID, t⟩)
    struct recovery_req req = {
        .thread_id = thread_id,
        .slot = slot
    };

    struct remote_attr *coord_ra = r->ra + COORDINATOR_NODE;
    uint64_t remote_recovery_addr =
        coord_ra->addr + (c->host_id * sizeof(struct recovery_req));

    struct ibv_sge sge = {
        .addr = (uint64_t)&req,
        .length = sizeof(struct recovery_req),
        .lkey = r->llsc_mr[1]->lkey
    };

    struct ibv_send_wr wr = {
        .wr_id = 0,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE,
        .send_flags = IBV_SEND_SIGNALED,
        .wr.rdma = {
            .remote_addr = remote_recovery_addr,
            .rkey = coord_ra->rkey
        }
    };

    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(r->qp[COORDINATOR_NODE], &wr, &bad_wr)) {
        FAA_LOG("Failed to notify coordinator for recovery");
        return -1;
    }

    // Wait for write completion
    struct ibv_wc wc;
    while (ibv_poll_cq(r->cq, 1, &wc) <= 0);

    if (wc.status != IBV_WC_SUCCESS) {
        FAA_LOG("Recovery notification failed");
        return -1;
    }

    // Step 3: Spin-wait on MSj (local spinning)
    int timeout = 10000000; // 10M iterations ~ 1-10 seconds
    while (timeout-- > 0) {
        volatile struct recovery_resp *resp = (volatile struct recovery_resp *)r->recovery_resp;
        if (resp->valid) {
            // Step 4: Check if Nj is the winner
            int won = (resp->thread_id == thread_id);

            // Clean up MSj for next use
            memset(r->recovery_resp, 0, sizeof(struct recovery_resp));

            return won ? 0 : -1;
        }
        cpu_relax();
    }

    FAA_LOG("Recovery timeout for slot %u", slot);
    return -1;
}

/* Coordinator recovery processing
 * This should be called periodically by the coordinator node
 * to handle recovery requests from other nodes */
void rdma_llsc_process_recovery(struct rdma_ctx *r) {
    struct config *c = r->c;

    // Only coordinator processes recovery
    if (c->host_id != COORDINATOR_NODE) {
        return;
    }

    // Check each node's recovery request
    for (int j = 0; j < c->n; ++j) {
        struct recovery_req *req = &r->recovery_reqs[j];

        // Check if there's a pending recovery request
        if (req->thread_id == 0) {
            continue; // No request from this node
        }

        uint32_t slot = req->slot;

        FAA_LOG("Coordinator processing recovery for node %d, slot %u", j, slot);

        // Step 1: rdma-read(Mi[slot]) from all replicas
        struct llsc_slot *reads = r->llsc_results;
        reads[c->host_id] = r->llsc_mem->slots[slot];

        for (int i = 0; i < c->n; ++i) {
            if (i != c->host_id) {
                struct remote_attr *ra = r->ra + i;
                uint64_t remote_slot_addr =
                    ra->addr + offsetof(typeof(*r->llsc_mem), slots) +
                    (slot * sizeof(struct llsc_slot));

                struct ibv_sge sge = {
                    .addr = (uint64_t)(reads + i),
                    .length = sizeof(struct llsc_slot),
                    .lkey = r->llsc_mr[1]->lkey
                };

                struct ibv_send_wr wr = {
                    .wr_id = i,
                    .sg_list = &sge,
                    .num_sge = 1,
                    .opcode = IBV_WR_RDMA_READ,
                    .send_flags = IBV_SEND_SIGNALED,
                    .wr.rdma = {
                        .remote_addr = remote_slot_addr,
                        .rkey = ra->rkey
                    }
                };

                struct ibv_send_wr *bad_wr;
                ibv_post_send(r->qp[i], &wr, &bad_wr);
            }
        }

        // Step 2: Wait for quorum of reads
        struct ibv_wc wc[c->n];
        int completed = 0;
        int num_posted = c->n - 1;

        while (completed < num_posted) {
            int n = ibv_poll_cq(r->cq, num_posted - completed, wc);
            if (n > 0) {
                completed += n;
            }
        }

        // Step 3: Find majority ballot or pick highest ballot
        struct llsc_slot chosen = {0, 0};
        uint64_t highest_ballot = 0;

        // Find highest ballot (most recent write)
        for (int i = 0; i < c->n; ++i) {
            if (reads[i].ballot > highest_ballot) {
                highest_ballot = reads[i].ballot;
                chosen = reads[i];
            }
        }

        // If no ballot found (all empty), use first non-empty
        if (chosen.ballot == 0) {
            for (int i = 0; i < c->n; ++i) {
                if (reads[i].ballot != 0) {
                    chosen = reads[i];
                    break;
                }
            }
        }

        // Step 4: Write chosen value to all replicas with coordinator's ballot
        uint64_t coord_ballot = gen_ballot(COORDINATOR_NODE);

        // Keep the existing value if found, otherwise use coordinator's thread_id
        struct llsc_slot final_slot = {
            .ballot = coord_ballot,
            .value = chosen.value
        };

        // Local write
        r->llsc_mem->slots[slot] = final_slot;

        // Remote writes
        for (int i = 0; i < c->n; ++i) {
            if (i != c->host_id) {
                struct remote_attr *ra = r->ra + i;
                uint64_t remote_slot_addr =
                    ra->addr + offsetof(typeof(*r->llsc_mem), slots) +
                    (slot * sizeof(struct llsc_slot));

                struct ibv_sge sge = {
                    .addr = (uint64_t)&final_slot,
                    .length = sizeof(struct llsc_slot),
                    .lkey = r->llsc_mr[1]->lkey
                };

                struct ibv_send_wr wr = {
                    .wr_id = i,
                    .sg_list = &sge,
                    .num_sge = 1,
                    .opcode = IBV_WR_RDMA_WRITE,
                    .send_flags = IBV_SEND_SIGNALED,
                    .wr.rdma = {
                        .remote_addr = remote_slot_addr,
                        .rkey = ra->rkey
                    }
                };

                struct ibv_send_wr *bad_wr;
                ibv_post_send(r->qp[i], &wr, &bad_wr);
            }
        }

        // Step 5: Notify the requester via MSj
        // Extract thread_id from the chosen ballot (lower 16 bits)
        uint16_t winner_thread_id = (chosen.ballot != 0) ? (chosen.ballot & 0xFFFF) : 0;

        struct recovery_resp resp = {
            .thread_id = winner_thread_id,
            .value = chosen.value,
            .ballot = final_slot.ballot,
            .valid = 1
        };

        struct remote_attr *req_ra = r->ra + j;
        uint64_t remote_resp_addr = req_ra->addr; // MSj address

        struct ibv_sge sge_resp = {
            .addr = (uint64_t)&resp,
            .length = sizeof(struct recovery_resp),
            .lkey = r->llsc_mr[1]->lkey
        };

        struct ibv_send_wr wr_resp = {
            .wr_id = j,
            .sg_list = &sge_resp,
            .num_sge = 1,
            .opcode = IBV_WR_RDMA_WRITE,
            .send_flags = IBV_SEND_SIGNALED,
            .wr.rdma = {
                .remote_addr = remote_resp_addr,
                .rkey = req_ra->rkey
            }
        };

        struct ibv_send_wr *bad_wr;
        ibv_post_send(r->qp[j], &wr_resp, &bad_wr);

        // Clear the recovery request
        memset(req, 0, sizeof(struct recovery_req));
    }
}
