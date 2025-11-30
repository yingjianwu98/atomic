#ifndef RDMA_H
#define RDMA_H

#include <infiniband/verbs.h>
#include <stdint.h>

#include "config.h"

/* Remote memory attributes.
 * Exchanged over TCP during the RDMA handshake */
struct remote_attr {
  uint64_t addr;
  uint32_t rkey;
  uint16_t lid;
  uint32_t qpn;
  uint32_t psn;
  uint8_t gid[16];
} __attribute__((packed));

/* Prepare phase result */
struct prep_res {
  uint64_t ballot;
  uint8_t success;
};

/* LL/SC slot entry
 * Since RDMA CAS is 64-bit only, we use two fields:
 * - ballot: 64-bit field for atomic CAS [timestamp:48 | thread_id:16]
 * - value: 64-bit payload, written after ballot CAS succeeds
 */
struct llsc_slot {
  uint64_t ballot;  // CAS target: encodes thread_id in lower 16 bits
  uint64_t value;   // Payload, written after winning CAS
} __attribute__((packed));

/* Recovery request (for RDMA-based coordinated recovery) */
struct recovery_req {
  uint16_t thread_id;
  uint32_t slot;
} __attribute__((packed));

/* Recovery response (for RDMA-based coordinated recovery) */
struct recovery_resp {
  uint16_t thread_id;
  uint64_t value;
  uint64_t ballot;
  uint8_t valid;
} __attribute__((packed));

/* Per-node RDMA context */
struct rdma_ctx {
  struct ibv_context *ctx;
  uint16_t lid;
  uint8_t gid[16];
  struct ibv_pd *pd;
  struct ibv_mr *mr[2];
  struct ibv_cq *cq;   // CQ for consensus operations
  struct ibv_cq *fcq;  // CQ for frontier operations
  struct ibv_qp **qp;  // QPs for consensus operations
  struct ibv_qp **fqp; // QPs for frontier FAA
  struct {             // Shared (RDMA accessible)
    uint64_t frontier;
    uint64_t slots[MAX_SLOTS];
  } *shared_mem;
  uint64_t *results;
  struct prep_res *prepares;
  struct remote_attr *ra;
  int max_inline;
  struct config *c;

  /* LL/SC specific fields */
  struct ibv_mr *llsc_mr[2];           // MRs for LL/SC: [0]=slots, [1]=recovery
  struct {                             // LL/SC slots (RDMA accessible)
    uint64_t frontier;                 // Current frontier at this replica
    struct llsc_slot slots[MAX_SLOTS]; // Mi[t] := ⟨ballot, value⟩
  } *llsc_mem;
  struct recovery_req *recovery_reqs;  // MRc[j]: recovery requests (coordinator only)
  struct recovery_resp *recovery_resp; // MSj: recovery response (spinning area)
  struct llsc_slot *llsc_results;      // Buffer for LL/SC slot reads
  uint64_t *frontier_results;          // Buffer for frontier reads
};

/* Initialize RDMA context */
int rdma_init(struct rdma_ctx *r, struct config *c);

/* Destroy RDMA context */
void rdma_destroy(struct rdma_ctx *r);

/* Get next slot from frontier node */
uint64_t rdma_get_next_slot(struct rdma_ctx *r);

/* Fast path operations */
int rdma_bcas(struct rdma_ctx *r, uint32_t slot, uint64_t swp);
#define rdma_btas(r, slot) rdma_bcas(r, slot, 1)

/* Slow path */
int rdma_slow_path(struct rdma_ctx *r, uint32_t slot, uint64_t ballot,
                   uint64_t proposed_value);

/* LL/SC operations */
int rdma_load_link(struct rdma_ctx *r, uint32_t *out_index, uint64_t *out_value);
int rdma_store_conditional(struct rdma_ctx *r, uint32_t index, uint64_t value);

/* LL/SC slow path (coordinated recovery) */
int rdma_llsc_slow_path(struct rdma_ctx *r, uint32_t slot, uint64_t value,
                        uint16_t thread_id, uint64_t ballot);

/* Coordinator recovery processing (call periodically from coordinator) */
void rdma_llsc_process_recovery(struct rdma_ctx *r);

/* Timestamp function */
static inline uint64_t ts_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000ULL;
}

/* Generate ballot number: (timestamp << 16) | node_id */
static inline uint64_t gen_ballot(uint16_t node_id) {
  uint64_t ts = ts_us() & 0xFFFFFFFFFFFFULL;
  if (ts == 0)
    ts = 1;
  return (ts << 16) | node_id;
}

#endif /* RDMA_H */
