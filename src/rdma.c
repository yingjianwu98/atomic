#include "rdma.h"

#include <stdio.h>
#include <stdlib.h>

/* Max Work requests */
#define MAX_WR (1 << 10)

/* Max scatter-gather entries */
#define MAX_SGE (1 << 1)

extern int rdma_handshake(struct rdma_ctx *r, struct config *c);

int __add_qp(struct rdma_ctx *r, int id, int port_num, int frontier) {
    struct ibv_qp **qp = (frontier ? r->fqp : r->qp);
    struct ibv_qp_init_attr init_attr = {.qp_type = IBV_QPT_RC,
                                         .send_cq = frontier ? r->fcq : r->cq,
                                         .recv_cq = frontier ? r->fcq : r->cq,
                                         .cap = {.max_send_wr = MAX_WR,
                                                 .max_recv_wr = MAX_WR,
                                                 .max_send_sge = MAX_SGE,
                                                 .max_recv_sge = MAX_SGE}};
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_INIT,
        .pkey_index = 0,
        .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC,
        .port_num = port_num,
    };
    if (!(qp[id] = ibv_create_qp(r->pd, &init_attr))) {
        FAA_LOG("ibv_create_qp failed");
        return -errno;
    }
    if (ibv_modify_qp(qp[id], &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
        ibv_destroy_qp(qp[id]);
        FAA_LOG("ibv_modify_qp failed");
        return -errno;
    }
    if (!ibv_query_qp(qp[id], &attr, IBV_QP_CAP, &init_attr))
        r->max_inline = init_attr.cap.max_inline_data;
    return 0;
}

int rdma_init(struct rdma_ctx *r, struct config *c) {
    union ibv_gid gid;
    struct ibv_port_attr pa;
    struct ibv_device **dev_list;
    struct node_config *host_cfg = c->c + c->host_id;
    uint16_t port_num = host_cfg->ib_port;
    uint16_t gid_index = host_cfg->gid_index;

    if (!(dev_list = ibv_get_device_list(NULL))) {
        FAA_LOG("ibv_get_device_list failed");
        goto exit;
    }

    // open rdma device
    if (!(r->ctx = ibv_open_device(dev_list[c->rdma_device]))) {
        FAA_LOG("ibv_open_device failed");
        ibv_free_device_list(dev_list);
        goto exit;
    }
    ibv_free_device_list(dev_list);

    if (ibv_query_gid(r->ctx, port_num, gid_index, &gid)) {
        FAA_LOG("ibv_query_gid failed");
        goto exit;
    }

#pragma GCC unroll 16
    for (int i = 0; i < 16; ++i) r->gid[i] = gid.raw[i];

    if (ibv_query_port(r->ctx, port_num, &pa)) {
        FAA_LOG("ibv_query_port failed");
        goto exit;
    }
    r->lid = pa.lid;

    if (!(r->pd = ibv_alloc_pd(r->ctx))) {
        FAA_LOG("ibv_alloc_pd failed");
        goto exit;
    }

    size_t nb = sizeof(*r->shared_mem);
    if (!(r->shared_mem = calloc(1, nb))) {
        perror("calloc:");
        goto errpd;
    }
    r->shared_mem->frontier = 0;

    r->mr[0] =
        ibv_reg_mr(r->pd, r->shared_mem, nb,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                       IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    if (!r->mr[0]) {
        FAA_LOG("Failed to register memory region");
        goto errslots;
    }

    // allocate per-thread results buffer
    nb = sizeof(uint64_t) * (c->n + 1) * MAX_CONCURRENT_REQ;
    if (!(r->results = calloc(1, nb))) {
        perror("calloc:");
        goto errmr;
    }
    r->mr[1] = ibv_reg_mr(r->pd, r->results, nb, IBV_ACCESS_LOCAL_WRITE);
    if (!r->mr[1]) {
        FAA_LOG("Failed to register memory region");
        goto errres;
    }

    // allocate completion queue for consensus
    if (!(r->cq = ibv_create_cq(r->ctx, 1024, NULL, NULL, 0))) {
        FAA_LOG("ibv_create_cq failed");
        goto errmr2;
    }

    // allocate completion queue for frontier operations
    if (!(r->fcq = ibv_create_cq(r->ctx, 16, NULL, NULL, 0))) {
        FAA_LOG("ibv_create_cq (frontier) failed");
        goto errcq;
    }

    // allocate queue-pairs
    nb = sizeof(struct ibv_qp *) * c->n;
    if (!(r->qp = calloc(1, nb))) {
        perror("calloc:");
        goto errfrontiercq;
    }
    if (!(r->fqp = calloc(1, nb))) {
        perror("calloc:");
        goto errfrontiercq;
    }

    // init queue pairs
    int i = 0;
    for (; i < c->n; ++i) {
        if (i != c->host_id && __add_qp(r, i, c->c[i].ib_port, 0)) {
            FAA_LOG("Failed to create QP %d", i);
            goto errra;
        }
        if (__add_qp(r, i, c->c[i].ib_port, 1)) {
            FAA_LOG("Failed to create QP %d", i);
            goto errra;
        }
    }

    nb = sizeof(struct remote_attr) * c->n;
    if (!(r->ra = calloc(1, nb))) {
        perror("calloc");
        goto errra;
    }

    nb = sizeof(struct prep_res) * c->n;
    if (!(r->prepares = calloc(1, nb))) {
        perror("calloc");
        goto err;
    }

    r->c = c;
    return rdma_handshake(r, c);
err:
    free(r->ra);
errra:
    for (int j = 0; j <= i; ++j) {
        if (r->qp[j]) ibv_destroy_qp(r->qp[j]);
        if (r->fqp[j]) ibv_destroy_qp(r->fqp[j]);
    }
    free(r->qp);
    free(r->fqp);
    r->qp = r->fqp = NULL;
errfrontiercq:
    ibv_destroy_cq(r->fcq);
    r->fcq = NULL;
errcq:
    ibv_destroy_cq(r->cq);
    r->cq = NULL;
errmr2:
    ibv_dereg_mr(r->mr[1]);
    r->mr[1] = NULL;
errres:
    free(r->results);
    r->results = NULL;
errmr:
    ibv_dereg_mr(r->mr[0]);
    r->mr[0] = NULL;
errslots:
    free(r->shared_mem);
errpd:
    ibv_dealloc_pd(r->pd);
    r->pd = NULL;
exit:
    ibv_close_device(r->ctx);
    r->ctx = NULL;
    return -errno;
}

void rdma_destroy(struct rdma_ctx *r) {
    for (int i = 0; i < 2; ++i)
        if (r->mr[i]) {
            ibv_dereg_mr(r->mr[i]);
            r->mr[i] = NULL;
        }
    for (int i = 0; i < r->c->n; ++i) {
        if (r->qp[i]) {
            ibv_destroy_qp(r->qp[i]);
            r->qp[i] = NULL;
        }
        if (r->fqp) {
            ibv_destroy_qp(r->fqp[i]);
            r->fqp[i] = NULL;
        }
    }
    if (r->cq) {
        ibv_destroy_cq(r->cq);
        r->cq = NULL;
    }
    if (r->fcq) {
        ibv_destroy_cq(r->fcq);
        r->fcq = NULL;
    }
    if (r->pd) {
        ibv_dealloc_pd(r->pd);
        r->pd = NULL;
    }
    if (r->ctx) {
        ibv_close_device(r->ctx);
        r->ctx = NULL;
    }
    free(r->ra);
    free(r->qp);
    free(r->fqp);
    free(r->shared_mem);
    free(r->prepares);
    free(r->results);
    r->ra = NULL;
    r->qp = NULL;
    r->fqp = NULL;
    r->shared_mem = NULL;
    r->results = NULL;
    r->prepares = NULL;
}
