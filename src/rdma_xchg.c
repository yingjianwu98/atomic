#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#include "rdma.h"

// Max number of retries on failed connections
#define MAX_RETRIES (1 << 2)

// Max number of outstanding RDMA reads & atomic operations on the dest QP
#define MAX_RD_ATOMIC (1 << 3)

/* Size of remote attribute struct (packed) sent over TCP */
#define RX_LEN (sizeof(struct remote_attr))

/* Convert bytes to network order */
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define htonll(x) (((uint64_t)htonl(x) << 32) | htonl((x) >> 32))
#define ntohll(x) (((uint64_t)ntohl(x) << 32) | ntohl((x) >> 32))
#else
#define htonll(x) (x)
#define ntohll(x) (x)
#endif

/* Convert remote attribute struct from host order to network order */
#define RA_TO_NET(r)                   \
    do {                               \
        (r)->addr = htonll((r)->addr); \
        (r)->rkey = htonl((r)->rkey);  \
        (r)->lid = htons((r)->lid);    \
        (r)->qpn = htonl((r)->qpn);    \
        (r)->psn = htonl((r)->psn);    \
    } while (0)

/* Convert remote attribute struct from network order to host order */
#define RA_FROM_NET(r)                 \
    do {                               \
        (r)->addr = ntohll((r)->addr); \
        (r)->rkey = ntohl((r)->rkey);  \
        (r)->lid = ntohs((r)->lid);    \
        (r)->qpn = ntohl((r)->qpn);    \
        (r)->psn = ntohl((r)->psn);    \
    } while (0)

/* Thread args for the RDMA handshake */
struct rdma_xchg_args {
    struct rdma_ctx *r;
    int id;
    int ret;
};

// Get local attributes for a given peer on this host
void __get_local_attr(struct rdma_ctx *r, struct remote_attr *p, int id,
                      int frontier) {
    p->addr = (uint64_t)r->mr[0]->addr;
    p->rkey = r->mr[0]->rkey;
    p->lid = r->lid;
    p->qpn = (frontier ? r->fqp[id] : r->qp[id])->qp_num;
    p->psn = 0;
#pragma GCC unroll 16
    for (int i = 0; i < 16; ++i) p->gid[i] = r->gid[i];
}

// Connect local QP using remote QP info
int __qp_connect(struct rdma_ctx *r, struct node_config *c,
                 struct remote_attr *ra, int frontier) {
    int ret = 0;
    uint16_t ib_port = c->ib_port;
    uint16_t gid_index = c->gid_index;
    struct ibv_qp_attr rtr_attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_1024,
        .max_dest_rd_atomic = MAX_RD_ATOMIC,
        .min_rnr_timer = 0x12,
        .ah_attr.is_global = 1,
        .ah_attr.sl = 0,
        .ah_attr.src_path_bits = 0,
        .ah_attr.grh.flow_label = 0,
        .ah_attr.grh.hop_limit = 1,
        .ah_attr.grh.traffic_class = 0,
        .ah_attr.port_num = ib_port,
        .ah_attr.grh.sgid_index = gid_index,
        .rq_psn = ra->psn,
        .dest_qp_num = ra->qpn,
        .ah_attr.dlid = ra->lid,
    };
    for (int i = 0; i < 16; ++i) rtr_attr.ah_attr.grh.dgid.raw[i] = ra->gid[i];

    // set QP to RTR state
    ret = ibv_modify_qp(frontier ? r->fqp[c->id] : r->qp[c->id], &rtr_attr,
                        IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                            IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (ret) {
        FAA_LOG("Failed to set QP to RTR state");
        return ret;
    }

    struct ibv_qp_attr rts_attr;
    memset(&rts_attr, 0, sizeof(rts_attr));
    rts_attr.qp_state = IBV_QPS_RTS;
    rts_attr.timeout = 0x12;
    rts_attr.retry_cnt = 7;
    rts_attr.rnr_retry = 7;
    rts_attr.sq_psn = ra->psn;
    rts_attr.max_rd_atomic = MAX_RD_ATOMIC;

    // set QP to RTS state
    ret = ibv_modify_qp(frontier ? r->fqp[c->id] : r->qp[c->id], &rts_attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret) FAA_LOG("Failed to set QP to RTS state");

    return ret;
}

// Server loop: accepts connections from higher-ranked peers
void *__server_thread(void *ptr) {
    struct remote_attr local;
    struct sockaddr_in server, client;
    socklen_t clientlen = sizeof(client);
    int serverfd, clientfd, nbytes, optval = 1;

    struct rdma_ctx *r = ((struct rdma_xchg_args *)ptr)->r;
    int *ret = &((struct rdma_xchg_args *)ptr)->ret;
    struct config *c = r->c;

    uint16_t host_port = c->c[c->host_id].tcp_port;
    uint32_t host_ip = c->c[c->host_id].v;

    if ((serverfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket:");
        *ret = -errno;
        pthread_exit(NULL);
    }

    setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));
    memset(&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(host_ip);
    server.sin_port = htons(host_port);

    if (bind(serverfd, (struct sockaddr *)&server, sizeof(server)) < 0) {
        perror("bind:");
        goto err;
    }

    if (listen(serverfd, c->n) < 0) {
        perror("listen:");
        goto err;
    }

    FAA_LOG("Server listening on %s:%d", inet_ntoa(server.sin_addr), host_port);

    // Accept connections from higher-ranked peers
    size_t expected_clients = c->n - c->host_id - 1;
    for (size_t i = 0; i < expected_clients; ++i) {
        clientfd = accept(serverfd, (struct sockaddr *)&client, &clientlen);
        if (clientfd < 0)
            perror("accept:");
        else {
            FAA_LOG("Established connection with %s",
                    inet_ntoa(client.sin_addr));

            // Read incoming peer ID.
            uint16_t id = 0;
            if ((nbytes = read(clientfd, &id, sizeof id)) != sizeof id) {
                perror("read");
                continue;
            }
            id = ntohs(id);
            FAA_LOG("Server received client ID = %d", id);

            // get local attributes for this host
            __get_local_attr(r, &local, id, 0);

            // write local attributes to remote peer
            RA_TO_NET(&local);
            if ((nbytes = write(clientfd, &local, RX_LEN)) != RX_LEN) {
                perror("write");
                close(clientfd);
                *ret = errno;
                goto err;
            }

            // read remote attributes from remote peer
            if ((nbytes = read(clientfd, r->ra + id, RX_LEN)) != RX_LEN) {
                perror("read");
                close(clientfd);
                *ret = errno;
                goto err;
            }
            RA_FROM_NET(r->ra + id);

            if (__qp_connect(r, c->c + id, r->ra + id, 0)) {
                FAA_LOG("QP connection failed");
                close(clientfd);
                *ret = 2;
                goto err;
            }

            // get frontier attributes for this host
            __get_local_attr(r, &local, id, 1);
            // write frontier attributes to remote peer
            RA_TO_NET(&local);
            if ((nbytes = write(clientfd, &local, RX_LEN)) != RX_LEN) {
                perror("write");
                close(clientfd);
                *ret = errno;
                goto err;
            }
            FAA_LOG("[%hu] Sent frontier attributes to node %hu\n", c->host_id,
                    id);
            memset(&local, 0, sizeof(local));
            // read frontier attributes from remote peer
            if ((nbytes = read(clientfd, &local, RX_LEN)) != RX_LEN) {
                perror("read");
                close(clientfd);
                *ret = errno;
                goto err;
            }
            RA_FROM_NET(&local);
            // connect queue pairs
            if (__qp_connect(r, c->c + id, &local, 1)) {
                FAA_LOG("QP connection failed");
                close(clientfd);
                *ret = 2;
                goto err;
            }
            FAA_LOG("[%hu] Connected frontier QP to node %hu\n", c->host_id,
                    id);

            FAA_LOG("RDMA exchange with node %d success", id);
            close(clientfd);
        }
    }

    close(serverfd);
    pthread_exit(NULL);

err:
    close(serverfd);
    *ret = -errno;
    pthread_exit(NULL);
}

// Client thread connects to a lower ranked peer
void *__client_thread(void *ptr) {
    int i, sockfd, nbytes;
    struct remote_attr local;
    struct sockaddr_in serveraddr;
    struct rdma_ctx *r = ((struct rdma_xchg_args *)ptr)->r;
    int id = ((struct rdma_xchg_args *)ptr)->id;
    int *ret = &((struct rdma_xchg_args *)ptr)->ret;
    struct config *c = r->c;
    struct node_config *remote_cfg = c->c + id;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket");
        *ret = -errno;
        pthread_exit(NULL);
    }

    memset(&serveraddr, 0, sizeof serveraddr);
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(remote_cfg->v);
    serveraddr.sin_port = htons(remote_cfg->tcp_port);

    /* Establish connection */
    for (i = 0; i < MAX_RETRIES; ++i) {
        sleep(5);
        if (!connect(sockfd, (struct sockaddr *)&serveraddr,
                     sizeof(serveraddr))) {
            break;
        } else {
            perror("connect");
            FAA_LOG("Connection failed. Retrying...");
        }
    }
    if (i >= MAX_RETRIES) {
        FAA_LOG("Host unreachable.");
        *ret = 1;
        goto exit;
    }

    /* Connection established. Exchange attributes */
    FAA_LOG("Established connection with node %d", id);

    // write peer id to server
    uint16_t hostid = htons(c->host_id);
    if ((nbytes = write(sockfd, &hostid, sizeof hostid)) != sizeof hostid) {
        perror("write");
        *ret = -errno;
        goto exit;
    }

    // local attributes for this host
    __get_local_attr(r, &local, id, 0);
    // write local attributes to peer
    RA_TO_NET(&local);
    if ((nbytes = write(sockfd, &local, RX_LEN)) != RX_LEN) {
        perror("write");
        *ret = errno;
        goto exit;
    }
    // read remote attributes from peer
    if ((nbytes = read(sockfd, r->ra + id, RX_LEN)) != RX_LEN) {
        perror("read");
        *ret = errno;
        goto exit;
    }
    RA_FROM_NET(r->ra + id);
    // connect queue pairs
    if (__qp_connect(r, c->c + id, r->ra + id, 0)) {
        FAA_LOG("QP connection failed");
        *ret = 2;
        goto exit;
    }

    // local attributes for this host
    __get_local_attr(r, &local, id, 1);

    // write local attributes to peer
    RA_TO_NET(&local);
    if ((nbytes = write(sockfd, &local, RX_LEN)) != RX_LEN) {
        perror("write");
        *ret = errno;
        goto exit;
    }

    memset(&local, 0, sizeof(local));

    // read remote attributes from peer
    if ((nbytes = read(sockfd, &local, RX_LEN)) != RX_LEN) {
        perror("read");
        *ret = errno;
        goto exit;
    }
    RA_FROM_NET(&local);
    FAA_LOG("[%hu] Received frontier RA from node %d", c->host_id, id);

    // connect queue pairs
    if (__qp_connect(r, c->c + id, &local, 1)) {
        FAA_LOG("QP connection failed");
        *ret = 2;
        goto exit;
    }

    FAA_LOG("[%hu] connected frontier QP to node %d", c->host_id, id);

    FAA_LOG("RDMA exchange with node %d success", id);
exit:
    close(sockfd);
    pthread_exit(NULL);
}

int rdma_handshake(struct rdma_ctx *r) {
    struct config *c = r->c;
    pthread_t st, ct[c->host_id];
    struct rdma_xchg_args sa = {.r = r, .ret = 0}, ca[c->host_id];
    int server = c->host_id != c->n - 1;

    /* Highest ranking peer doesn't serve */
    if (server && pthread_create(&st, NULL, __server_thread, (void *)&sa)) {
        perror("pthread_create:");
        return -errno;
    }

    /* Connect to lower peers */
    for (size_t i = 0; i < c->host_id; ++i) {
        ca[i].r = r;
        ca[i].id = i;
        ca[i].ret = 0;
        if (pthread_create(ct + i, NULL, __client_thread, (void *)(ca + i))) {
            perror("pthread_create:");
            return -errno;
        }
    }

    /* Client threads block here */
    for (size_t i = 0; i < c->host_id; ++i) {
        pthread_join(ct[i], NULL);
        FAA_LOG("Client thread exited with status %d", ca[i].ret);
        if (ca[i].ret) {
            return ca[i].ret;
        }
    }

    /* Server loop blocks here */
    if (server) pthread_join(st, NULL);

    /* Setup loopback connection for frontier FAA */
    if (!sa.ret) {
        __get_local_attr(r, r->ra + c->host_id, c->host_id, 1);
        return __qp_connect(r, c->c + c->host_id, r->ra + c->host_id, 1);
    }

    return sa.ret;
}
