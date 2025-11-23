#define _GNU_SOURCE
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "net_map.h"
#include "node.h"

struct request_msg {
    uint8_t op_type;  // 0 = FAA, 1 = TAS
    uint32_t slot;    // For TAS: which slot to test-and-set
};

struct client_handler_args {
    int client_fd;
    struct node_ctx *ctx;
    int client_id;
    int node_id;
};

void *handle_client(void *arg) {
    struct client_handler_args *args = (struct client_handler_args *)arg;
    int client_fd = args->client_fd;
    struct node_ctx *ctx = args->ctx;
    int client_id = args->client_id;
    int node_id = args->node_id;

    char filename[256];
    snprintf(filename, sizeof(filename), "latency_node%d_client%d.csv", node_id,
             client_id);
    FILE *log = fopen(filename, "w");
    if (log) fprintf(log, "Node,Slot,Latency_us,OpType\n");

    int request_count = 0;

    while (1) {
        // Read request
        struct request_msg req;
        ssize_t n = recv(client_fd, &req, sizeof(req), MSG_WAITALL);
        if (n <= 0) break;

        uint64_t start, elapsed;
        int64_t result;
        if (req.op_type == 0) {
            // FAA
            start = ts_us();
            result = fetch_and_add(ctx);
            elapsed = ts_us() - start;
        } else if (req.op_type == 1) {
            // TAS
            start = ts_us();
            result = test_and_set(ctx, req.slot);
            elapsed = ts_us() - start;
        } else
            continue;

        if (log && result >= 0) {
            fprintf(log, "%d,%ld,%lu,%d\n", node_id, result, elapsed,
                    req.op_type);
            fflush(log);
        }

        // Send response
        send(client_fd, &result, sizeof(result), 0);
        ++request_count;
        if (result == -ENOMEM) break;
    }

    if (log) fclose(log);
    close(client_fd);
    free(args);
    return NULL;
}

void *client_service_thread(void *arg) {
    struct node_ctx *ctx = (struct node_ctx *)arg;
    struct config *c = ctx->r.c;
    int host_id = c->host_id;

    uint16_t service_port = CLIENT_SERVICE_PORT;
    uint32_t host_ip = c->c[host_id].v;

    int serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverfd < 0) {
        perror("socket");
        return NULL;
    }

    int optval = 1;
    setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));

    struct sockaddr_in server_addr = {.sin_family = AF_INET,
                                      .sin_addr.s_addr = htonl(host_ip),
                                      .sin_port = htons(service_port)};

    if (bind(serverfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) <
        0) {
        perror("bind");
        close(serverfd);
        return NULL;
    }

    if (listen(serverfd, 100) < 0) {
        perror("listen");
        close(serverfd);
        return NULL;
    }

    FAA_LOG("Node %d: Client service listening on %s:%d", host_id,
            inet_ntoa(server_addr.sin_addr), service_port);

    int client_count = 0;
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd =
            accept(serverfd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        // Spawn thread to handle this client
        pthread_t thread;
        struct client_handler_args *args = malloc(sizeof(*args));
        args->client_fd = client_fd;
        args->ctx = ctx;
        args->client_id = client_count++;
        args->node_id = host_id;

        pthread_create(&thread, NULL, handle_client, args);
        pthread_detach(thread);
    }

    close(serverfd);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <host id>\n", argv[0]);
        return 1;
    }

    int host_id = atoi(argv[1]);
    int num_nodes = sizeof(net_cfg) / sizeof(net_cfg[0]);

    if (host_id < 0 || host_id >= num_nodes) {
        fprintf(stderr, "Invalid host_id %d (must be 0-%d)\n", host_id,
                num_nodes - 1);
        return 1;
    }

    FAA_LOG("Node %d: Starting", host_id);

    struct node_ctx ctx;
    struct config c = {
        .n = num_nodes,
        .host_id = host_id,
        .rdma_device = 0,
        .c = (struct node_config *)net_cfg,
    };

    if (node_init(&ctx, &c) != 0) {
        FAA_LOG("Node %d: node_init failed", host_id);
        return 1;
    }

    FAA_LOG("Node %d: RDMA cluster initialized", host_id);

    // Start client service thread
    pthread_t service_thread;
    if (pthread_create(&service_thread, NULL, client_service_thread, &ctx) !=
        0) {
        perror("pthread_create");
        node_destroy(&ctx);
        return 1;
    }

    FAA_LOG("Node %d: Client service started", host_id);

    // Wait for service thread
    pthread_join(service_thread, NULL);

    node_destroy(&ctx);
    return 0;
}
