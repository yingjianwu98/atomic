#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "net_map.h"
#include "rdma.h"

struct request_msg {
    uint8_t op_type;
    uint32_t slot;
};

struct client_thread_args {
    int thread_id;
    int num_requests;
};

void *client_thread(void *arg) {
    struct client_thread_args *args = (struct client_thread_args *)arg;
    int num_requests = args->num_requests;
    int num_nodes = sizeof(net_cfg) / sizeof(net_cfg[0]);

    FAA_LOG("Client thread %d: starting with %d requests", args->thread_id,
            num_requests);

    // Connect to all nodes
    int sockets[num_nodes];
    for (int i = 0; i < num_nodes; i++) {
        sockets[i] = socket(AF_INET, SOCK_STREAM, 0);
        if (sockets[i] < 0) {
            perror("socket");
            return NULL;
        }

        struct sockaddr_in server_addr = {
            .sin_family = AF_INET,
            .sin_addr.s_addr = htonl(net_cfg[i].v),
            .sin_port = htons(CLIENT_SERVICE_PORT)};

        if (connect(sockets[i], (struct sockaddr *)&server_addr,
                    sizeof(server_addr)) < 0) {
            perror("connect");
            close(sockets[i]);
            return NULL;
        }
    }

    FAA_LOG("Client thread %d: connected to all nodes", args->thread_id);

    int completed = 0;
    for (int i = 0; i < num_requests; ++i) {
        int target_node = i % num_nodes;
        struct request_msg req = {.op_type = 0, .slot = 0};
        if (send(sockets[target_node], &req, sizeof(req), 0) < 0) {
            perror("send");
            break;
        }
        int64_t result;
        ssize_t n =
            recv(sockets[target_node], &result, sizeof(result), MSG_WAITALL);
        if (n != sizeof(result)) {
            perror("recv");
            break;
        }
        if (result == -ENOMEM) break;
        if (++completed % 10000 == 0)
            FAA_LOG("Client thread %d: %d requests completed", args->thread_id,
                    completed);
    }

    // Close connections
    for (int i = 0; i < num_nodes; ++i) close(sockets[i]);
    FAA_LOG("Client thread %d: finished (%d/%d requests)", args->thread_id,
            completed, num_requests);

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <num_threads> <requests_per_thread>\n",
                argv[0]);
        return 1;
    }

    int num_threads = atoi(argv[1]);
    int requests_per_thread = atoi(argv[2]);
    int num_nodes = sizeof(net_cfg) / sizeof(net_cfg[0]);

    printf("================================\n\n");
    printf("Cluster nodes: %d\n", num_nodes);
    printf("Client threads: %d\n", num_threads);
    printf("Requests per thread: %d\n", requests_per_thread);
    printf("Total requests: %d\n", num_threads * requests_per_thread);
    printf("================================\n\n");

    pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
    struct client_thread_args *args =
        malloc(sizeof(struct client_thread_args) * num_threads);

    uint64_t start_time = ts_us();

    for (int i = 0; i < num_threads; ++i) {
        args[i].thread_id = i;
        args[i].num_requests = requests_per_thread;
        if (pthread_create(&threads[i], NULL, client_thread, args + i)) {
            perror("pthread_create");
            return -errno;
        }
    }

    for (int i = 0; i < num_threads; i++) pthread_join(threads[i], NULL);

    uint64_t total_time = ts_us() - start_time;
    double throughput =
        (num_threads * requests_per_thread) / (total_time / 1000000.0);

    printf("===============\n");
    printf("Total time: %.2f seconds\n", total_time / 1000000.0);
    printf("Throughput: %.2f ops/sec\n", throughput);
    printf("===============\n");

    free(threads);
    free(args);
    return 0;
}
