#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "net_map.h"
#include "node.h"

#define NUM_INCREMENTS (100)

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <host id>\n", argv[0]);
        return 1;
    }

    int host_id = atoi(argv[1]);

    // Pin to CPU
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(host_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    struct node_ctx ctx;
    struct config c = {
        .n = sizeof(net_cfg) / sizeof(net_cfg[0]),
        .host_id = host_id,
        .rdma_device = 0,
        .c = (struct node_config *)net_cfg,
    };

    assert(!node_init(&ctx, &c));

    fprintf(stderr, "Host ID,Attempt,LL_Index,LL_Value,SC_Result,Latency_us\n");

    int successful_increments = 0;
    int total_attempts = 0;

    while (successful_increments < NUM_INCREMENTS) {
        uint64_t start = ts_us();
        uint64_t value = 0;

        // Load-Link: Read current value
        int ll_ret = load_link(&ctx, &value);
        if (ll_ret != 0) {
            fprintf(stderr, "Load-Link failed\n");
            usleep(100);
            continue;
        }

        // Increment value
        uint64_t new_value = value + 1;

        // Store-Conditional: Try to write incremented value
        int sc_ret = store_conditional(&ctx, new_value);

        uint64_t elapsed = ts_us() - start;
        total_attempts++;

        // Log result
        const char *result_str = (sc_ret == 0) ? "SUCCESS" : "FAILED";
        fprintf(stderr, "%d,%d,%u,%lu,%s,%lu\n",
                host_id, total_attempts, ctx.my_index, value, result_str, elapsed);

        if (sc_ret == 0) {
            successful_increments++;
        } else {
            // SC failed, retry with backoff
            usleep(10 + (rand() % 100));
        }
    }

    fprintf(stderr, "\nNode %d Summary:\n", host_id);
    fprintf(stderr, "  Successful increments: %d\n", successful_increments);
    fprintf(stderr, "  Total attempts: %d\n", total_attempts);
    fprintf(stderr, "  Success rate: %.2f%%\n",
            100.0 * successful_increments / total_attempts);

    node_destroy(&ctx);
    return 0;
}
