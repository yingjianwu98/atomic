#ifndef NODE_H
#define NODE_H

#include "rdma.h"

/* Per-node context */
struct node_ctx {
  uint16_t id;
  uint32_t seed;
  struct rdma_ctx r;
#ifdef TRACK_SLOTS
  struct {
    uint64_t lat;
    uint16_t path;
    uint16_t won;
  } *s;
#endif
};

/* Initialize node context */
int node_init(struct node_ctx *ctx, struct config *c);

/* Destroy context */
void node_destroy(struct node_ctx *ctx);

/* Distributed atomic operations */
int64_t fetch_and_add(struct node_ctx *ctx);
int64_t test_and_set(struct node_ctx *ctx, uint32_t slot);

#ifdef TRACK_SLOTS
#include <stdio.h>
#define DUMP_CSV(fp, ctx)                                                      \
  do {                                                                         \
    fprintf(fp, "Host ID,Slot,Latency,Path,Won\n");                            \
    for (int i = 0; i < MAX_SLOTS; ++i)                                        \
      if ((ctx)->s[i].lat)                                                     \
        fprintf(fp, "%hu,%d,%lu,%hu,%hu\n", (ctx)->id, i, (ctx)->s[i].lat,     \
                (ctx)->s[i].path, (ctx)->s[i].won);                            \
  } while (0)
#endif

#endif /* NODE_H */
