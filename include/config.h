#ifndef CONFIG_H
#define CONFIG_H

#include <stdint.h>

#define MAX_SLOTS (1000000)
#define FRONTIER_NODE (0)
#define MAX_CONCURRENT_REQ (64)
#define TRACK_SLOTS (1)

// #define DEBUG (0)

#ifdef DEBUG
#include <stdio.h>
#define __FAA_LOG(fmt, ...)                                                    \
  do {                                                                         \
    time_t _now = time(NULL);                                                  \
    struct tm _tm;                                                             \
    localtime_r(&_now, &_tm);                                                  \
    char _buf[20];                                                             \
    strftime(_buf, sizeof(_buf), "%Y-%m-%d %H:%M:%S", &_tm);                   \
    fprintf(stderr, "[%s][%s:%d] " fmt "\n", _buf, __FILE__, __LINE__,         \
            ##__VA_ARGS__);                                                    \
  } while (0)
#define FAA_LOG(MSG, ...) __FAA_LOG(MSG, ##__VA_ARGS__)
#else
#define FAA_LOG(MSG, ...)                                                      \
  do {                                                                         \
  } while (0)
#endif

/* Node entry */
struct node_config {
  union {
    char ip[4];
    uint32_t v;
  }; // peer ip addr
  uint16_t id;        // peer rank
  uint16_t tcp_port;  // peer tcp port
  uint16_t ib_port;   // peer ib device port
  uint16_t gid_index; // peer ib device global id
};

/* Node configuration used for network discovery
 * during the initial bootstrapping phase.
 * Every node should have a copy of this struct. */
struct config {
  uint16_t n;            // number of nodes
  uint16_t host_id;      // this node's rank
  uint8_t rdma_device;   // index into rdma device list
  struct node_config *c; // all nodes
};

#endif /* CONFIG_H */
