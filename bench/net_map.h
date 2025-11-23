#ifndef NET_MAP_H
#define NET_MAP_H

#include "config.h"

/* Static definition of the network map */
#define TCP_PORT (8888)
#define CLIENT_SERVICE_PORT (9000)
#define IB_PORT (1)
#define GID_IDX (0)

static const struct node_config net_cfg[] = {
    {
        .ip = {2, 1, 10, 10},
        .id = 0,
        .tcp_port = TCP_PORT,
        .ib_port = IB_PORT,
        .gid_index = GID_IDX,
    },
    {
        .ip = {3, 1, 10, 10},
        .id = 1,
        .tcp_port = TCP_PORT,
        .ib_port = IB_PORT,
        .gid_index = GID_IDX,
    },
    {
        .ip = {4, 1, 10, 10},
        .id = 2,
        .tcp_port = TCP_PORT,
        .ib_port = IB_PORT,
        .gid_index = GID_IDX,
    },
    {
        .ip = {5, 1, 10, 10},
        .id = 3,
        .tcp_port = TCP_PORT,
        .ib_port = IB_PORT,
        .gid_index = GID_IDX,
    },
    {
        .ip = {6, 1, 10, 10},
        .id = 4,
        .tcp_port = TCP_PORT,
        .ib_port = IB_PORT,
        .gid_index = GID_IDX,
    },

    {
        .ip = {7, 1, 10, 10},
        .id = 5,
        .tcp_port = TCP_PORT,
        .ib_port = IB_PORT,
        .gid_index = GID_IDX,
    },
    {
        .ip = {8, 1, 10, 10},
        .id = 6,
        .tcp_port = TCP_PORT,
        .ib_port = IB_PORT,
        .gid_index = GID_IDX,
    },
};

#endif /* NET_MAP_H */
