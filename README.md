# libatomic

# HW Requirements

RDMA NIC, OFED drivers

# Dependencies

To install dependencies

```bash
sudo apt-get install rdma-core libibverbs-dev librdmacm-dev ibverbs-utils
```

# Build

```sh
make
sudo make install
```

# Running Benchmarks

Benchmarks can be found in bench/

1. Configure network

Edit the network config file in `bench/net_map.h`.

2. Build the benchmark

```bash
make bench
```

3. Run the server

On each replica node:

```bash
bench/server <node_id>
```

4. Run the client

From any client machine:

```bash
bench/client <Number of threads> <Requests per thread>
```

# Docker

```sh
# Build image
docker build -t atomic .

# Pass specific RDMA devices
docker run --device=/dev/infiniband/uverbs0 \
  --device=/dev/infiniband/rdma_cm \
  --cap-add=IPC_LOCK \
  --cap-add=SYS_RESOURCE \
  -it atomic
```

# Tests

Test binaries are found in `tests/`

RoCE can be setup for testing with

```sh
sudo make roce
```

and removed with

```sh
sudo make roce-clean
```
