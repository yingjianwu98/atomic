# Download base image
FROM ubuntu:latest

# Disable Prompt During Packages Installation
ARG DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt update -y
RUN apt upgrade -y
RUN apt-get install -y build-essential make rdma-core libibverbs-dev librdmacm-dev ibverbs-utils

# Copy and build library
COPY . atomic
RUN make -C atomic all install

ENTRYPOINT ["/bin/bash"]
