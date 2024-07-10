#!/bin/bash

set -ex


source /etc/ceph/nvmeof.env

# Set these in job yaml
RBD_POOL="${RBD_POOL:-mypool}"
RBD_IMAGE_PREFIX="${RBD_IMAGE_PREFIX:-myimage}"

HOSTNAME=$(hostname)
sudo podman images
sudo podman ps
sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT --format json subsystem list
sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT subsystem add --subsystem $NVMEOF_NQN

# add all namespaces
for i in $(seq 1 $NVMEOF_NAMESPACES_COUNT); do
    image="${RBD_IMAGE_PREFIX}${i}"
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT namespace add --subsystem $NVMEOF_NQN --rbd-pool $RBD_POOL --rbd-image $image
done

# add all gateway listeners
IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
IFS=',' read -ra gateway_names <<< "$NVMEOF_GATEWAY_NAMES"
for i in "${!gateway_ips[@]}"
do
    ip="${gateway_ips[i]}"
    name="${gateway_names[i]}"
    echo "Adding gateway listener $index with IP ${ip} and name ${name}"
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT listener add --subsystem $NVMEOF_NQN --gateway-name client.$name --traddr $ip --trsvcid $NVMEOF_PORT
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT --format json subsystem list
done

sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT host add --subsystem $NVMEOF_NQN --host "*"

echo "[nvmeof] Subsystem setup done"
