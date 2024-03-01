#!/bin/bash

set -ex

sudo modprobe nvme-fabrics
sudo modprobe nvme-tcp
sudo dnf install nvme-cli -y

source /etc/ceph/nvmeof.env

# RBD_POOL and RBD_IMAGE is intended to be set from yaml, 'mypool' and 'myimage' are defaults
RBD_POOL="${RBD_POOL:-mypool}"
RBD_IMAGE="${RBD_IMAGE:-myimage}"

HOSTNAME=$(hostname)
sudo podman images
sudo podman ps
sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT subsystem list
sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT subsystem add --subsystem $NVMEOF_NQN
sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT namespace add --subsystem $NVMEOF_NQN --rbd-pool $RBD_POOL --rbd-image $RBD_IMAGE
sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT listener add --subsystem $NVMEOF_NQN --gateway-name client.$NVMEOF_GATEWAY_NAME --traddr $NVMEOF_GATEWAY_IP_ADDRESS --trsvcid $NVMEOF_PORT
sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT host add --subsystem $NVMEOF_NQN --host "*"
sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT subsystem list
sudo lsmod | grep nvme
sudo nvme list

echo "[nvmeof] Initiator setup done"