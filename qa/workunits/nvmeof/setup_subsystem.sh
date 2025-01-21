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

IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
IFS=',' read -ra gateway_names <<< "$NVMEOF_GATEWAY_NAMES"
gateways_count=$(( $(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | tr -cd ',' | wc -c) + 1 ))

list_subsystems () { 
   for i in "${!gateway_ips[@]}"
    do
        ip="${gateway_ips[i]}"
        sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT --format json subsystem list
    done
}

list_namespaces () { 
    for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
        subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
        sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT --format plain namespace list --subsystem $subsystem_nqn        
    done
}

echo "[nvmeof] Starting subsystem setup..."

# add all subsystems
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT subsystem add --subsystem $subsystem_nqn --no-group-append
done

# add all gateway listeners 
for i in "${!gateway_ips[@]}"
do
    ip="${gateway_ips[i]}"
    name="${gateway_names[i]}"
    for j in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
        subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${j}"
        echo "Adding gateway listener $index with IP ${ip} and name ${name}"
        sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT listener add --subsystem $subsystem_nqn --host-name $name --traddr $ip --trsvcid $NVMEOF_PORT
    done
done

# add all hosts
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT host add --subsystem $subsystem_nqn --host "*"
done

# add all namespaces
image_index=1
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    for ns in $(seq 1 $NVMEOF_NAMESPACES_COUNT); do
        image="${RBD_IMAGE_PREFIX}${image_index}"
        sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT namespace add --subsystem $subsystem_nqn --rbd-pool $RBD_POOL --rbd-image $image --load-balancing-group $(($image_index % $gateways_count + 1))
        ((image_index++))
    done
done

list_subsystems


echo "[nvmeof] Subsystem setup done"
