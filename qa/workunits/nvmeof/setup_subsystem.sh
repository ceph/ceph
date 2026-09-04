#!/bin/bash

set -ex


source /etc/ceph/nvmeof.env

# Set these in job yaml
RBD_POOL="${RBD_POOL:-mypool}"
RBD_IMAGE_PREFIX="${RBD_IMAGE_PREFIX:-myimage}"
NVMEOF_NATIVE_CLI="${NVMEOF_NATIVE_CLI:-false}"
NVMEOF_AUTO_LISTENER="${NVMEOF_AUTO_LISTENER:-false}"

HOSTNAME=$(hostname)
sudo podman images
sudo podman ps

nvmeof_cli () {
    local server_ip="$1"
    shift
    for attempt in 1 2 3; do
        if [ "$NVMEOF_NATIVE_CLI" = "true" ]; then
            ceph nvmeof --server-address $server_ip "$@" && return 0
        else
            sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $server_ip --server-port $NVMEOF_SRPORT "$@" && return 0
        fi
        echo "[nvmeof_cli] attempt $attempt failed, retrying..." >&2
        sleep 2
    done
    return 1
}

# some different flags in native CLI and  container CLI
if [ "$NVMEOF_NATIVE_CLI" = "true" ]; then
    SUBSYSTEM_FLAG="--nqn"
    HOST_FLAG="--host-nqn"
    RBD_IMAGE_FLAG="--rbd-image-name"
else
    SUBSYSTEM_FLAG="--subsystem"
    HOST_FLAG="--host"
    RBD_IMAGE_FLAG="--rbd-image"
fi

nvmeof_cli $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --format json subsystem list

IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
IFS=',' read -ra gateway_names <<< "$NVMEOF_GATEWAY_NAMES"
gateways_count=$(( $(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | tr -cd ',' | wc -c) + 1 ))

echo "[nvmeof] Starting subsystem setup..."

# add all subsystems
extra_subsystem_add_args=""
if [ "$NVMEOF_AUTO_LISTENER" = "true" ]; then
    network_mask="${NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS%.*}.0/24"
    extra_subsystem_add_args="--network-mask $network_mask"
fi
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    nvmeof_cli $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS subsystem add $SUBSYSTEM_FLAG $subsystem_nqn --no-group-append $extra_subsystem_add_args
done

# add all gateway listeners 
if [ "$NVMEOF_AUTO_LISTENER" = "false" ]; then
    for i in "${!gateway_ips[@]}"
    do
        ip="${gateway_ips[i]}"
        name="${gateway_names[i]}"
        for j in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
            subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${j}"
            echo "Adding gateway listener $i with IP ${ip} and name ${name}"
            nvmeof_cli $ip listener add $SUBSYSTEM_FLAG $subsystem_nqn --host-name $name --traddr $ip --trsvcid $NVMEOF_PORT
        done
    done
fi

# add all hosts
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    nvmeof_cli $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS host add $SUBSYSTEM_FLAG $subsystem_nqn $HOST_FLAG "*"
done

# add all namespaces
image_index=1
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    for ns in $(seq 1 $NVMEOF_NAMESPACES_COUNT); do
        image="${RBD_IMAGE_PREFIX}${image_index}"
        nvmeof_cli $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS namespace add $SUBSYSTEM_FLAG $subsystem_nqn --rbd-pool $RBD_POOL $RBD_IMAGE_FLAG $image --load-balancing-group $(($image_index % $gateways_count + 1))
        ((image_index++))
    done
done

echo "[nvmeof] Verifying subsystem setup..."

# verify subsystem count on each gateway
for i in "${!gateway_ips[@]}"; do
    ip="${gateway_ips[i]}"
    output=$(nvmeof_cli $ip --format json subsystem list)
    actual_count=$(echo "$output" | jq '.subsystems | length')
    if [ "$actual_count" -ne "$NVMEOF_SUBSYSTEMS_COUNT" ]; then
        echo "[nvmeof] ERROR: gateway $ip reports $actual_count subsystems, expected $NVMEOF_SUBSYSTEMS_COUNT"
        echo "$output"
        exit 1
    fi
done

# verify namespace count and listeners for each subsystem, on each gateway
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"

    for gw_ip in "${gateway_ips[@]}"; do
        ns_output=$(nvmeof_cli $gw_ip --format json namespace list $SUBSYSTEM_FLAG $subsystem_nqn)
        actual_ns_count=$(echo "$ns_output" | jq '.namespaces | length')
        if [ "$actual_ns_count" -ne "$NVMEOF_NAMESPACES_COUNT" ]; then
            echo "[nvmeof] ERROR: subsystem $subsystem_nqn has $actual_ns_count namespaces via gateway $gw_ip, expected $NVMEOF_NAMESPACES_COUNT"
            echo "$ns_output"
            exit 1
        fi

        listener_output=$(nvmeof_cli $gw_ip --format json listener list $SUBSYSTEM_FLAG $subsystem_nqn)
        for ip in "${gateway_ips[@]}"; do
            has_listener=$(echo "$listener_output" | jq --arg ip "$ip" '[.listeners[] | select(.traddr == $ip)] | length')
            if [ "$has_listener" -eq 0 ]; then
                echo "[nvmeof] ERROR: subsystem $subsystem_nqn has no listener for gateway $ip (queried via $gw_ip)"
                echo "$listener_output"
                exit 1
            fi
        done
    done
done

echo "[nvmeof] Verified $NVMEOF_SUBSYSTEMS_COUNT subsystems, $NVMEOF_NAMESPACES_COUNT namespaces each, and listeners on all gateways"

echo "[nvmeof] Subsystem setup done"
