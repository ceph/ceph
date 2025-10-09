#!/bin/bash -xe

# It's assumed in this test that each subsystem has equal number
# of namespaces (i.e. NVMEOF_NAMESPACES_COUNT ns per subsystem). 
# This script then adds NEW_NAMESPACES_COUNT amount of namespaces
# to each subsystem and then deletes those new namespaces.

source /etc/ceph/nvmeof.env

RBD_POOL="${RBD_POOL:-mypool}"
NEW_IMAGE_SIZE="${RBD_IMAGE_SIZE:-8192}" # 1024*8
NEW_NAMESPACES_COUNT="${NEW_NAMESPACES_COUNT:-3}"

gateways_count=$(( $(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | tr -cd ',' | wc -c) + 1 ))
new_images_count=$(( $NVMEOF_SUBSYSTEMS_COUNT * $NEW_NAMESPACES_COUNT)) 


assert_namespaces_count() {
    expected_count_per_subsys=$1
    actual_count=$(sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT --format json subsystem list | 
        grep namespace_count | grep $expected_count_per_subsys | wc -l)
    if [ "$actual_count" -ne "$NVMEOF_SUBSYSTEMS_COUNT" ]; then
        sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT --format json subsystem list
        echo "Expected count of namepaces not found, expected (per subsystem): $expected_count_per_subsys"
        return 1
    fi
}


# add rbd images
for i in $(seq 1 $new_images_count); do
    image_name="test${i}"
    rbd create $RBD_POOL/$image_name --size $NEW_IMAGE_SIZE
done

# add new namespaces
image_index=1
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    for ns in $(seq 1 $NEW_NAMESPACES_COUNT); do
        image="test${image_index}"
        sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT namespace add --subsystem $subsystem_nqn --rbd-pool $RBD_POOL --rbd-image $image --load-balancing-group $(($image_index % $gateways_count + 1))
        ((image_index++))
    done
done

# list namespaces
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT --format plain namespace list --subsystem $subsystem_nqn        
done

# verify namespaces added
expected_count_per_subsys=$(( $NEW_NAMESPACES_COUNT + $NVMEOF_NAMESPACES_COUNT ))
assert_namespaces_count $expected_count_per_subsys

# delete namespaces
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    NSIDs=$(sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT --format json namespace list --subsystem $subsystem_nqn | 
            jq -r '.namespaces[] | select(.rbd_image_name | startswith("test")) | .nsid')

    for nsid in $NSIDs; do
        sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT namespace del --subsystem $subsystem_nqn --nsid $nsid
    done
done

# verify namespaces deleted
expected_count_per_subsys=$NVMEOF_NAMESPACES_COUNT
assert_namespaces_count $expected_count_per_subsys

