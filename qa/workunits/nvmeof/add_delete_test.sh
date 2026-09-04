#!/bin/bash -xe


source /etc/ceph/nvmeof.env

RBD_POOL="${RBD_POOL:-mypool}"
NEW_IMAGE_SIZE="${NEW_IMAGE_SIZE:-8192}" # 1024*8 MB / 8 GB
NEW_NAMESPACES_COUNT="${NEW_NAMESPACES_COUNT:-3}"
NEW_SUBSYSTEMS_COUNT="${NEW_SUBSYSTEMS_COUNT:-2}"
NEW_HOSTS_COUNT="${NEW_HOSTS_COUNT:-3}"

gateways_count=$(( $(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | tr -cd ',' | wc -c) + 1 ))
new_images_count=$(( $NVMEOF_SUBSYSTEMS_COUNT * $NEW_NAMESPACES_COUNT ))

IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
IFS=',' read -ra gateway_names <<< "$NVMEOF_GATEWAY_NAMES"


assert_namespaces_count() {
    expected_count_per_subsys=$1
    for gw_ip in "${gateway_ips[@]}"; do
        actual_count=$(ceph nvmeof --server-address $gw_ip --format json subsystem list |
            jq --argjson expected "$expected_count_per_subsys" '[.subsystems[] | select(.namespace_count == $expected)] | length')
        if [ "$actual_count" -ne "$NVMEOF_SUBSYSTEMS_COUNT" ]; then
            ceph nvmeof --server-address $gw_ip --format json subsystem list
            echo "Expected count of namespaces not found via gateway $gw_ip, expected (per subsystem): $expected_count_per_subsys"
            return 1
        fi
    done
}

assert_subsystems_count() {
    expected_count=$1
    for gw_ip in "${gateway_ips[@]}"; do
        actual_count=$(ceph nvmeof --server-address $gw_ip --format json subsystem list |
            jq '.subsystems | length')
        if [ "$actual_count" -ne "$expected_count" ]; then
            ceph nvmeof --server-address $gw_ip --format json subsystem list
            echo "Expected $expected_count subsystems via gateway $gw_ip, found $actual_count"
            return 1
        fi
    done
}

# count named hosts (i.e. not *)
assert_hosts_count() {
    subsystem_nqn=$1
    expected_count=$2
    for gw_ip in "${gateway_ips[@]}"; do
        actual_count=$(ceph nvmeof --server-address $gw_ip --format json host list --nqn $subsystem_nqn |
            jq '[.hosts[] | select(.nqn != "*")] | length')
        if [ "$actual_count" -ne "$expected_count" ]; then
            ceph nvmeof --server-address $gw_ip --format json host list --nqn $subsystem_nqn
            echo "Expected $expected_count hosts on $subsystem_nqn via gateway $gw_ip, found $actual_count"
            return 1
        fi
    done
}

connected_or_disconnected() { [ "$1" = "true" ] && echo "connected" || echo "disconnected"; }

# Only checks the default gateway: the initiator connects via a single traddr,
# so no session exists on the other gateways to check.
assert_host_connected() {
    subsystem_nqn=$1
    host_nqn=$2
    expected=$3 # "true" or "false"
    actual=$(ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --format json connection list --nqn $subsystem_nqn |
        jq -r --arg h "$host_nqn" '[.connections[] | select(.nqn == $h)][0].connected // false')
    if [ "$actual" != "$expected" ]; then
        ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --format json connection list --nqn $subsystem_nqn
        echo "Expected host $host_nqn to be $(connected_or_disconnected "$expected")" \
            "on $subsystem_nqn, found $(connected_or_disconnected "$actual")"
        return 1
    fi
}

present_or_absent() { [ "$1" = "true" ] && echo "present" || echo "absent"; }

assert_listener_exists() {
    subsystem_nqn=$1
    traddr=$2
    trsvcid=$3
    expected=$4 # "true" or "false"
    for gw_ip in "${gateway_ips[@]}"; do
        actual=$(ceph nvmeof --server-address $gw_ip --format json listener list --nqn $subsystem_nqn |
            jq -r --arg t "$traddr" --arg p "$trsvcid" '[.listeners[] | select(.traddr == $t and (.trsvcid | tostring) == $p)] | length > 0')
        if [ "$actual" != "$expected" ]; then
            ceph nvmeof --server-address $gw_ip --format json listener list --nqn $subsystem_nqn
            echo "Expected listener $traddr:$trsvcid to be $(present_or_absent "$expected") on $subsystem_nqn" \
                "via gateway $gw_ip, found $(present_or_absent "$actual")"
            return 1
        fi
    done
}

# runs a command expected to fail
assert_command_fails() {
    set +e
    "$@"
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
        echo "[nvmeof.add_delete] ERROR: command unexpectedly succeeded (exit code 0): $*"
        exit 1
    fi
    echo "[nvmeof.add_delete] Command expectedly failed (with exit code $rc): $*"
}

echo "[nvmeof.add_delete] Namespace add/delete testing.."

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
        ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS namespace add \
            --nqn $subsystem_nqn --rbd-pool $RBD_POOL --rbd-image-name $image \
            --load-balancing-group $(($image_index % $gateways_count + 1))
        ((image_index++))
    done
done

# list namespaces
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --format plain namespace list --nqn $subsystem_nqn        
done

# verify namespaces added
expected_count_per_subsys=$(( $NEW_NAMESPACES_COUNT + $NVMEOF_NAMESPACES_COUNT ))
assert_namespaces_count $expected_count_per_subsys

# delete namespaces
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    NSIDs=$(ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --format json namespace list --nqn $subsystem_nqn | 
            jq -r '.namespaces[] | select(.rbd_image_name | startswith("test")) | .nsid')

    for nsid in $NSIDs; do
        ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS namespace del --nqn $subsystem_nqn --nsid $nsid
    done
    assert_command_fails ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS namespace del --nqn $subsystem_nqn --nsid 99999
done

# verify namespaces deleted
expected_count_per_subsys=$NVMEOF_NAMESPACES_COUNT
assert_namespaces_count $expected_count_per_subsys

# delete rbd images
for i in $(seq 1 $new_images_count); do
    image_name="test${i}"
    rbd rm $RBD_POOL/$image_name
done

echo "[nvmeof.add_delete] Namespace add/delete testing passed!"


echo "[nvmeof.add_delete] Subsystem add/delete testing.."

# add new subsystems, each with one namespace
for i in $(seq 1 $NEW_SUBSYSTEMS_COUNT); do
    new_subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}test${i}"
    ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS subsystem add --nqn $new_subsystem_nqn --no-group-append
    ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS namespace add \
        --nqn $new_subsystem_nqn --rbd-pool $RBD_POOL --rbd-image-name "subsystest${i}" \
        --create-image --rbd-image-size 16MB
done

# verify subsystems added
assert_subsystems_count $(( $NVMEOF_SUBSYSTEMS_COUNT + $NEW_SUBSYSTEMS_COUNT ))

# delete new subsystems (force, since they have a namespace)
for i in $(seq 1 $NEW_SUBSYSTEMS_COUNT); do
    new_subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}test${i}"
    assert_command_fails ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS subsystem del --nqn $new_subsystem_nqn
    ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS subsystem del --nqn $new_subsystem_nqn --force true
done

# verify subsystems deleted
assert_subsystems_count $NVMEOF_SUBSYSTEMS_COUNT

echo "[nvmeof.add_delete] Subsystem add/delete testing passed!"


echo "[nvmeof.add_delete] Listener add/delete testing.."

NEW_LISTENER_PORT="${NEW_LISTENER_PORT:-$((NVMEOF_PORT + 100))}"
listener_test_subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}1"

# add an extra listener on a new port
for i in "${!gateway_ips[@]}"; do
    gw_ip="${gateway_ips[i]}"
    gw_name="${gateway_names[i]}"
    ceph nvmeof --server-address $gw_ip listener add --nqn $listener_test_subsystem_nqn \
        --host-name $gw_name --traddr $gw_ip --trsvcid $NEW_LISTENER_PORT
done

# verify new listeners
for gw_ip in "${gateway_ips[@]}"; do
    assert_listener_exists $listener_test_subsystem_nqn $gw_ip $NEW_LISTENER_PORT "true"
done

# delete the new listeners
for i in "${!gateway_ips[@]}"; do
    gw_ip="${gateway_ips[i]}"
    gw_name="${gateway_names[i]}"
    ceph nvmeof --server-address $gw_ip listener del --nqn $listener_test_subsystem_nqn \
        --host-name $gw_name --traddr $gw_ip --trsvcid $NEW_LISTENER_PORT
done

# verify listeners deleted
for gw_ip in "${gateway_ips[@]}"; do
    assert_listener_exists $listener_test_subsystem_nqn $gw_ip $NEW_LISTENER_PORT "false"
done

echo "[nvmeof.add_delete] Listener add/delete testing passed!"


echo "[nvmeof.add_delete] Host add/delete testing.."

# add new hosts to each existing subsystem
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    for h in $(seq 1 $NEW_HOSTS_COUNT); do
        host_nqn="nqn.2014-08.org.nvmexpress:uuid:00000000-0000-0000-0000-$(printf '%012d' $((i * 1000 + h)))"
        ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS host add --nqn $subsystem_nqn --host-nqn $host_nqn
    done
done

# verify hosts added
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    assert_hosts_count $subsystem_nqn $NEW_HOSTS_COUNT
done

# delete new hosts
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    for h in $(seq 1 $NEW_HOSTS_COUNT); do
        host_nqn="nqn.2014-08.org.nvmexpress:uuid:00000000-0000-0000-0000-$(printf '%012d' $((i * 1000 + h)))"
        ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS host del --nqn $subsystem_nqn --host-nqn $host_nqn
    done
done

# verify hosts deleted
for i in $(seq 1 $NVMEOF_SUBSYSTEMS_COUNT); do
    subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
    assert_hosts_count $subsystem_nqn 0
done

echo "[nvmeof.add_delete] Host add/delete testing passed!"


echo "[nvmeof.add_delete] Connected host deletion testing.."

LOCAL_HOST_NQN=$(cat /etc/nvme/hostnqn)
dummy_subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}dummy1"

# add dummy subsystem and add current initiator as host
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS subsystem add --nqn $dummy_subsystem_nqn --no-group-append
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS listener add \
    --nqn $dummy_subsystem_nqn --host-name $NVMEOF_DEFAULT_GATEWAY_HOSTNAME \
    --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --trsvcid $NVMEOF_PORT
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS host add --nqn $dummy_subsystem_nqn --host-nqn $LOCAL_HOST_NQN

# connect and verify 
sudo nvme connect -t tcp --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS -s $NVMEOF_PORT -n $dummy_subsystem_nqn
sleep 5
assert_host_connected $dummy_subsystem_nqn $LOCAL_HOST_NQN "true"

# delete the connected host
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS host del --nqn $dummy_subsystem_nqn --host-nqn $LOCAL_HOST_NQN

sleep 5
assert_host_connected $dummy_subsystem_nqn $LOCAL_HOST_NQN "false"

# cleanup
sudo nvme disconnect -n $dummy_subsystem_nqn || true
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS subsystem del --nqn $dummy_subsystem_nqn --force true
assert_subsystems_count $NVMEOF_SUBSYSTEMS_COUNT

echo "[nvmeof.add_delete] Connected host deletion testing passed!"


echo "[nvmeof.add_delete] Connected listener deletion testing.."

listener_dummy_subsystem_nqn="${NVMEOF_SUBSYSTEMS_PREFIX}dummy2"

# add dummy subsystem, a listener, and add current initiator as host
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS subsystem add --nqn $listener_dummy_subsystem_nqn --no-group-append
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS listener add \
    --nqn $listener_dummy_subsystem_nqn --host-name $NVMEOF_DEFAULT_GATEWAY_HOSTNAME \
    --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --trsvcid $NVMEOF_PORT
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS host add --nqn $listener_dummy_subsystem_nqn --host-nqn $LOCAL_HOST_NQN

# connect and verify
sudo nvme connect -t tcp --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS -s $NVMEOF_PORT -n $listener_dummy_subsystem_nqn
sleep 5
assert_listener_exists $listener_dummy_subsystem_nqn $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS $NVMEOF_PORT "true"

# delete without --force should fail (active connection)
assert_command_fails ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS listener del \
    --nqn $listener_dummy_subsystem_nqn --host-name $NVMEOF_DEFAULT_GATEWAY_HOSTNAME \
    --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --trsvcid $NVMEOF_PORT
assert_listener_exists $listener_dummy_subsystem_nqn $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS $NVMEOF_PORT "true"

# --force tears down the connection along with the listener
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS listener del \
    --nqn $listener_dummy_subsystem_nqn --host-name $NVMEOF_DEFAULT_GATEWAY_HOSTNAME \
    --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --trsvcid $NVMEOF_PORT --force
assert_listener_exists $listener_dummy_subsystem_nqn $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS $NVMEOF_PORT "false"

# cleanup
sudo nvme disconnect -n $listener_dummy_subsystem_nqn || true
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS subsystem del --nqn $listener_dummy_subsystem_nqn --force true
assert_subsystems_count $NVMEOF_SUBSYSTEMS_COUNT

echo "[nvmeof.add_delete] Connected listener deletion testing passed!"

echo "[nvmeof.add_delete] ALL add/delete tests PASSED!"
