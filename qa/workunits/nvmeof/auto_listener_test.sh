#!/bin/bash -xe

unset CEPH_CLI_TEST_DUP_COMMAND

source /etc/ceph/nvmeof.env

IFS=',' read -ra GATEWAY_IPS <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
gateways_count=$(( $(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | tr -cd ',' | wc -c) + 1 ))

SUBNET_MASK="${NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS%.*}.0/24"
WIDER_SUBNET_MASK="${NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS%.*.*}.0.0/16"

NETWORK_SUBSYSTEM="nqn.2016-06.io.spdk:cnode-netmask1"
GROUP_MASK_SUBSYSTEM="nqn.2016-06.io.spdk:cnode-spec-netmask1"


retry() {
    local attempt rc=0
    for ((attempt=1; attempt<=12; attempt++)); do
        "$@" && return 0
        rc=$?
        if [ "$attempt" -lt 12 ]; then
            echo "[nvmeof.auto_listener] '$*' failed (rc=$rc, attempt $attempt/12)," \
                 "retrying in 5s..."
            sleep 5
        fi
    done
    echo "[nvmeof.auto_listener] '$*' still failing after 12 attempts (rc=$rc)"
    return $rc
}

wait_for_service() {
    local i running
    for ((i=1; i<=30; i++)); do
        running=$(ceph orch ps --service-name nvmeof.mypool.mygroup0 | grep -o 'running' | wc -l)
        if [ "$running" -eq "$gateways_count" ]; then
            echo "[nvmeof.auto_listener] nvmeof service is running ($running/$gateways_count daemons)"
            ceph orch ps
            ceph orch ls
            return 0
        fi
        echo "[nvmeof.auto_listener] Waiting for nvmeof service ($running/$gateways_count" \
             "running, attempt $i/30)..."
        sleep 10
    done
    echo "[nvmeof.auto_listener] Timed out waiting for nvmeof service"
    ceph orch ps
    ceph orch ls
    exit 1
}

assert_fails() { # assert that $@ command fails (non-zero exit)
    local rc=0
    set +e
    "$@"
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
        echo "[nvmeof.auto_listener] ERROR: expected '$*' to fail, but it succeeded"
        return 1
    fi
}

assert_listener_on_every_gateway() {
    local nqn=$1 listeners actual_count ip info count
    listeners=$(ceph nvmeof --format json listener list --nqn "$nqn")
    actual_count=$(echo "$listeners" | jq '.listeners | length')
    if [ "$actual_count" -ne "$gateways_count" ]; then
        echo "[nvmeof.auto_listener] ERROR: expected $gateways_count listener(s) for $nqn, found $actual_count"
        echo "$listeners" | jq .
        return 1
    fi
    for ip in "${GATEWAY_IPS[@]}"; do
        if ! echo "$listeners" | jq -e --arg ip "$ip" \
                '[.listeners[] | select(.traddr == $ip)] | length > 0'; then
            echo "[nvmeof.auto_listener] ERROR: no listener found for gateway $ip on $nqn"
            echo "$listeners" | jq .
            return 1
        fi
    done
    # check gateway's actual SPDK listener state
    for ip in "${GATEWAY_IPS[@]}"; do
        info=$(ceph nvmeof --server-address "$ip" --format json gateway listener_info --nqn "$nqn")
        count=$(echo "$info" | jq --arg ip "$ip" \
                '[.gw_listeners[] | select(.listener.traddr == $ip)] | length')
        if [ "$count" -eq 0 ]; then
            echo "[nvmeof.auto_listener] ERROR: gateway $ip reports no listener for $nqn"
            echo "$info" | jq .
            return 1
        fi
    done
}

assert_no_listeners() {
    local nqn=$1 actual_count
    actual_count=$(ceph nvmeof --format json listener list --nqn "$nqn" | jq '.listeners | length')
    if [ "$actual_count" -ne 0 ]; then
        echo "[nvmeof.auto_listener] ERROR: expected no listeners for $nqn, found $actual_count"
        return 1
    fi
}

assert_listener_exists() {
    local nqn=$1 traddr=$2 trsvcid=$3 expected=$4 actual # expected: "true" or "false"
    actual=$(ceph nvmeof --server-address "$NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" --format json \
            listener list --nqn "$nqn" |
        jq -r --arg t "$traddr" --arg p "$trsvcid" \
            '[.listeners[] | select(.traddr == $t and (.trsvcid | tostring) == $p)] | length > 0')
    if [ "$actual" != "$expected" ]; then
        echo "[nvmeof.auto_listener] ERROR: listener $traddr:$trsvcid on $nqn:" \
             "expected exist=$expected, actual exist=$actual"
        return 1
    fi
}

assert_subsystem() {
    local nqn=$1 mask=$2 subsys filter
    if [ -n "$mask" ]; then
        filter='.subsystems[0].network_mask | index($mask) != null'
    else
        filter='.subsystems[0].network_mask | length == 0'
    fi
    subsys=$(ceph nvmeof --format json subsystem list --nqn "$nqn")
    if ! echo "$subsys" | jq --arg mask "$mask" -e "$filter"; then
        echo "[nvmeof.auto_listener] ERROR: unexpected network_mask for $nqn" \
             "${mask:+(expected $mask present)}"
        echo "$subsys" | jq .
        return 1
    fi
}

# CASE 1: add_network / del_network
echo "[nvmeof.auto_listener] CASE 1: Starting test with add_network / del_network"

ceph nvmeof subsystem add --nqn $NETWORK_SUBSYSTEM --no-group-append

echo "[nvmeof.auto_listener] 1: testing add_network with a single /24 covering every gateway"
ceph nvmeof subsystem add_network --nqn $NETWORK_SUBSYSTEM --network_mask $SUBNET_MASK
retry assert_listener_on_every_gateway $NETWORK_SUBSYSTEM
assert_subsystem $NETWORK_SUBSYSTEM $SUBNET_MASK

echo "[nvmeof.auto_listener] 2. testing refresh_network is a no-op on every gateway when " \
     "the listener is already correctly in sync with the configured mask"
for ip in "${GATEWAY_IPS[@]}"; do
    ceph nvmeof --server-address "$ip" gateway refresh_network --nqn $NETWORK_SUBSYSTEM
done
retry assert_listener_on_every_gateway $NETWORK_SUBSYSTEM
assert_subsystem $NETWORK_SUBSYSTEM $SUBNET_MASK

echo "[nvmeof.auto_listener] 3. negative: use_group_config_network_masks must fail while " \
     "network_mask is set, and listener add/del must fail while an auto-listener exists"
assert_fails ceph nvmeof subsystem set_use_group_config_network_masks --nqn $NETWORK_SUBSYSTEM --use-group-config-network-masks=true
assert_fails ceph nvmeof listener add --nqn $NETWORK_SUBSYSTEM --host-name $NVMEOF_DEFAULT_GATEWAY_HOSTNAME --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS
assert_fails ceph nvmeof listener del --nqn $NETWORK_SUBSYSTEM --host-name $NVMEOF_DEFAULT_GATEWAY_HOSTNAME --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --trsvcid 4420
assert_listener_on_every_gateway $NETWORK_SUBSYSTEM
assert_subsystem $NETWORK_SUBSYSTEM $SUBNET_MASK

echo "[nvmeof.auto_listener] 4. testing del_network"
ceph nvmeof subsystem del_network --nqn $NETWORK_SUBSYSTEM --network_mask $SUBNET_MASK
retry assert_no_listeners $NETWORK_SUBSYSTEM
assert_subsystem $NETWORK_SUBSYSTEM

echo "[nvmeof.auto_listener] 5. testing add_network with two overlapping subnets "
ceph nvmeof subsystem add_network --nqn $NETWORK_SUBSYSTEM --network_mask $SUBNET_MASK
ceph nvmeof subsystem add_network --nqn $NETWORK_SUBSYSTEM --network_mask $WIDER_SUBNET_MASK
retry assert_listener_on_every_gateway $NETWORK_SUBSYSTEM
assert_subsystem $NETWORK_SUBSYSTEM $SUBNET_MASK
assert_subsystem $NETWORK_SUBSYSTEM $WIDER_SUBNET_MASK

echo "[nvmeof.auto_listener] 6. testing del_network for the narrower subnet only - the " \
     "listener should stay, it's still covered by the wider subnet"
ceph nvmeof subsystem del_network --nqn $NETWORK_SUBSYSTEM --network_mask $SUBNET_MASK
retry assert_listener_on_every_gateway $NETWORK_SUBSYSTEM
assert_subsystem $NETWORK_SUBSYSTEM $WIDER_SUBNET_MASK

echo "[nvmeof.auto_listener] 7. testing del_network for the wider subnet too - now the " \
     "listener should be gone"
ceph nvmeof subsystem del_network --nqn $NETWORK_SUBSYSTEM --network_mask $WIDER_SUBNET_MASK
retry assert_no_listeners $NETWORK_SUBSYSTEM
assert_subsystem $NETWORK_SUBSYSTEM

ceph nvmeof subsystem del --nqn $NETWORK_SUBSYSTEM

echo "[nvmeof.auto_listener] CASE 1: TEST PASSED with add_network / del_network"

# CASE 2: set_use_group_config_network_masks
echo "[nvmeof.auto_listener] CASE 2: Starting test with set_use_group_config_network_masks"

echo "[nvmeof.auto_listener] 1. enabling before listener_network_masks is configured on " \
     "the spec - should succeed with a warning and create no listener yet"
ceph nvmeof subsystem add --nqn $GROUP_MASK_SUBSYSTEM --no-group-append
ceph nvmeof subsystem set_use_group_config_network_masks --nqn $GROUP_MASK_SUBSYSTEM --use-group-config-network-masks=true
assert_no_listeners $GROUP_MASK_SUBSYSTEM

ceph orch ls nvmeof --export > /tmp/gw-conf-original.yaml
sed "/^  group:/a\\  listener_network_masks: [\"$SUBNET_MASK\"]" \
    /tmp/gw-conf-original.yaml > /tmp/gw-conf-with-listener-masks.yaml
cat /tmp/gw-conf-with-listener-masks.yaml
ceph orch apply -i /tmp/gw-conf-with-listener-masks.yaml
ceph orch redeploy nvmeof.mypool.mygroup0
sleep 100
wait_for_service

echo "[nvmeof.auto_listener] 2. listener_network_masks was just added to the spec - the " \
     "subsystem enabled earlier should now automatically have listeners"
retry assert_listener_on_every_gateway $GROUP_MASK_SUBSYSTEM
assert_subsystem $GROUP_MASK_SUBSYSTEM $SUBNET_MASK
ceph nvmeof --format json subsystem list --nqn $GROUP_MASK_SUBSYSTEM |
    jq -e '.subsystems[0].use_group_config_network_masks == true'

echo "[nvmeof.auto_listener] 3. enabling again while already enabled should succeed with a warning"
ceph nvmeof subsystem set_use_group_config_network_masks --nqn $GROUP_MASK_SUBSYSTEM --use-group-config-network-masks=true
assert_listener_on_every_gateway $GROUP_MASK_SUBSYSTEM

echo "[nvmeof.auto_listener] 4. negative: add_network must fail while use_group_config_network_masks " \
     "is set, and listener add/del must fail while an auto-listener exists"
assert_fails ceph nvmeof subsystem add_network --nqn $GROUP_MASK_SUBSYSTEM --network_mask $SUBNET_MASK
assert_fails ceph nvmeof listener add --nqn $GROUP_MASK_SUBSYSTEM --host-name $NVMEOF_DEFAULT_GATEWAY_HOSTNAME --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS
assert_fails ceph nvmeof listener del --nqn $GROUP_MASK_SUBSYSTEM --host-name $NVMEOF_DEFAULT_GATEWAY_HOSTNAME --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --trsvcid 4420
assert_listener_on_every_gateway $GROUP_MASK_SUBSYSTEM
ceph nvmeof --format json subsystem list --nqn $GROUP_MASK_SUBSYSTEM |
    jq -e '.subsystems[0].use_group_config_network_masks == true'

echo "[nvmeof.auto_listener] 5. testing disable group config network masks"
ceph nvmeof subsystem set_use_group_config_network_masks --nqn $GROUP_MASK_SUBSYSTEM --use-group-config-network-masks=false
retry assert_no_listeners $GROUP_MASK_SUBSYSTEM
assert_subsystem $GROUP_MASK_SUBSYSTEM
ceph nvmeof --format json subsystem list --nqn $GROUP_MASK_SUBSYSTEM |
    jq -e '.subsystems[0].use_group_config_network_masks == false'

echo "[nvmeof.auto_listener] 6. testing enable group config network masks (on a subsystem where its disabled)"
ceph nvmeof subsystem set_use_group_config_network_masks --nqn $GROUP_MASK_SUBSYSTEM --use-group-config-network-masks=true
retry assert_listener_on_every_gateway $GROUP_MASK_SUBSYSTEM
ceph nvmeof --format json subsystem list --nqn $GROUP_MASK_SUBSYSTEM |
    jq -e '.subsystems[0].use_group_config_network_masks == true'

echo "[nvmeof.auto_listener] 7. testing refresh_network is a no-op on every gateway when " \
     "the listener is already correctly in sync with the group config mask"
for ip in "${GATEWAY_IPS[@]}"; do
    ceph nvmeof --server-address "$ip" gateway refresh_network --nqn $GROUP_MASK_SUBSYSTEM
done
retry assert_listener_on_every_gateway $GROUP_MASK_SUBSYSTEM
assert_subsystem $GROUP_MASK_SUBSYSTEM $SUBNET_MASK

echo "[nvmeof.auto_listener] 8. removing the subnet from the spec and redeploying - the listeners should automatically be removed"
ceph orch apply -i /tmp/gw-conf-original.yaml
ceph orch redeploy nvmeof.mypool.mygroup0
sleep 100
wait_for_service
retry assert_no_listeners $GROUP_MASK_SUBSYSTEM

ceph nvmeof subsystem del --nqn $GROUP_MASK_SUBSYSTEM

echo "[nvmeof.auto_listener] CASE 2: TEST PASSED with set_use_group_config_network_masks"

# CASE 3: refresh_network reacting to this gateway's own NIC going down/up
echo "[nvmeof.auto_listener] CASE 3: Starting test with refresh_network reacting to NIC changes"

DUMMY_NIC="nvmeof-dummy0"
DUMMY_IP="10.99.63.4"
DUMMY_SUBNET="10.99.63.0/24"
REFRESH_NIC_SUBSYSTEM="nqn.2016-06.io.spdk:cnode-refresh-nic1"

cleanup_dummy_nic() {
    sudo ip link del $DUMMY_NIC
}
trap cleanup_dummy_nic EXIT

echo "[nvmeof.auto_listener] 1. bringing up a dummy NIC at $DUMMY_IP and creating a " \
     "subsystem with an auto-listener on it"
sudo ip link add $DUMMY_NIC type dummy
sudo ip addr add $DUMMY_IP/24 dev $DUMMY_NIC
sudo ip link set $DUMMY_NIC up
ceph nvmeof subsystem add --nqn $REFRESH_NIC_SUBSYSTEM --network-mask $DUMMY_SUBNET --no-group-append
retry assert_listener_exists $REFRESH_NIC_SUBSYSTEM $DUMMY_IP $NVMEOF_PORT "true"
assert_subsystem $REFRESH_NIC_SUBSYSTEM $DUMMY_SUBNET

echo "[nvmeof.auto_listener] 2. downscale: bringing the NIC down and refreshing - the " \
     "listener should automatically be removed"
sudo ip link set $DUMMY_NIC down
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS gateway refresh_network --nqn $REFRESH_NIC_SUBSYSTEM
retry assert_listener_exists $REFRESH_NIC_SUBSYSTEM $DUMMY_IP $NVMEOF_PORT "false"
assert_subsystem $REFRESH_NIC_SUBSYSTEM $DUMMY_SUBNET

echo "[nvmeof.auto_listener] 3. upscale: bringing the NIC back up and refreshing - the " \
     "listener should automatically be recreated"
sudo ip link set $DUMMY_NIC up
ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS gateway refresh_network --nqn $REFRESH_NIC_SUBSYSTEM
retry assert_listener_exists $REFRESH_NIC_SUBSYSTEM $DUMMY_IP $NVMEOF_PORT "true"
assert_subsystem $REFRESH_NIC_SUBSYSTEM $DUMMY_SUBNET

ceph nvmeof subsystem del --nqn $REFRESH_NIC_SUBSYSTEM
sudo ip link del $DUMMY_NIC
trap - EXIT

echo "[nvmeof.auto_listener] CASE 3: TEST PASSED with refresh_network reacting to NIC changes"
