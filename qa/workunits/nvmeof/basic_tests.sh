#!/bin/bash -x

# https://tracker.ceph.com/issues/74922
sudo systemctl stop udisks2 2>/dev/null || true

# install nvme 2.13 (issue with latest nvme version 2.16 with centos9: https://tracker.ceph.com/issues/74615#note-5)
sudo dnf install nvme-cli-2.13 libnvme-1.13 -y
sleep 10 
sudo modprobe nvme-fabrics
sudo modprobe nvme-tcp
nvme version
sleep 20
sudo lsmod | grep nvme


source /etc/ceph/nvmeof.env
SPDK_CONTROLLER="Ceph bdev Controller"
DISCOVERY_PORT="8009"

discovery() {
    output=$(sudo nvme discover -t tcp -a $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS -s $DISCOVERY_PORT)
    sleep 5
    expected_discovery_stdout="subtype: nvme subsystem"
    if ! echo "$output" | grep -q "$expected_discovery_stdout"; then
        return 1
    fi
}

connect() {
    sudo nvme connect -t tcp --traddr $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS -s $NVMEOF_PORT -n "${NVMEOF_SUBSYSTEMS_PREFIX}1"
    sleep 5
    output=$(sudo nvme list --output-format=json)
    if ! echo "$output" | grep -q "$SPDK_CONTROLLER"; then
        return 1
    fi
}

disconnect_all() {
    sudo nvme disconnect-all
    output=$(sudo nvme list)
    if echo "$output" | grep -q "$SPDK_CONTROLLER"; then
        return 1
    fi
}

connect_all() {
    sudo nvme connect-all --traddr=$NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --transport=tcp -l 3600
    sleep 5
    expected_devices_count=$1
    actual_devices=$(sudo nvme list --output-format=json | grep -o "$SPDK_CONTROLLER" | wc -l)
    if [ "$actual_devices" -ne "$expected_devices_count" ]; then
        sudo nvme list --output-format=json
        return 1
    fi
}

list_subsys() {
    expected_count=$1
    output=$(sudo nvme list-subsys --output-format=json)
    multipath=$(echo $output | grep -o '"tcp"' | wc -l)
    if [ "$multipath" -ne "$expected_count" ]; then
        return 1
    fi
}


test_run() {
    echo "[nvmeof] Running test: $1"
    $1 "${@:2}" # execute func
    if [ $? -eq 0 ]; then
        echo "[nvmeof] $1 test passed!"
    else
        echo "[nvmeof] $1 test failed!"
        sudo nvme list-subsys
        sudo nvme list
        sudo dmesg -T > $TESTDIR/archive/dmesg-basic_tests.log
        exit 1
    fi
}


test_run disconnect_all
test_run discovery 
test_run connect
test_run list_subsys 1
test_run disconnect_all
test_run list_subsys 0
devices_count=$(( $NVMEOF_NAMESPACES_COUNT * $NVMEOF_SUBSYSTEMS_COUNT )) 
test_run connect_all $devices_count
gateways_count=$(( $(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | tr -cd ',' | wc -c) + 1 ))
multipath_count=$(( $gateways_count * $NVMEOF_SUBSYSTEMS_COUNT)) 
test_run list_subsys $multipath_count



echo "-------------Test Summary-------------"
echo "[nvmeof] All nvmeof basic tests passed!"
