#!/bin/bash -x

source /etc/ceph/nvmeof.env
SPDK_CONTROLLER="SPDK bdev Controller"
DISCOVERY_PORT="8009"

discovery() {
    output=$(sudo nvme discover -t tcp -a $NVMEOF_GATEWAY_IP_ADDRESS -s $DISCOVERY_PORT)
    expected_discovery_stdout="subtype: nvme subsystem"
    if ! echo "$output" | grep -q "$expected_discovery_stdout"; then
        return 1
    fi
}

connect() {
    sudo nvme connect -t tcp --traddr $NVMEOF_GATEWAY_IP_ADDRESS -s $NVMEOF_PORT -n $NVMEOF_NQN
    output=$(sudo nvme list)
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
    sudo nvme connect-all --traddr=$NVMEOF_GATEWAY_IP_ADDRESS --transport=tcp
    output=$(sudo nvme list)
    if ! echo "$output" | grep -q "$SPDK_CONTROLLER"; then
        return 1
    fi
}

list_subsys() {
    expected_count=$1
    output=$(sudo nvme list-subsys --output-format=json)
    multipath=$(echo $output | grep -c '"tcp"')
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
        exit 1
    fi
}


test_run disconnect_all
test_run discovery 
test_run connect
test_run list_subsys 1
test_run disconnect_all
test_run list_subsys 0
test_run connect_all
test_run list_subsys 1


echo "-------------Test Summary-------------"
echo "[nvmeof] All nvmeof basic tests passed!"
