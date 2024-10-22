#!/bin/bash -xe


GATEWAYS=$1 # exmaple "nvmeof.a,nvmeof.b"
DELAY="${SCALING_DELAYS:-50}"
POOL="${RBD_POOL:-mypool}"
GROUP="${NVMEOF_GROUP:-mygroup0}"
source /etc/ceph/nvmeof.env

if [ -z "$GATEWAYS" ]; then
    echo "At least one gateway needs to be defined for scalability test"
    exit 1
fi

status_checks() {
    expected_count=$1

    output=$(ceph nvme-gw show $POOL $GROUP) 
    nvme_show=$(echo $output | grep -o '"AVAILABLE"' | wc -l)
    if [ "$nvme_show" -ne "$expected_count" ]; then
        return 1
    fi

    orch_ls=$(ceph orch ls)
    if ! echo "$orch_ls" | grep -q "$expected_count/$expected_count"; then
        return 1
    fi

    output=$(ceph orch ps --service-name nvmeof.$POOL.$GROUP)     
    orch_ps=$(echo $output | grep -o 'running' | wc -l)
    if [ "$orch_ps" -ne "$expected_count" ]; then
        return 1
    fi

    ceph_status=$(ceph -s)
    if ! echo "$ceph_status" | grep -q "HEALTH_OK"; then
        return 1
    fi
}

total_gateways_count=$(( $(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | tr -cd ',' | wc -c) + 1 ))
scaled_down_gateways_count=$(( total_gateways_count - $(echo "$GATEWAYS" | tr -cd ',' | wc -c) - 1 ))


echo "[nvmeof.scale] Setting up config to remove gateways ${GATEWAYS}"
ceph orch ls --service-name nvmeof.$POOL.$GROUP --export > /tmp/nvmeof-gw.yaml
ceph orch ls nvmeof --export > /tmp/nvmeof-gw.yaml
cat /tmp/nvmeof-gw.yaml

pattern=$(echo $GATEWAYS | sed 's/,/\\|/g')
sed "/$pattern/d" /tmp/nvmeof-gw.yaml > /tmp/nvmeof-gw-new.yaml  
cat /tmp/nvmeof-gw-new.yaml

echo "[nvmeof.scale] Starting scale testing by removing ${GATEWAYS}"
status_checks $total_gateways_count 
ceph orch apply -i /tmp/nvmeof-gw-new.yaml # downscale
ceph orch redeploy nvmeof.$POOL.$GROUP 
sleep $DELAY
status_checks $scaled_down_gateways_count
echo "[nvmeof.scale] Downscale complete - removed gateways (${GATEWAYS}); now scaling back up"
ceph orch apply -i /tmp/nvmeof-gw.yaml #upscale
ceph orch redeploy nvmeof.$POOL.$GROUP 
sleep $DELAY
status_checks $total_gateways_count

echo "[nvmeof.scale] Scale testing passed for ${GATEWAYS}"
