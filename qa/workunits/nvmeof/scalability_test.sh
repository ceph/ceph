#!/bin/bash -xe


GATEWAYS=$1 # exmaple "nvmeof.a,nvmeof.b"
DELAY="${SCALING_DELAYS:-50}"

if [ -z "$GATEWAYS" ]; then
    echo "At least one gateway needs to be defined for scalability test"
    exit 1
fi

pip3 install yq

status_checks() {
    ceph nvme-gw show mypool ''
    ceph orch ls
    ceph orch ps 
    ceph -s
}


echo "[nvmeof.scale] Setting up config to remove gateways ${GATEWAYS}"
ceph orch ls nvmeof --export > /tmp/nvmeof-gw.yaml
cat /tmp/nvmeof-gw.yaml
yq "del(.placement.hosts[] | select(. | test(\".*($(echo $GATEWAYS | sed 's/,/|/g'))\")))" /tmp/nvmeof-gw.yaml > /tmp/nvmeof-gw-new.yaml
cat /tmp/nvmeof-gw-new.yaml

echo "[nvmeof.scale] Starting scale testing by removing ${GATEWAYS}"
status_checks
ceph orch rm nvmeof.mypool && sleep 20 # temp workaround
ceph orch apply -i /tmp/nvmeof-gw-new.yaml # downscale
sleep $DELAY
status_checks
ceph orch rm nvmeof.mypool && sleep 20 # temp workaround
ceph orch apply -i /tmp/nvmeof-gw.yaml #upscale
sleep $DELAY
status_checks

echo "[nvmeof.scale] Scale testing passed for ${GATEWAYS}"
