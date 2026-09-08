#!/bin/bash

set -ex
source /etc/ceph/nvmeof.env

# install yq
wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /tmp/yq && chmod +x /tmp/yq

total_gateways_count=$(( $(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | tr -cd ',' | wc -c) + 1 ))

wait_for_service() {
    MAX_RETRIES=30
    for ((i=1; i<=MAX_RETRIES; i++)); do
        running=$(ceph orch ps --service-name nvmeof.mypool.mygroup0 | grep -o 'running' | wc -l)
        if [ "$running" -eq "$total_gateways_count" ]; then
            echo "[nvmeof.mtls] nvmeof service is running ($running/$total_gateways_count daemons)"
            ceph orch ps
            ceph orch ls
            return 0
        fi
        echo "[nvmeof.mtls] Waiting for nvmeof service ($running/$total_gateways_count running, attempt $i/$MAX_RETRIES)..."
        sleep 10
    done
    echo "[nvmeof.mtls] Timed out waiting for nvmeof service"
    ceph orch ps
    ceph orch ls
    exit 1
}

# CASE 1: certs in spec file (server/client + root CA)
echo "[nvmeof.mtls] Starting test with certs in spec file (server/client + root CA)"

# create mtls spec files
ceph orch ls nvmeof --export > /tmp/gw-conf-original.yaml
sudo /tmp/yq ".spec.enable_auth=true | \
    .spec.root_ca_cert=\"mountcert\" | \
    .spec.client_cert = load_str(\"/etc/ceph/client.crt\") | \
    .spec.client_key = load_str(\"/etc/ceph/client.key\") | \
    .spec.server_cert = load_str(\"/etc/ceph/server.crt\") | \
    .spec.server_key = load_str(\"/etc/ceph/server.key\")" /tmp/gw-conf-original.yaml > /tmp/gw-conf-with-mtls.yaml
cp /tmp/gw-conf-original.yaml /tmp/gw-conf-without-mtls.yaml 
sudo /tmp/yq '.spec.enable_auth=false' -i /tmp/gw-conf-without-mtls.yaml

# deploy mtls
cat /tmp/gw-conf-with-mtls.yaml 
ceph orch apply -i /tmp/gw-conf-with-mtls.yaml
ceph orch redeploy nvmeof.mypool.mygroup0 
sleep 100
wait_for_service

# check certs/keys
ceph orch certmgr cert ls --include-cephadm-signed
ceph orch certmgr key ls --include-cephadm-generated-keys

# test
echo "[nvmeof.mtls] testing with mtls"
IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
for i in "${!gateway_ips[@]}"
do
    ip="${gateway_ips[i]}"
    sudo podman run -v /etc/ceph/server.crt:/server.crt:z -v /etc/ceph/client.crt:/client.crt:z \
        -v /etc/ceph/client.key:/client.key:z  \
        -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT \
        --client-key /client.key --client-cert /client.crt --server-cert /server.crt --format json subsystem list
done


# remove mtls
cat /tmp/gw-conf-without-mtls.yaml 
ceph orch apply -i /tmp/gw-conf-without-mtls.yaml
ceph orch redeploy nvmeof.mypool.mygroup0 
sleep 100
wait_for_service

# test
echo "[nvmeof.mtls] testing after removing mtls"
IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
for i in "${!gateway_ips[@]}"
do
    ip="${gateway_ips[i]}"
    ceph nvmeof subsystem list --server-address $ip
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT \
        --format json subsystem list
done

echo "[nvmeof.mtls] TEST PASSED with certs in spec file (server/client + root CA)"


# CASE 2: cephadm-signed cert (enable_auth=true, no certs)
echo "[nvmeof.mtls] Starting test with cephadm-signed cert (ssl=true + enable_auth=true, no certs)"

# deploy mtls with cephadm-signed certs
sudo /tmp/yq '.spec.enable_auth=true' /tmp/gw-conf-original.yaml > /tmp/gw-conf-cephadm-certs.yaml
cat /tmp/gw-conf-cephadm-certs.yaml
ceph orch apply -i /tmp/gw-conf-cephadm-certs.yaml
ceph orch redeploy nvmeof.mypool.mygroup0
sleep 100
wait_for_service

# retrieve cephadm-generated client certs
ceph orch certmgr cert ls --include-cephadm-signed
ceph orch certmgr key ls --include-cephadm-generated-keys

SERVICE_NAME=$(ceph orch ps --daemon-type nvmeof --format json | jq -r '.[0].service_name')
NVMEOF_SERVER_CERT_NAME="cephadm-signed_${SERVICE_NAME}_cert"
NVMEOF_CLIENT_CERT_NAME="cephadm-signed_${SERVICE_NAME}__lbl__client_cert"
NVMEOF_CLIENT_KEY_NAME="cephadm-signed_${SERVICE_NAME}__lbl__client_key"
echo "Found nvmeof cert names: server_cert=$NVMEOF_SERVER_CERT_NAME, client_cert=$NVMEOF_CLIENT_CERT_NAME, client_key=$NVMEOF_CLIENT_KEY_NAME"

# test
echo "[nvmeof.mtls] testing with cephadm-signed mtls"
IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
for i in "${!gateway_ips[@]}"
do
    ip="${gateway_ips[i]}"
    HOST=$(ceph orch host ls --format json | jq -r --arg ip "$ip" '.[] | select(.addr == $ip) | .hostname')

    ceph orch certmgr cert get $NVMEOF_SERVER_CERT_NAME --hostname $HOST > /tmp/cephadm_server.crt
    ceph orch certmgr cert get $NVMEOF_CLIENT_CERT_NAME --hostname $HOST > /tmp/cephadm_client.crt
    ceph orch certmgr key get $NVMEOF_CLIENT_KEY_NAME --service-name $SERVICE_NAME --hostname $HOST > /tmp/cephadm_client.key

    ceph nvmeof subsystem list --server-address $ip
    sudo podman run -v /tmp/cephadm_server.crt:/server.crt:z -v /tmp/cephadm_client.crt:/client.crt:z \
        -v /tmp/cephadm_client.key:/client.key:z  \
        -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT \
        --client-key /client.key --client-cert /client.crt --server-cert /server.crt --format json subsystem list

    set +e
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT --format json subsystem list
    rc=$?
    set -e
    if [ "$rc" -eq 0 ]; then
        echo "[nvmeof.mtls] ERROR: container CLI unexpectedly succeeded (exit code 0) without certs"
        exit 1
    fi
    echo "[nvmeof.mtls] container CLI correctly failed (exit code $rc) without certs, as expected"
done

# remove mtls
ceph orch apply -i /tmp/gw-conf-without-mtls.yaml
ceph orch redeploy nvmeof.mypool.mygroup0
sleep 100
wait_for_service

# test
echo "[nvmeof.mtls] testing after removing cephadm-signed mtls"
IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
for i in "${!gateway_ips[@]}"
do
    ip="${gateway_ips[i]}"
    ceph nvmeof subsystem list --server-address $ip
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT \
        --format json subsystem list
done

echo "[nvmeof.mtls] TEST PASSED with cephadm-signed cert (enable_auth=true, no certs)"
