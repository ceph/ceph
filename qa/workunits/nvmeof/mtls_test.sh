#!/bin/bash

set -ex
source /etc/ceph/nvmeof.env

# install yq
wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /tmp/yq && chmod +x /tmp/yq

subjectAltName=$(echo "$NVMEOF_GATEWAY_IP_ADDRESSES" | sed 's/,/,IP:/g')

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

wait_for_service() {
    MAX_RETRIES=30
    for ((RETRY_COUNT=1; RETRY_COUNT<=MAX_RETRIES; RETRY_COUNT++)); do

        if ceph orch ls --refresh | grep -q "nvmeof"; then
            echo "Found nvmeof in the output!"
            break
        fi
        if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
            echo "Reached maximum retries ($MAX_RETRIES). Exiting."
            break
        fi
        sleep 5
    done
    ceph orch ps
    ceph orch ls --refresh
}

# deploy mtls
cat /tmp/gw-conf-with-mtls.yaml 
ceph orch apply -i /tmp/gw-conf-with-mtls.yaml
ceph orch redeploy nvmeof.mypool.mygroup0 
sleep 100
wait_for_service


# test
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
IFS=',' read -ra gateway_ips <<< "$NVMEOF_GATEWAY_IP_ADDRESSES"
for i in "${!gateway_ips[@]}"
do
    ip="${gateway_ips[i]}"
    sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $ip --server-port $NVMEOF_SRPORT \
        --format json subsystem list
done

