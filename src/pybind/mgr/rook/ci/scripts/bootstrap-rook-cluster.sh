#!/usr/bin/env bash

set -eEx

: ${CEPH_DEV_FOLDER:=${PWD}}
KUBECTL="minikube kubectl --"

# We build a local ceph image that contains the latest code
# plus changes from the PR. This image will be used by the docker
# running inside the minikube to start the different ceph pods
LOCAL_CEPH_IMG="local/ceph"

on_error() {
    echo "on error"
    minikube delete
}

setup_minikube_env() {

    # Check if Minikube is running
    if minikube status > /dev/null 2>&1; then
	echo "Minikube is running"
	minikube stop
	minikube delete
    else
	echo "Minikube is not running"
    fi

    rm -rf ~/.minikube
    minikube start --memory="4096" --cpus="2" --disk-size=10g --extra-disks=1 --driver kvm2
    # point Docker env to use docker daemon running on minikube
    eval $(minikube docker-env -p minikube)
}

build_ceph_image() {
    wget -q -O cluster-test.yaml https://raw.githubusercontent.com/rook/rook/master/deploy/examples/cluster-test.yaml
    CURR_CEPH_IMG=$(grep -E '^\s*image:\s+' cluster-test.yaml | sed 's/.*image: *\([^ ]*\)/\1/')

    cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci
    mkdir -p tmp_build/rook
    mkdir -p tmp_build/orchestrator
    cp ./../../orchestrator/*.py tmp_build/orchestrator
    cp ../*.py tmp_build/rook

    # we use the following tag to trick the Docker
    # running inside minikube so it uses this image instead
    # of pulling it from the registry
    docker build --tag ${LOCAL_CEPH_IMG} .
    docker tag ${LOCAL_CEPH_IMG} ${CURR_CEPH_IMG}

    # cleanup
    rm -rf tmp_build
    cd ${CEPH_DEV_FOLDER}
}

create_rook_cluster() {
    wget -q -O cluster-test.yaml https://raw.githubusercontent.com/rook/rook/master/deploy/examples/cluster-test.yaml
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/crds.yaml
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/common.yaml
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/operator.yaml
    $KUBECTL create -f cluster-test.yaml
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/dashboard-external-http.yaml
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/toolbox.yaml
}

wait_for_rook_operator() {
    local max_attempts=10
    local sleep_interval=20
    local attempts=0
    $KUBECTL rollout status deployment rook-ceph-operator -n rook-ceph --timeout=180s
    PHASE=$($KUBECTL get cephclusters.ceph.rook.io -n rook-ceph -o jsonpath='{.items[?(@.kind == "CephCluster")].status.phase}')
    echo "PHASE: $PHASE"
    while ! $KUBECTL get cephclusters.ceph.rook.io -n rook-ceph -o jsonpath='{.items[?(@.kind == "CephCluster")].status.phase}' | grep -q "Ready"; do
	echo "Waiting for cluster to be ready..."
	sleep $sleep_interval
	attempts=$((attempts+1))
        if [ $attempts -ge $max_attempts ]; then
            echo "Maximum number of attempts ($max_attempts) reached. Exiting..."
            return 1
        fi
    done
}

wait_for_ceph_cluster() {
    local max_attempts=10
    local sleep_interval=20
    local attempts=0
    $KUBECTL rollout status deployment rook-ceph-tools -n rook-ceph --timeout=90s
    while ! $KUBECTL get cephclusters.ceph.rook.io -n rook-ceph -o jsonpath='{.items[?(@.kind == "CephCluster")].status.ceph.health}' | grep -q "HEALTH_OK"; do
	echo "Waiting for Ceph cluster installed"
	sleep $sleep_interval
	attempts=$((attempts+1))
        if [ $attempts -ge $max_attempts ]; then
            echo "Maximum number of attempts ($max_attempts) reached. Exiting..."
            return 1
        fi
    done
    echo "Ceph cluster installed and running"
}

show_info() {
    DASHBOARD_PASSWORD=$($KUBECTL -n rook-ceph get secret rook-ceph-dashboard-password -o jsonpath="{['data']['password']}" | base64 --decode && echo)
    IP_ADDR=$($KUBECTL get po --selector="app=rook-ceph-mgr" -n rook-ceph --output jsonpath='{.items[*].status.hostIP}')
    PORT="$($KUBECTL -n rook-ceph -o=jsonpath='{.spec.ports[?(@.name == "dashboard")].nodePort}' get services rook-ceph-mgr-dashboard-external-http)"
    BASE_URL="http://$IP_ADDR:$PORT"
    echo "==========================="
    echo "Ceph Dashboard:  "
    echo "   IP_ADDRESS: $BASE_URL"
    echo "   PASSWORD: $DASHBOARD_PASSWORD"
    echo "==========================="
}

configure_libvirt(){
    if sudo usermod -aG libvirt $(id -un); then
	echo "User added to libvirt group successfully."
	sudo systemctl enable --now libvirtd
	sudo systemctl restart libvirtd
	sleep 10 # wait some time for libvirtd service to restart
	newgrp libvirt
    else
	echo "Error adding user to libvirt group."
	return 1
    fi
}

recreate_default_network(){

    # destroy any existing kvm default network
    if sudo virsh net-destroy default; then
	sudo virsh net-undefine default
    fi

    # let's create a new kvm default network
    sudo virsh net-define /usr/share/libvirt/networks/default.xml
    if sudo virsh net-start default; then
        echo "Network 'default' started successfully."
    else
        # Optionally, handle the error
        echo "Failed to start network 'default', but continuing..."
    fi

    # restart libvirtd service and wait a little bit for the service
    sudo systemctl restart libvirtd
    sleep 20

    # Just some debugging information
    all_networks=$(virsh net-list --all)
    groups=$(groups)
}

####################################################################
####################################################################

trap 'on_error $? $LINENO' ERR

configure_libvirt
recreate_default_network
setup_minikube_env
build_ceph_image
create_rook_cluster
wait_for_rook_operator
wait_for_ceph_cluster
show_info

####################################################################
####################################################################
