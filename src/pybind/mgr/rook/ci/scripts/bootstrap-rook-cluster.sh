#!/usr/bin/env bash

set -eEx

: ${CEPH_DEV_FOLDER:=${PWD}}
CLUSTER_SPEC=${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci/cluster-specs/cluster-on-pvc-minikube.yaml
DEFAULT_NS="rook-ceph"
KUBECTL="minikube kubectl --"
export ROOK_CLUSTER_NS="${ROOK_CLUSTER_NS:=$DEFAULT_NS}" ## CephCluster namespace

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
    minikube start --memory="6144" --disk-size=20g --extra-disks=4 --driver kvm2
    # point Docker env to use docker daemon running on minikube
    eval $(minikube docker-env -p minikube)
}

build_ceph_image() {

    CURR_CEPH_IMG=$(grep -E '^\s*image:\s+' $CLUSTER_SPEC | sed 's/.*image: *\([^ ]*\)/\1/')

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
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/crds.yaml
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/common.yaml
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/operator.yaml
    $KUBECTL create -f $CLUSTER_SPEC
    $KUBECTL create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/toolbox.yaml
}

is_operator_ready() {
    local phase
    phase=$($KUBECTL get cephclusters.ceph.rook.io -n rook-ceph -o jsonpath='{.items[?(@.kind == "CephCluster")].status.phase}')
    echo "PHASE: $phase"
    [[ "$phase" == "Ready" ]]
}

wait_for_rook_operator() {
    local max_attempts=10
    local sleep_interval=20
    local attempts=0

    $KUBECTL rollout status deployment rook-ceph-operator -n rook-ceph --timeout=180s

    while ! is_operator_ready; do
        echo "Waiting for rook operator to be ready..."
        sleep $sleep_interval

	# log current cluster state and pods info for debugging
        PHASE=$($KUBECTL get cephclusters.ceph.rook.io -n rook-ceph -o jsonpath='{.items[?(@.kind == "CephCluster")].status.phase}')
        $KUBECTL -n rook-ceph get pods

        attempts=$((attempts + 1))
        if [ $attempts -ge $max_attempts ]; then
            echo "Maximum number of attempts ($max_attempts) reached. Exiting..."
            $KUBECTL -n rook-ceph get pods | grep operator | awk '{print $1}' | xargs $KUBECTL -n rook-ceph logs
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
	echo "Waiting for Ceph cluster to enter HEALTH_OK" state
	sleep $sleep_interval
	attempts=$((attempts+1))
        if [ $attempts -ge $max_attempts ]; then
            echo "Maximum number of attempts ($max_attempts) reached. Exiting..."
            return 1
        fi
    done
    echo "Ceph cluster installed and running"

    # add an additional wait to cover with any subttle change in the state
    sleep 20
}

configure_libvirt(){
    if sudo usermod -aG libvirt $(id -un); then
	echo "User added to libvirt group successfully."
	sudo systemctl enable --now libvirtd
	sudo systemctl restart libvirtd
	sleep 30 # wait some time for libvirtd service to restart
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
    sleep 30

    # Just some debugging information
    all_networks=$(virsh net-list --all)
    groups=$(groups)
}

enable_rook_orchestrator() {
    echo "Enabling rook orchestrator"
    $KUBECTL rollout status deployment rook-ceph-tools -n "$ROOK_CLUSTER_NS" --timeout=90s
    $KUBECTL -n "$ROOK_CLUSTER_NS" exec -it deploy/rook-ceph-tools -- ceph mgr module enable rook
    $KUBECTL -n "$ROOK_CLUSTER_NS" exec -it deploy/rook-ceph-tools -- ceph orch set backend rook
    $KUBECTL -n "$ROOK_CLUSTER_NS" exec -it deploy/rook-ceph-tools -- ceph orch status
}

enable_monitoring() {
    echo "Enabling monitoring"
    $KUBECTL apply -f https://raw.githubusercontent.com/coreos/prometheus-operator/v0.40.0/bundle.yaml
    $KUBECTL wait --for=condition=ready pod -l app.kubernetes.io/name=prometheus-operator --timeout=90s
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/rbac.yaml
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/service-monitor.yaml
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/exporter-service-monitor.yaml
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/prometheus.yaml
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/prometheus-service.yaml
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
enable_rook_orchestrator
enable_monitoring
sleep 30 # wait for the metrics cache warmup

####################################################################
####################################################################
