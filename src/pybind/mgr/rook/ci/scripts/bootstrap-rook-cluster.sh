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

detect_container_runtime() {
    if command -v docker &> /dev/null; then
        CONTAINER_RUNTIME="docker"
        echo "Using Docker as container runtime"
    elif command -v podman &> /dev/null; then
        CONTAINER_RUNTIME="podman"
        echo "Using Podman as container runtime"
    else
        echo "Error: neither Docker nor Podman found. Exiting."
        exit 1
    fi
}

on_error() {
    echo "on error"
    minikube delete
}

setup_minikube_env() {
    if minikube status > /dev/null 2>&1; then
        echo "Minikube is running"
        minikube stop
        minikube delete
    else
        echo "Minikube is not running"
    fi

    rm -rf ~/.minikube

    if [[ "$CONTAINER_RUNTIME" == "podman" ]]; then
        sg libvirt -c "minikube start --memory=6144 --disk-size=20g --extra-disks=4 --driver=kvm2 --container-runtime=cri-o"
        sg libvirt -c "minikube podman-env -p minikube" > /tmp/minikube-env.sh
    else
        sg libvirt -c "minikube start --memory=6144 --disk-size=20g --extra-disks=4 --driver=kvm2"
        sg libvirt -c "minikube docker-env -p minikube" > /tmp/minikube-env.sh
    fi

    source /tmp/minikube-env.sh
    rm -f /tmp/minikube-env.sh

    # Pin Docker API version to what minikube's daemon supports,
    # in case the host docker client is newer
    if [[ "$CONTAINER_RUNTIME" == "docker" ]]; then
        export DOCKER_API_VERSION=$(docker version --format '{{.Server.APIVersion}}' 2>/dev/null || echo "1.43")
    fi
}

build_ceph_image() {
    CURR_CEPH_IMG=$(grep -E '^\s*image:\s+' $CLUSTER_SPEC | sed 's/.*image: *\([^ ]*\)/\1/')

    cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci
    mkdir -p tmp_build/rook tmp_build/orchestrator tmp_build/dashboard tmp_build/dashboard_plugins
    cp ./../../orchestrator/*.py tmp_build/orchestrator
    cp ../*.py tmp_build/rook
    cp -r ../../../../../src/python-common/ceph/ tmp_build/

    # Copy all mgr Python modules from source tree to override stale versions in base image
    cp ${CEPH_DEV_FOLDER}/src/pybind/mgr/*.py tmp_build/

    # Copy updated dashboard cli to match new mgr_module API
    cp ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/cli.py tmp_build/dashboard/
    cp ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/plugins/plugin.py tmp_build/dashboard_plugins/

    $CONTAINER_RUNTIME build --tag ${LOCAL_CEPH_IMG} .
    $CONTAINER_RUNTIME tag ${LOCAL_CEPH_IMG} ${CURR_CEPH_IMG}

    rm -rf tmp_build
    cd ${CEPH_DEV_FOLDER}
}

create_rook_cluster() {
    local base="https://raw.githubusercontent.com/rook/rook/master/deploy/examples"
    $KUBECTL create -f ${base}/crds.yaml
    $KUBECTL create -f ${base}/common.yaml
    $KUBECTL create -f ${base}/csi-operator.yaml
    $KUBECTL create -f ${base}/operator.yaml
    $KUBECTL create -f $CLUSTER_SPEC
    $KUBECTL create -f ${base}/toolbox.yaml
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

    # destroy and undefine any existing default network, active or not
    if sudo virsh net-destroy default 2>/dev/null; then
        sudo virsh net-undefine default
    elif sudo virsh net-info default &>/dev/null; then
        # network exists but is already inactive, just undefine it
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
    $KUBECTL apply --server-side -f https://raw.githubusercontent.com/coreos/prometheus-operator/v0.90.1/bundle.yaml
    $KUBECTL wait --for=condition=ready pod -l app.kubernetes.io/name=prometheus-operator --timeout=90s
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/rbac.yaml
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/service-monitor.yaml
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/exporter-service-monitor.yaml
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/prometheus.yaml
    $KUBECTL apply -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/monitoring/prometheus-service.yaml

    # Wait for the prometheus pod itself to be ready before checking metrics
    $KUBECTL wait --for=condition=ready pod -l app.kubernetes.io/name=prometheus \
        -n rook-ceph --timeout=120s

    # If the ceph mgr prometheus module is not yet serving on port 9283, enable
    # it explicitly. We check first to avoid restarting an already-running
    # instance, which would trigger a port 9283 conflict.
    local mgr_pod
    mgr_pod=$($KUBECTL -n rook-ceph get pods -l app=rook-ceph-mgr \
        -o jsonpath='{.items[0].metadata.name}')
    if ! $KUBECTL -n rook-ceph exec "$mgr_pod" -- \
            curl -sf http://localhost:9283/metrics 2>/dev/null | grep -q 'ceph_health_status'; then
        echo "ceph mgr prometheus module not yet serving, enabling it..."
        $KUBECTL -n rook-ceph exec deploy/rook-ceph-tools -- ceph mgr module enable prometheus
    fi

    local attempts=0
    until $KUBECTL -n rook-ceph exec "$mgr_pod" -- \
        curl -sf http://localhost:9283/metrics | grep -q 'ceph_health_status'; do
        echo "Waiting for ceph mgr prometheus module on port 9283..."
        sleep 10
        attempts=$((attempts + 1))
        if [ $attempts -ge 18 ]; then
            echo "ceph mgr prometheus module not available after max attempts"
            exit 1
        fi
    done
    echo "ceph mgr prometheus module is serving metrics"

    # Wait for prometheus to scrape ceph metrics
    attempts=0
    until $KUBECTL -n rook-ceph exec deploy/rook-ceph-tools -- \
        curl -s 'http://rook-prometheus.rook-ceph:9090/api/v1/query?query=ceph_osd_in' \
        | grep -q '"result":\[{'; do
        echo "Waiting for prometheus to scrape ceph metrics..."
        sleep 20
        attempts=$((attempts + 1))
        if [ $attempts -ge 10 ]; then
            echo "Prometheus metrics not available after max attempts"
            exit 1
        fi
    done
}

####################################################################
####################################################################

trap 'on_error $? $LINENO' ERR

detect_container_runtime
configure_libvirt
recreate_default_network
setup_minikube_env
build_ceph_image
create_rook_cluster
wait_for_rook_operator
wait_for_ceph_cluster
enable_rook_orchestrator
enable_monitoring

####################################################################
####################################################################
