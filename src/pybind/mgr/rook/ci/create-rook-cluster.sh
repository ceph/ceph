#!/usr/bin/env bash

set -eEx

export CEPH_DEV_FOLDER=/home/redo/workspaces/ceph

kcli_run_cmd() {
    local remote_host="$1"
    local ssh_command="$2"

    # Run the SSH command and capture its exit status
    output=$(kcli ssh -u root -- "$remote_host" "$ssh_command")
    ssh_exit_status=$?

    # Check if the SSH command was successful
    if [ "$ssh_exit_status" -ne 0 ]; then
        exit $ssh_exit_status
    else
	echo $output
    fi
}

cleanup() {
    set +x
    if [[ -n "$JENKINS_HOME" ]]; then
        echo "Starting cleanup..."
        kcli delete plan -y cephkube || true
        kcli delete pool cephkubePool -y
        rm -rf ${HOME}/.kcli
        docker container prune -f
        echo "Cleanup completed."
    fi
}

on_error() {
    set +x
    if [ "$1" != "0" ]; then
        echo "ERROR $1 thrown on line $2"
        echo
        echo "Collecting info..."
        echo
        echo "Saving MGR logs:"
        echo
        mkdir -p ${CEPH_DEV_FOLDER}/logs
        active_manager=$(kcli_run_cmd cephkube-ctlplane-0 "kubectl rook-ceph ceph mgr dump | grep active_name | awk 'NF>1{print $NF}'")
        active_manager=$(echo $active_manager | tr -d \",\')

        kcli_run_cmd cephkube-ctlplane-0 'kubectl logs -n rook-ceph -l ceph_daemon_id='${active_manager} > ${CEPH_DEV_FOLDER}/logs/mgr.${active_manager}.rook.log
        for vm in cephkube-ctlplane-0 cephkube-worker-0 cephkube-worker-1
        do
            echo "Saving journalctl from VM ${vm}:"
            echo
            kcli_run_cmd ${vm} 'journalctl --no-tail --no-pager -t cloud-init' > ${CEPH_DEV_FOLDER}/logs/journal.ceph-node-0${vm_id}.log || true
            echo "Saving container logs:"
            echo
            kcli_run_cmd ${vm} 'podman logs --names --since 30s \$(podman ps -aq)' > ${CEPH_DEV_FOLDER}/logs/container.ceph-node-0${vm_id}.log || true
        done
        echo "TEST FAILED."
    fi
}

build_ceph_image() {
    : ${REPOTAG:='rkachach/ceph:redo-rook'}
    cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci
    mkdir -p tmpBuild/rook
    mkdir -p tmpBuild/orchestrator
    cp ./../../orchestrator/*.py tmpBuild/orchestrator
    cp ../*.py tmpBuild/rook
    docker build --tag ${REPOTAG} .
    docker push ${REPOTAG}
    sed "s#testimage#${REPOTAG}#g" rook_cluster_basic.yaml > tmpBuild/rook_cluster_basic.yaml
    kcli scp -u root tmpBuild/rook_cluster_basic.yaml cephkube-ctlplane-0:/root
    rm -rf tmpBuild
    cd ${CEPH_DEV_FOLDER}
}

install_rook_operator() {
    kcli_run_cmd cephkube-ctlplane-0 'git clone --single-branch --branch master https://github.com/rook/rook.git'
    kcli_run_cmd cephkube-ctlplane-0 'kubectl create -f /root/rook/deploy/examples/crds.yaml'
    kcli_run_cmd cephkube-ctlplane-0 'kubectl create -f /root/rook/deploy/examples/common.yaml'
    kcli_run_cmd cephkube-ctlplane-0 'kubectl create -f /root/rook/deploy/examples/operator.yaml'
    # wait some time for the operator to start running
    sleep 20 # use some check instead of just waiting a fixed amount of time
    kcli_run_cmd cephkube-ctlplane-0 'kubectl -n rook-ceph get pods'
}

wait_for_rook_operator() {
    : ${WAIT_OPERATOR_RUNNING:=5}
    while [[ $(kcli ssh -u root -- cephkube-ctlplane-0 "kubectl get pods --selector=app=rook-ceph-operator -n rook-ceph -o jsonpath='{.items[*].status.phase}'")  != "Running" ]]; do
	echo "Waiting for operator pod running..."
	sleep ${WAIT_OPERATOR_RUNNING}
    done
    echo "Rook operator running"
}

install_krew() {
    kcli scp -u root ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci/install_krew.sh cephkube-ctlplane-0:/root
    kcli_run_cmd cephkube-ctlplane-0 'chmod +x /root/install_krew.sh'
    kcli_run_cmd cephkube-ctlplane-0 '/root/install_krew.sh'
}

prepare_kcli() {
    : ${VM_IMAGE:='fedora36'}
    : ${VM_IMAGE_URL:='https://download.fedoraproject.org/pub/fedora/linux/releases/36/Cloud/x86_64/images/Fedora-Cloud-Base-36-1.5.x86_64.qcow2'}
    kcli create pool cephkubePool -p /tmp/kcli_pool || true
    kcli delete plan -y cephkube || true
    kcli download image -p cephkubePool -u ${VM_IMAGE_URL} ${VM_IMAGE}
    kcli delete plan -y cephkube || true
    kcli create kube generic --paramfile=./k8cluster.yaml cephkube
    #export KUBECONFIG=$HOME/.kcli/clusters/cephkube/auth/kubeconfig

    : ${WAIT_MASTER_READY:=30}
    while [[ -z $(kcli_run_cmd cephkube-ctlplane-0 'journalctl --no-tail --no-pager -t cloud-init' | grep "kcli boot finished") ]]; do
	echo "Waiting for master node ready..."
	sleep ${WAIT_MASTER_READY}
    done

    : ${k8NODESREADY_CHECK_INTERVAL:=30}
    : ${MIN_READY_NODES:=3}
    while [[  $(kcli_run_cmd cephkube-ctlplane-0 'kubectl get nodes | grep "Ready" | wc -l') != ${MIN_READY_NODES} ]]; do
        echo "Waiting for $MIN_READY_NODES nodes ready..."
        sleep ${k8NODESREADY_CHECK_INTERVAL}
    done
    echo "$MIN_READY_NODES nodes ready"
}

####################################################################
####################################################################

trap 'on_error $? $LINENO' ERR
trap 'cleanup $? $LINENO' EXIT

: ${CEPH_DEV_FOLDER:=${PWD}}
prepare_kcli
install_krew
install_rook_operator
build_ceph_image
wait_for_rook_operator

# Deploy ceph cluster and Install rook ceph krew plugin
kcli_run_cmd cephkube-ctlplane-0 'kubectl create -f /root/rook_cluster_basic.yaml'
kcli_run_cmd cephkube-ctlplane-0 'kubectl krew install rook-ceph'

# Wait for ceph cluster ready
: ${WAIT_CEPH_CLUSTER_RUNNING:=20}
until [[ $(kcli_run_cmd cephkube-ctlplane-0 'kubectl rook-ceph ceph status') =~ .*'health: HEALTH_OK'.* ]]; do
      echo "Waiting for Ceph cluster installed"
      sleep ${WAIT_CEPH_CLUSTER_RUNNING}
done
echo "Ceph cluster installed and running"

# Set rook orchestrator as backend
kcli_run_cmd cephkube-ctlplane-0 'kubectl rook-ceph ceph mgr module enable rook'
kcli_run_cmd cephkube-ctlplane-0 'kubectl rook-ceph ceph orch set backend rook'
kcli_run_cmd cephkube-ctlplane-0 'kubectl rook-ceph ceph orch status'
