#!/usr/bin/env bash

set -eEx

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
        active_manager=$(kcli ssh -u root -- cephkube-ctlplane-0 "kubectl rook-ceph ceph mgr dump | grep active_name | awk 'NF>1{print $NF}'")
        active_manager=$(echo $active_manager | tr -d \",\')

        kcli ssh -u root -- cephkube-ctlplane-0 'kubectl logs -n rook-ceph -l ceph_daemon_id='${active_manager} > ${CEPH_DEV_FOLDER}/logs/mgr.${active_manager}.rook.log
        for vm in cephkube-ctlplane-0 cephkube-worker-0 cephkube-worker-1
        do
            echo "Saving journalctl from VM ${vm}:"
            echo
            kcli ssh -u root -- ${vm} 'journalctl --no-tail --no-pager -t cloud-init' > ${CEPH_DEV_FOLDER}/logs/journal.ceph-node-0${vm_id}.log || true
            echo "Saving container logs:"
            echo
            kcli ssh -u root -- ${vm} 'podman logs --names --since 30s \$(podman ps -aq)' > ${CEPH_DEV_FOLDER}/logs/container.ceph-node-0${vm_id}.log || true
        done
        echo "TEST FAILED."
    fi
}

trap 'on_error $? $LINENO' ERR
trap 'cleanup $? $LINENO' EXIT

kcli create pool cephkubePool -p /tmp/kcli_pool || true
kcli delete plan -y cephkube || true

: ${CEPH_DEV_FOLDER:=${PWD}}
: ${VM_IMAGE:='fedora36'}
: ${VM_IMAGE_URL:='https://download.fedoraproject.org/pub/fedora/linux/releases/36/Cloud/x86_64/images/Fedora-Cloud-Base-36-1.5.x86_64.qcow2'}
kcli download image -p cephkubePool -u ${VM_IMAGE_URL} ${VM_IMAGE}
kcli delete plan -y cephkube || true
kcli create kube generic --paramfile=./k8cluster.yaml cephkube
#export KUBECONFIG=$HOME/.kcli/clusters/cephkube/auth/kubeconfig


: ${WAIT_MASTER_READY:=30}
while [[ -z $(kcli ssh -u root -- cephkube-ctlplane-0 'journalctl --no-tail --no-pager -t cloud-init' | grep "kcli boot finished") ]]; do
    echo "Waiting for master node ready..."
    sleep ${WAIT_MASTER_READY}
done

#
: ${k8NODESREADY_CHECK_INTERVAL:=30}
: ${MIN_READY_NODES:=3}
while [[  $(kcli ssh -u root -- cephkube-ctlplane-0 'kubectl get nodes | grep "Ready" | wc -l') != ${MIN_READY_NODES} ]]; do
        echo "Waiting for $MIN_READY_NODES nodes ready..."
        sleep ${k8NODESREADY_CHECK_INTERVAL}
done
echo "$MIN_READY_NODES nodes ready"


# Install krew
kcli scp -u root ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci/install_krew.sh cephkube-ctlplane-0:/root
kcli ssh -u root -- cephkube-ctlplane-0 'chmod +x /root/install_krew.sh'
kcli ssh -u root -- cephkube-ctlplane-0 '/root/install_krew.sh'
if [[ $? != 0 ]]; then
        echo "Krew plugin not available. Not possible to continue test"
        exit 1
fi

# Install rook operator and ceph cluster
kcli ssh -u root -- cephkube-ctlplane-0 'git clone --single-branch --branch master https://github.com/rook/rook.git'
kcli ssh -u root -- cephkube-ctlplane-0 'kubectl create -f /root/rook/deploy/examples/crds.yaml'
kcli ssh -u root -- cephkube-ctlplane-0 'kubectl create -f /root/rook/deploy/examples/common.yaml'
kcli ssh -u root -- cephkube-ctlplane-0 'kubectl create -f /root/rook/deploy/examples/operator.yaml'
sleep ${k8NODESREADY_CHECK_INTERVAL}
kcli ssh -u root -- cephkube-ctlplane-0 'kubectl -n rook-ceph get pods'

# Meanwhile the operator is installed, build the ceph image to test and copy it to the vm cluster
: ${REPOTAG:='quay.io/jmolmo/ceph:rookorch'}
cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci
mkdir -p tmpBuild/rook
mkdir -p tmpBuild/orchestrator
cp ./../../orchestrator/*.py tmpBuild/orchestrator
cp ../*.py tmpBuild/rook
docker build --tag ${REPOTAG} .
docker push ${REPOTAG}
sed "s#testimage#${REPOTAG}#g" rook_cluster_basic.yaml > tmpBuild/testcluster.yaml
kcli scp -u root tmpBuild/testcluster.yaml cephkube-ctlplane-0:/root
rm -rf tmpBuild
cd ${CEPH_DEV_FOLDER}

# wait for operator running
: ${WAIT_OPERATOR_RUNNING:=5}
while [[ $(kcli ssh -u root -- cephkube-ctlplane-0 "kubectl get pods --selector=app=rook-ceph-operator -n rook-ceph -o jsonpath='{.items[*].status.phase}'")  != "Running" ]]; do
    echo "Waiting for operator pod running..."
    sleep ${WAIT_OPERATOR_RUNNING}
done
echo "Rook operator running"

# Deploy ceph cluster
kcli ssh -u root -- cephkube-ctlplane-0 'kubectl create -f /root/testcluster.yaml'

# Install rook ceph krew plugin
kcli ssh -u root -- cephkube-ctlplane-0 'kubectl krew install rook-ceph'

# Wait for ceph cluster ready
: ${WAIT_CEPH_CLUSTER_RUNNING:=20}
until [[ $(kcli ssh -u root -- cephkube-ctlplane-0 'kubectl rook-ceph ceph status') =~ .*'health: HEALTH_OK'.* ]]; do
      echo "Waiting for Ceph cluster installed"
      sleep ${WAIT_CEPH_CLUSTER_RUNNING}
done
echo "Ceph cluster installed and running"

# Set rook orchestrator as backend
kcli ssh -u root -- cephkube-ctlplane-0 'kubectl rook-ceph ceph orch set backend rook'
kcli ssh -u root -- cephkube-ctlplane-0 'kubectl rook-ceph ceph orch status'
