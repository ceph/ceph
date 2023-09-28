#!/usr/bin/env bash

set -ex

get_vm_ip () {
    local ip=$(kcli info vm "$1" -f ip -v | grep -Eo '[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}')
    echo -n $ip
}

if [[ -n "${JENKINS_HOME}" || -z "$(get_vm_ip cephkube-ctlplane-0)" ]]; then
    . "$(dirname $0)"/start-cluster.sh

fi

# Execute tests
: ${CEPH_DEV_FOLDER:=${PWD}}
cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci/tests
behave
