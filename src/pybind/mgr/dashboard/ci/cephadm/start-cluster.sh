#!/usr/bin/env bash

set -ex

cleanup() {
    set +x
    if [[ -n "$JENKINS_HOME" ]]; then
        printf "\n\nStarting cleanup...\n\n"
        kcli delete plan -y ceph || true
        sudo podman container prune -f
        printf "\n\nCleanup completed.\n\n"
    fi
}

on_error() {
    set +x
    if [ "$1" != "0" ]; then
        printf "\n\nERROR $1 thrown on line $2\n\n"
        printf "\n\nCollecting info...\n\n"
        for vm_id in 0 1 2
        do
            local vm="ceph-node-0${vm_id}"
            printf "\n\nDisplaying journalctl from VM ${vm}:\n\n"
            kcli ssh -u root -- ${vm} 'journalctl --no-tail --no-pager -t cloud-init' || true
            printf "\n\nEnd of journalctl from VM ${vm}\n\n"
            printf "\n\nDisplaying podman logs:\n\n"
            kcli ssh -u root -- ${vm} 'podman logs --names --since 30s $(podman ps -aq)' || true
        done
        printf "\n\nTEST FAILED.\n\n"
    fi
}

trap 'on_error $? $LINENO' ERR
trap 'cleanup $? $LINENO' EXIT

sed -i '/ceph-node-/d' $HOME/.ssh/known_hosts

: ${CEPH_DEV_FOLDER:=${PWD}}
EXTRA_PARAMS=''
DEV_MODE=''
# Check script args/options.
for arg in "$@"; do
  shift
  case "$arg" in
    "--dev-mode") DEV_MODE='true'; EXTRA_PARAMS="-P dev_mode=${DEV_MODE}" ;;
  esac
done

kcli delete plan -y ceph || true

# Build dashboard frontend (required to start the module).
cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/frontend
NG_CLI_ANALYTICS=false npm ci
FRONTEND_BUILD_OPTS='-- --prod'
if [[ -n "${DEV_MODE}" ]]; then
    FRONTEND_BUILD_OPTS+=' --deleteOutputPath=false --watch'
fi
npm run build ${FRONTEND_BUILD_OPTS} &

cd ${CEPH_DEV_FOLDER}
kcli delete plan -y ceph || true
kcli create plan -f ./src/pybind/mgr/dashboard/ci/cephadm/ceph_cluster.yml \
    -P ceph_dev_folder=${CEPH_DEV_FOLDER} \
    ${EXTRA_PARAMS} ceph

while [[ -z $(kcli ssh -u root -- ceph-node-00 'journalctl --no-tail --no-pager -t cloud-init' | grep "Dashboard is now available") ]]; do
    sleep 30
    kcli list vm
    # Uncomment for debugging purposes.
    #kcli ssh -u root -- ceph-node-00 'podman ps -a'
    #kcli ssh -u root -- ceph-node-00 'podman logs --names --since 30s $(podman ps -aq)'
    kcli ssh -u root -- ceph-node-00 'journalctl -n 100 --no-pager -t cloud-init'
done
