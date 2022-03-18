#!/usr/bin/env bash

set -ex

: ${CYPRESS_BASE_URL:=''}
: ${CYPRESS_LOGIN_USER:='admin'}
: ${CYPRESS_LOGIN_PWD:='password'}
: ${CYPRESS_ARGS:=''}
: ${DASHBOARD_PORT:='8443'}

get_vm_ip () {
    local ip=$(kcli info vm "$1" -f ip -v | grep -Eo '[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}')
    echo -n $ip
}

if [[ -n "${JENKINS_HOME}" || (-z "${CYPRESS_BASE_URL}" && -z "$(get_vm_ip ceph-node-00)") ]]; then
    . "$(dirname $0)"/start-cluster.sh

    CYPRESS_BASE_URL="https://$(get_vm_ip ceph-node-00):${DASHBOARD_PORT}"
fi

export CYPRESS_BASE_URL CYPRESS_LOGIN_USER CYPRESS_LOGIN_PWD

cypress_run () {
    local specs="$1"
    local timeout="$2"
    local override_config="ignoreTestFiles=*.po.ts,retries=0,testFiles=${specs}"
    if [[ -n "$timeout" ]]; then
        override_config="${override_config},defaultCommandTimeout=${timeout}"
    fi

    rm -f cypress/reports/results-*.xml || true

    npx --no-install cypress run ${CYPRESS_ARGS} --browser chrome --headless --config "$override_config"
}

: ${CEPH_DEV_FOLDER:=${PWD}}

cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/frontend

cypress_run "orchestrator/workflow/*.feature"
cypress_run "orchestrator/workflow/*-spec.ts"
