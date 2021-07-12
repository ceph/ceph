#!/usr/bin/env bash

set -ex

: ${CYPRESS_BASE_URL:=''}
: ${CYPRESS_LOGIN_USER:='admin'}
: ${CYPRESS_LOGIN_PWD:='password'}
: ${CYPRESS_ARGS:=''}

if [[ -z "${CYPRESS_BASE_URL}" ]]; then
    . "$(dirname $0)"/start-cluster.sh

    CYPRESS_BASE_URL="https://$(kcli info vm ceph-node-00 -f ip -v | sed -e 's/[^0-9.]//'):8443"
fi

export CYPRESS_BASE_URL CYPRESS_LOGIN_USER CYPRESS_LOGIN_PWD

cypress_run () {
    local specs="$1"
    local timeout="$2"
    local override_config="ignoreTestFiles=*.po.ts,retries=0,testFiles=${specs}"

    if [[ -n "$timeout" ]]; then
        override_config="${override_config},defaultCommandTimeout=${timeout}"
    fi
    npx cypress run ${CYPRESS_ARGS} --browser chrome --headless --config "$override_config"
}

cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/frontend

cypress_run "orchestrator/workflow/*-spec.ts"
