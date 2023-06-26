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
    local override_config="excludeSpecPattern=*.po.ts,retries=0,specPattern=${specs},chromeWebSecurity=false"
    if [[ -n "$timeout" ]]; then
        override_config="${override_config},defaultCommandTimeout=${timeout}"
    fi

    rm -f cypress/reports/results-*.xml || true

    npx --no-install cypress run ${CYPRESS_ARGS} --browser chrome --headless --config "$override_config"
}

: ${CEPH_DEV_FOLDER:=${PWD}}

cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/dashboard/frontend

kcli ssh -u root ceph-node-00 'cephadm shell "ceph config set mgr mgr/prometheus/exclude_perf_counters false"'

# check if the prometheus daemon is running
# before starting the e2e tests

PROMETHEUS_RUNNING_COUNT=$(kcli ssh -u root ceph-node-00 'cephadm shell "ceph orch ls --service_name=prometheus --format=json"' | jq -r '.[] | .status.running')
while [[ $PROMETHEUS_RUNNING_COUNT -lt 1 ]]; do
    PROMETHEUS_RUNNING_COUNT=$(kcli ssh -u root ceph-node-00 'cephadm shell "ceph orch ls --service_name=prometheus --format=json"' | jq -r '.[] | .status.running')
done

# grafana ip address is set to the fqdn by default.
# kcli is not working with that, so setting the IP manually.
kcli ssh -u root ceph-node-00 'cephadm shell "ceph dashboard set-alertmanager-api-host http://192.168.100.100:9093"'
kcli ssh -u root ceph-node-00 'cephadm shell "ceph dashboard set-prometheus-api-host http://192.168.100.100:9095"'
kcli ssh -u root ceph-node-00 'cephadm shell "ceph dashboard set-grafana-api-url https://192.168.100.100:3000"'
kcli ssh -u root ceph-node-00 'cephadm shell "ceph orch apply node-exporter --placement 'count:2'"'

cypress_run ["cypress/e2e/orchestrator/workflow/*.feature","cypress/e2e/orchestrator/workflow/*-spec.ts"]
cypress_run "cypress/e2e/orchestrator/grafana/*.feature"
