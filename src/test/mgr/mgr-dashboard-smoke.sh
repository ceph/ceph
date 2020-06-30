#!/usr/bin/env bash
#
# Copyright (C) 2014,2015,2017 Red Hat <contact@redhat.com>
# Copyright (C) 2018 SUSE LLC
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

mon_port=$(get_unused_port)
dashboard_port=$((mon_port+1))

function run() {
    local dir=$1
    shift

    export CEPH_MON=127.0.0.1:$mon_port
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-initial-members=a --mon-host=$MON "
    CEPH_ARGS+="--mgr-initial-modules=dashboard "
    CEPH_ARGS+="--mon-host=$CEPH_MON"

    setup $dir || return 1
    TEST_dashboard $dir || return 1
    teardown $dir || return 1
}

function TEST_dashboard() {
    local dir=$1
    shift

    run_mon $dir a || return 1
    timeout 30 ceph mon stat || return 1
    ceph config-key set mgr/dashboard/x/server_port $dashboard_port
    MGR_ARGS+="--mgr_module_path=${CEPH_ROOT}/src/pybind/mgr "
    run_mgr $dir x ${MGR_ARGS} || return 1

    tries=0
    while [[ $tries < 30 ]] ; do
        if [ $(ceph status -f json | jq .mgrmap.available) = "true" ]
        then
            break
        fi
        tries=$((tries+1))
        sleep 1
    done
    ceph_adm dashboard set-login-credentials admin admin

    tries=0
    while [[ $tries < 30 ]] ; do
        if curl -c $dir/cookiefile -X POST -d '{"username":"admin","password":"admin"}' http://127.0.0.1:$dashboard_port/api/auth
        then
            if curl -b $dir/cookiefile -s http://127.0.0.1:$dashboard_port/api/summary | \
                 jq '.health.overall_status' | grep HEALTH_
            then
                break
            fi
        fi
        tries=$((tries+1))
        sleep 0.5
    done
}

main mgr-dashboard-smoke "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 TESTS=test/mgr/mgr-dashboard-smoke.sh check"
# End:
