#!/bin/bash
#
# Copyright (C) 2014,2015,2017 Red Hat <contact@redhat.com>
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
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1

    setup $dir || return 1

    MON=127.0.0.1:7150  # git grep '\<7150\>' : there must be only one
    (
        FSID=$(uuidgen) 
        export CEPH_ARGS
        CEPH_ARGS+="--fsid=$FSID --auth-supported=none "
        CEPH_ARGS+="--mon-initial-members=a --mon-host=$MON "
        CEPH_ARGS+="--mgr-initial-modules=dashbaord "
	CEPH_ARGS+="--mon-host=$MON"
        run_mon $dir a --public-addr $MON || return 1
    )

    timeout 360 ceph --mon-host $MON mon stat || return 1
    export CEPH_ARGS="--mon_host $MON "
    ceph config-key put mgr/x/dashboard/server_port 7001
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

    tries=0
    while [[ $tries < 30 ]] ; do
        if [ curl -s http://127.0.0.1:7001/toplevel_data | \
             jq '.health.overall_status' | grep HEALTH_ ]
        then
            break
        fi
        tries=$((tries+1))
        sleep 0.5
    done

    teardown $dir || return 1
}

main mgr-dashboard-smoke "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 TESTS=test/mon/mon-dashboard-smoke.sh check"
# End:
