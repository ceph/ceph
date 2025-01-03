#!/usr/bin/env bash
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
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

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

export CEPH_VSTART_WRAPPER=1
export CEPH_DIR="${TMPDIR:-$PWD}/td/t-$CEPH_PORT"
export CEPH_DEV_DIR="$CEPH_DIR/dev"
export CEPH_OUT_DIR="$CEPH_DIR/out"
export CEPH_ASOK_DIR="$CEPH_DIR/out"

export MGR_PYTHON_PATH=$CEPH_ROOT/src/pybind/mgr

function vstart_setup()
{
    rm -fr $CEPH_DEV_DIR $CEPH_OUT_DIR
    mkdir -p $CEPH_DEV_DIR
    trap "teardown $CEPH_DIR" EXIT
    export LC_ALL=C # some tests are vulnerable to i18n
    export PATH="$(pwd):${PATH}"
    OBJSTORE_ARGS=""
    if [ "bluestore" = "${CEPH_OBJECTSTORE}" ]; then
        OBJSTORE_ARGS="-b"
    fi
    $CEPH_ROOT/src/vstart.sh \
        --short \
        $OBJSTORE_ARGS \
        -o 'paxos propose interval = 0.01' \
        -d -n -l || return 1
    export CEPH_CONF=$CEPH_DIR/ceph.conf

    crit=$(expr 100 - $(ceph-conf --show-config-value mon_data_avail_crit))
    if [ $(df . | perl -ne 'print if(s/.*\s(\d+)%.*/\1/)') -ge $crit ] ; then
        df . 
        cat <<EOF
error: not enough free disk space for mon to run
The mon will shutdown with a message such as 
 "reached critical levels of available space on local monitor storage -- shutdown!"
as soon as it finds the disk has is more than ${crit}% full. 
This is a limit determined by
 ceph-conf --show-config-value mon_data_avail_crit
EOF
        return 1
    fi
}

function main()
{
    teardown $CEPH_DIR
    vstart_setup || return 1
    if CEPH_CONF=$CEPH_DIR/ceph.conf "$@"; then
        code=0
    else
        code=1
        display_logs $CEPH_OUT_DIR
    fi
    return $code
}

main "$@"
