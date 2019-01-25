#!/usr/bin/env bash
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014,2015 Red Hat <contact@redhat.com>
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

function run() {
    local dir=$1

    setup $dir || return 1

    MONA=127.0.0.1:7300
    MONB=127.0.0.1:7301
    (
        FSID=$(uuidgen)
        export CEPH_ARGS
        CEPH_ARGS+="--fsid=$FSID --auth-supported=none "
        CEPH_ARGS+="--mon-initial-members=a,b --mon-host=$MONA,$MONB "
        run_mon $dir a --public-addr $MONA || return 1
        run_mon $dir b --public-addr $MONB || return 1
    )

    timeout 360 ceph --mon-host $MONA mon stat || return 1
    # check that MONB is indeed a peon
    ceph --admin-daemon $(get_asok_path mon.b) mon_status |
       grep '"peon"' || return 1
    # when the leader ( MONA ) is used, there is no message forwarding
    ceph --mon-host $MONA osd pool create POOL1 12
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.a) log flush || return 1
    grep 'mon_command(.*"POOL1"' $dir/mon.a.log || return 1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.b) log flush || return 1
    grep 'mon_command(.*"POOL1"' $dir/mon.b.log && return 1
    # when the peon ( MONB ) is used, the message is forwarded to the leader
    ceph --mon-host $MONB osd pool create POOL2 12
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.b) log flush || return 1
    grep 'forward_request.*mon_command(.*"POOL2"' $dir/mon.b.log || return 1
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.a) log flush || return 1
    grep ' forward(mon_command(.*"POOL2"' $dir/mon.a.log || return 1
    # forwarded messages must retain features from the original connection
    features=$(sed -n -e 's|.*127.0.0.1:0.*accept features \([0-9][0-9]*\)|\1|p' < \
        $dir/mon.b.log)
    grep ' forward(mon_command(.*"POOL2".*con_features '$features $dir/mon.a.log || return 1

    teardown $dir || return 1
}

main mon-handle-forward "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 TESTS=test/mon/mon-handle-forward.sh check"
# End:
