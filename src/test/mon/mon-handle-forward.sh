#!/bin/bash
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014 Red Hat <contact@redhat.com>
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
source test/mon/mon-test-helpers.sh

function run() {
    local dir=$1

    PORT=7300 # CEPH_MON=
    MONA=127.0.0.1:$PORT
    MONB=127.0.0.1:$(($PORT + 1))
    (
        FSID=$(uuidgen) 
        export CEPH_ARGS
        CEPH_ARGS+="--fsid=$FSID --auth-supported=none "
        CEPH_ARGS+="--mon-initial-members=a,b --mon-host=$MONA,$MONB "
        run_mon $dir a --public-addr $MONA
        run_mon $dir b --public-addr $MONB
    )

    timeout 360 ./ceph --mon-host $MONA mon stat || return 1
    # check that MONB is indeed a peon
    ./ceph --admin-daemon $dir/b/ceph-mon.b.asok mon_status | 
       grep '"peon"' || return 1
    # when the leader ( MONA ) is used, there is no message forwarding
    ./ceph --mon-host $MONA osd pool create POOL1 12 
    grep 'mon_command(.*"POOL1"' $dir/a/log 
    grep 'mon_command(.*"POOL1"' $dir/b/log && return 1
    # when the peon ( MONB ) is used, the message is forwarded to the leader
    ./ceph --mon-host $MONB osd pool create POOL2 12 
    grep 'forward_request.*mon_command(.*"POOL2"' $dir/b/log
    grep ' forward(mon_command(.*"POOL2"' $dir/a/log 
    # forwarded messages must retain features from the original connection
    features=$(sed -n -e 's|.*127.0.0.1:0.*accept features \([0-9][0-9]*\)|\1|p' < \
        $dir/b/log)
    grep ' forward(mon_command(.*"POOL2".*con_features '$features $dir/a/log
}

main mon-handle-forward

# Local Variables:
# compile-command: "cd ../.. ; make TESTS=test/mon/mon-handle-forward.sh check"
# End:
