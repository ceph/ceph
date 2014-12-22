#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
# Author: Sage Weil <sage@redhat.com>
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
source test/osd/osd-test-helpers.sh

function run() {
    local dir=$1

    export CEPH_MON="127.0.0.1:7111"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local id=a
    call_TEST_functions $dir $id || return 1
}

function TEST_copy_from() {
    local dir=$1

    run_mon $dir a --public-addr $CEPH_MON \
        || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    # success
    ./rados -p rbd put foo rados
    ./rados -p rbd cp foo foo2
    ./rados -p rbd stat foo2

    # failure
    ./ceph tell osd.\* injectargs -- --osd-debug-inject-copyfrom-error
    ! ./rados -p rbd cp foo foo3
    ! ./rados -p rbd stat foo3

    # success again
    ./ceph tell osd.\* injectargs -- --no-osd-debug-inject-copyfrom-error
    ! ./rados -p rbd cp foo foo3
    ./rados -p rbd stat foo3
}

main osd-copy-from

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bench.sh"
# End:
