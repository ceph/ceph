#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
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

source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7111" # git grep '\<7111\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_copy_from() {
    local dir=$1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    # success
    rados -p rbd put foo rados
    rados -p rbd cp foo foo2
    rados -p rbd stat foo2

    # failure
    ceph tell osd.\* injectargs -- --osd-debug-inject-copyfrom-error
    ! rados -p rbd cp foo foo3
    ! rados -p rbd stat foo3

    # success again
    ceph tell osd.\* injectargs -- --no-osd-debug-inject-copyfrom-error
    ! rados -p rbd cp foo foo3
    rados -p rbd stat foo3
}

main osd-copy-from "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bench.sh"
# End:
