#! /bin/bash
#
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
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7123" # git grep '\<7123\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_reuse_id() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    wait_for_clean || return 1
    destroy_osd $dir 1 || return 1
    run_osd $dir 1 || return 1
}

main osd-reuse-id "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-reuse-id.sh"
# End:

