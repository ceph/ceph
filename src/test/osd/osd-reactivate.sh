#!/bin/bash
#
# Author: Vicente Cheng <freeze.bilsted@gmail.com>
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

    export CEPH_MON="127.0.0.1:7122" # git grep '\<7122\>' : there must be only one
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

function TEST_reactivate() {
    local dir=$1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1

    kill_daemons $dir TERM osd || return 1

    ready_path=$dir"/0/ready"
    activate_path=$dir"/0/active"
    # trigger mkfs again
    rm -rf $ready_path $activate_path
    activate_osd $dir 0 || return 1

}

main osd-reactivate "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-reactivate.sh"
# End:
