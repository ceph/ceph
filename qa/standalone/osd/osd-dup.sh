#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

[ `uname` = FreeBSD ] && exit 0

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7146" # git grep '\<7146\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # avoid running out of fds in rados bench
    CEPH_ARGS+="--filestore_wbthrottle_xfs_ios_hard_limit=900 "
    CEPH_ARGS+="--filestore_wbthrottle_btrfs_ios_hard_limit=900 "
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

main osd-dup "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-dup.sh"
# End:
