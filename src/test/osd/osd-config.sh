#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
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

    export CEPH_MON="127.0.0.1:7100" # git grep '\<7100\>' : there must be only one
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

function TEST_config_init() {
    local dir=$1

    run_mon $dir a || return 1
    local advance=1000
    local stale=1000
    local cache=500
    run_osd $dir 0 \
        --osd-map-max-advance $advance \
        --osd-map-cache-size $cache \
        --osd-pg-epoch-persisted-max-stale $stale \
        || return 1
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'is not > osd_map_max_advance' $dir/osd.0.log || return 1
    grep 'is not > osd_pg_epoch_persisted_max_stale' $dir/osd.0.log || return 1
}

function TEST_config_track() {
    local dir=$1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1

    local osd_map_cache_size=$(CEPH_ARGS='' ceph-conf \
        --show-config-value osd_map_cache_size)
    local osd_map_max_advance=$(CEPH_ARGS='' ceph-conf \
        --show-config-value osd_map_max_advance)
    local osd_pg_epoch_persisted_max_stale=$(CEPH_ARGS='' ceph-conf \
        --show-config-value osd_pg_epoch_persisted_max_stale)
    #
    # lower cache_size under max_advance to trigger the warning
    #
    ! grep 'is not > osd_map_max_advance' $dir/osd.0.log || return 1
    local cache=$(($osd_map_max_advance / 2))
    ceph tell osd.0 injectargs "--osd-map-cache-size $cache" || return 1
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'is not > osd_map_max_advance' $dir/osd.0.log || return 1
    rm $dir/osd.0.log
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log reopen || return 1

    #
    # reset cache_size to the default and assert that it does not trigger the warning
    #
    ! grep 'is not > osd_map_max_advance' $dir/osd.0.log || return 1
    local cache=$osd_map_cache_size
    ceph tell osd.0 injectargs "--osd-map-cache-size $cache" || return 1
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    ! grep 'is not > osd_map_max_advance' $dir/osd.0.log || return 1

    #
    # increase the osd_map_max_advance above the default cache_size
    #
    ! grep 'is not > osd_map_max_advance' $dir/osd.0.log || return 1
    local advance=$(($osd_map_cache_size * 2))
    ceph tell osd.0 injectargs "--osd-map-max-advance $advance" || return 1
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'is not > osd_map_max_advance' $dir/osd.0.log || return 1

    #
    # increase the osd_pg_epoch_persisted_max_stale above the default cache_size
    #
    ! grep 'is not > osd_pg_epoch_persisted_max_stale' $dir/osd.0.log || return 1
    local stale=$(($osd_map_cache_size * 2))
    ceph tell osd.0 injectargs "--osd-pg-epoch-persisted-max-stale $stale" || return 1
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'is not > osd_pg_epoch_persisted_max_stale' $dir/osd.0.log || return 1
}

main osd-config "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-config.sh"
# End:
