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

source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7106" # git grep '\<7106\>' : there must be only one
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

function TEST_bench() {
    local dir=$1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1

    local osd_bench_small_size_max_iops=$(CEPH_ARGS='' ceph-conf \
        --show-config-value osd_bench_small_size_max_iops)
    local osd_bench_large_size_max_throughput=$(CEPH_ARGS='' ceph-conf \
        --show-config-value osd_bench_large_size_max_throughput)
    local osd_bench_max_block_size=$(CEPH_ARGS='' ceph-conf \
        --show-config-value osd_bench_max_block_size)
    local osd_bench_duration=$(CEPH_ARGS='' ceph-conf \
        --show-config-value osd_bench_duration)

    #
    # block size too high
    #
    expect_failure $dir osd_bench_max_block_size \
        ceph tell osd.0 bench 1024 $((osd_bench_max_block_size + 1)) || return 1

    #
    # count too high for small (< 1MB) block sizes
    #
    local bsize=1024
    local max_count=$(($bsize * $osd_bench_duration * $osd_bench_small_size_max_iops))
    expect_failure $dir bench_small_size_max_iops \
        ceph tell osd.0 bench $(($max_count + 1)) $bsize || return 1

    #
    # count too high for large (>= 1MB) block sizes
    #
    local bsize=$((1024 * 1024 + 1))
    local max_count=$(($osd_bench_large_size_max_throughput * $osd_bench_duration))
    expect_failure $dir osd_bench_large_size_max_throughput \
        ceph tell osd.0 bench $(($max_count + 1)) $bsize || return 1

    #
    # default values should work
    #
    ceph tell osd.0 bench || return 1
}

main osd-bench "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bench.sh"
# End:
