#!/bin/bash 
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
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
set -e

export PATH=/sbin:$PATH

: ${VERBOSE:=false}
: ${CEPH_ERASURE_CODE_BENCHMARK:=ceph_erasure_code_benchmark}
: ${PLUGIN_DIRECTORY:=/usr/lib/ceph/erasure-code}
: ${PLUGINS:=example jerasure}
: ${ITERATIONS:=1024}
: ${SIZE:=1048576}

function bench_header() {
    echo -e "seconds\tKB\tplugin\tk\tm\twork.\titer.\tsize\teras.\tcommand."
}

function bench() {
    local plugin=$1
    shift
    local k=$1
    shift
    local m=$1
    shift
    local workload=$1
    shift
    local iterations=$1
    shift
    local size=$1
    shift
    local erasures=$1
    shift
    command=$(echo $CEPH_ERASURE_CODE_BENCHMARK \
        --plugin $plugin \
        --workload $workload \
        --iterations $iterations \
        --size $size \
        --erasures $erasures \
        --parameter erasure-code-k=$k \
        --parameter erasure-code-m=$m \
        --parameter erasure-code-directory=$PLUGIN_DIRECTORY)
    result=$($command "$@")
    echo -e "$result\t$plugin\t$k\t$m\t$workload\t$iterations\t$size\t$erasures\t$command ""$@"
}

function example_test() {
    local plugin=example

    bench $plugin 2 1 encode $ITERATIONS $SIZE 0
    bench $plugin 2 1 decode $ITERATIONS $SIZE 1
}

#
# The results are expected to be consistent with 
# https://www.usenix.org/legacy/events/fast09/tech/full_papers/plank/plank_html/
# 
function jerasure_test() {
    local plugin=jerasure

    for technique in reed_sol_van ; do
        for k in 4 6 10 ; do
            for m in $(seq 1 4) ; do
                bench $plugin $k $m encode $ITERATIONS $SIZE 0 \
                    --parameter erasure-code-technique=$technique

                for erasures in $(seq 1 $m) ; do
                    bench $plugin $k $m decode $ITERATIONS $SIZE $erasures \
                        --parameter erasure-code-technique=$technique
                done
            done
        done
    done

    for technique in cauchy_orig cauchy_good ; do
        for packetsize in $(seq 512 512 4096) ; do
            for k in 4 6 10 ; do
                for m in $(seq 1 4) ; do
                    bench $plugin $k $m encode $ITERATIONS $SIZE 0 \
                        --parameter erasure-code-packetsize=$packetsize \
                        --parameter erasure-code-technique=$technique

                    for erasures in $(seq 1 $m) ; do
                        bench $plugin $k $m decode $ITERATIONS $SIZE $erasures \
                            --parameter erasure-code-packetsize=$packetsize \
                            --parameter erasure-code-technique=$technique
                    done
                done
            done
        done
    done
}

function main() {
    bench_header
    for plugin in ${PLUGINS} ; do
        ${plugin}_test || return 1
    done
}

if [ "$1" = TEST ]
then
    set -x
    set -o functrace
    PS4=' ${FUNCNAME[0]}: $LINENO: '

    ITERATIONS=1
    SIZE=1024

    function run_test() {
        dir=/tmp/erasure-code
        rm -fr $dir
        mkdir $dir
        expected=$(cat <<EOF
plugin	k	m	work.	iter.	size	eras.
example	2	1	encode	1	1024	0
example	2	1	decode	1	1024	1
jerasure	2	1	encode	1	1024	0
jerasure	2	1	decode	1	1024	1
jerasure	2	2	encode	1	1024	0
jerasure	2	2	decode	1	1024	1
jerasure	2	2	decode	1	1024	2
jerasure	2	1	encode	1	1024	0
jerasure	2	1	decode	1	1024	1
jerasure	2	2	encode	1	1024	0
jerasure	2	2	decode	1	1024	1
jerasure	2	2	decode	1	1024	2
jerasure	2	1	encode	1	1024	0
jerasure	2	1	decode	1	1024	1
jerasure	2	2	encode	1	1024	0
jerasure	2	2	decode	1	1024	1
jerasure	2	2	decode	1	1024	2
jerasure	2	1	encode	1	1024	0
jerasure	2	1	decode	1	1024	1
jerasure	2	2	encode	1	1024	0
jerasure	2	2	decode	1	1024	1
jerasure	2	2	decode	1	1024	2
jerasure	2	1	encode	1	1024	0
jerasure	2	1	decode	1	1024	1
jerasure	2	2	encode	1	1024	0
jerasure	2	2	decode	1	1024	1
jerasure	2	2	decode	1	1024	2
jerasure	2	1	encode	1	1024	0
jerasure	2	1	decode	1	1024	1
jerasure	2	2	encode	1	1024	0
jerasure	2	2	decode	1	1024	1
jerasure	2	2	decode	1	1024	2
jerasure	2	1	encode	1	1024	0
jerasure	2	1	decode	1	1024	1
jerasure	2	2	encode	1	1024	0
jerasure	2	2	decode	1	1024	1
jerasure	2	2	decode	1	1024	2
jerasure	6	3	encode	1	1024	0
jerasure	6	3	decode	1	1024	1
jerasure	6	3	decode	1	1024	2
jerasure	6	3	decode	1	1024	3
jerasure	6	4	encode	1	1024	0
jerasure	6	4	decode	1	1024	1
jerasure	6	4	decode	1	1024	2
jerasure	6	4	decode	1	1024	3
jerasure	6	4	decode	1	1024	4
jerasure	10	3	encode	1	1024	0
jerasure	10	3	decode	1	1024	1
jerasure	10	3	decode	1	1024	2
jerasure	10	3	decode	1	1024	3
jerasure	10	4	encode	1	1024	0
jerasure	10	4	decode	1	1024	1
jerasure	10	4	decode	1	1024	2
jerasure	10	4	decode	1	1024	3
jerasure	10	4	decode	1	1024	4
jerasure	6	3	encode	1	1024	0
jerasure	6	3	decode	1	1024	1
jerasure	6	3	decode	1	1024	2
jerasure	6	3	decode	1	1024	3
jerasure	6	4	encode	1	1024	0
jerasure	6	4	decode	1	1024	1
jerasure	6	4	decode	1	1024	2
jerasure	6	4	decode	1	1024	3
jerasure	6	4	decode	1	1024	4
jerasure	10	3	encode	1	1024	0
jerasure	10	3	decode	1	1024	1
jerasure	10	3	decode	1	1024	2
jerasure	10	3	decode	1	1024	3
jerasure	10	4	encode	1	1024	0
jerasure	10	4	decode	1	1024	1
jerasure	10	4	decode	1	1024	2
jerasure	10	4	decode	1	1024	3
jerasure	10	4	decode	1	1024	4
jerasure	6	3	encode	1	1024	0
jerasure	6	3	decode	1	1024	1
jerasure	6	3	decode	1	1024	2
jerasure	6	3	decode	1	1024	3
jerasure	6	4	encode	1	1024	0
jerasure	6	4	decode	1	1024	1
jerasure	6	4	decode	1	1024	2
jerasure	6	4	decode	1	1024	3
jerasure	6	4	decode	1	1024	4
jerasure	10	3	encode	1	1024	0
jerasure	10	3	decode	1	1024	1
jerasure	10	3	decode	1	1024	2
jerasure	10	3	decode	1	1024	3
jerasure	10	4	encode	1	1024	0
jerasure	10	4	decode	1	1024	1
jerasure	10	4	decode	1	1024	2
jerasure	10	4	decode	1	1024	3
jerasure	10	4	decode	1	1024	4
EOF
)
        test "$(main | cut --fields=3-9 )" = "$expected" || return 1
    }

    run_test
else
    main
fi
# Local Variables:
# compile-command: "\
#   CEPH_ERASURE_CODE_BENCHMARK=../../../src/ceph_erasure_code_benchmark \
#   PLUGIN_DIRECTORY=../../../src/.libs \
#   ./bench.sh TEST
# "
# End:
