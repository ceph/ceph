#!/bin/bash 
#
# Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
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
# Test that it works from sources with:
#
#  TOTAL_SIZE=$((1024 * 1024)) \
#  CEPH_ERASURE_CODE_BENCHMARK=src/ceph_erasure_code_benchmark  \
#  PLUGIN_DIRECTORY=src/.libs \
#      qa/workunits/erasure-code/bench.sh fplot jerasure |
#      tee qa/workunits/erasure-code/bench.js
#
# This should start immediately and display:
#
# var encode_reed_sol_van_4096 = [
# [ '2/1',  .48035538612887358583  ],
# [ '3/2',  .21648470405675016626  ],
# etc.
#
# and complete within a few seconds. The result can then be displayed with:
#
#  firefox qa/workunits/erasure-code/bench.html
#
# Once it is confirmed to work, it can be run with a more significant
# volume of data so that the measures are more reliable:
#
#  CEPH_ERASURE_CODE_BENCHMARK=src/ceph_erasure_code_benchmark  \
#  PLUGIN_DIRECTORY=src/.libs \
#      qa/workunits/erasure-code/bench.sh fplot jerasure |
#      tee qa/workunits/erasure-code/bench.js
#
set -e

export PATH=/sbin:$PATH

: ${VERBOSE:=false}
: ${CEPH_ERASURE_CODE_BENCHMARK:=ceph_erasure_code_benchmark}
: ${PLUGIN_DIRECTORY:=/usr/lib/ceph/erasure-code}
: ${PLUGINS:=example jerasure isa}
: ${TOTAL_SIZE:=$((10 * 1024 * 1024))}
: ${SIZES:=4096 $((1024 * 1024))}
: ${PARAMETERS:=--parameter jerasure-per-chunk-alignment=true}

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
        --parameter k=$k \
        --parameter m=$m \
        --parameter directory=$PLUGIN_DIRECTORY)
    result=$($command "$@")
    echo -e "$result\t$plugin\t$k\t$m\t$workload\t$iterations\t$size\t$erasures\t$command ""$@"
}

function example_test() {
    local plugin=example
    local size
    for size in $SIZES ; do
        bench $plugin 2 1 encode $ITERATIONS $size 0
        bench $plugin 2 1 decode $ITERATIONS $size 1
    done
}

function packetsize() {
    local k=$1
    local w=$2
    local vector_wordsize=$3
    local size=$4

    local p=$(( ($size / $k / $w / $vector_wordsize ) * $vector_wordsize))
    if [ $p -gt 3100 ] ; then
        p=3100
    fi
    echo $p
}

function jerasure_test() {
    local plugin=jerasure
    local w=8
    local VECTOR_WORDSIZE=16
    local ks="2 3 4 6 10"
    declare -A k2ms
    k2ms[2]="1"
    k2ms[3]="2"
    k2ms[4]="2 3"
    k2ms[6]="2 3 4"
    k2ms[10]="3 4"
    for technique in reed_sol_van cauchy_good ; do
        for size in $SIZES ; do
            echo "serie encode_${technique}_${size}"
            for k in $ks ; do
                for m in ${k2ms[$k]} ; do
                    bench $plugin $k $m encode $(($TOTAL_SIZE / $size)) $size 0 \
                        --parameter packetsize=$(packetsize $k $w $VECTOR_WORDSIZE $size) \
                        ${PARAMETERS} \
                        --parameter technique=$technique

                done
            done
        done
    done
    for technique in reed_sol_van cauchy_good ; do
        for size in $SIZES ; do
            echo "serie decode_${technique}_${size}"
            for k in $ks ; do
                for m in ${k2ms[$k]} ; do
                    echo
                    for erasures in $(seq 1 $m) ; do
                        bench $plugin $k $m decode $(($TOTAL_SIZE / $size)) $size $erasures \
                            --parameter packetsize=$(packetsize $k $w $VECTOR_WORDSIZE  $size) \
                            ${PARAMETERS} \
                            --parameter technique=$technique
                    done
                done
            done
        done
    done
}

function isa_test() {
    local plugin=isa
    local ks="2 3 4 6 10"
    declare -A k2ms
    k2ms[2]="1"
    k2ms[3]="2"
    k2ms[4]="2 3"
    k2ms[6]="2 3 4"
    k2ms[10]="3 4"
    for technique in reed_sol_van cauchy ; do
        for size in $SIZES ; do
            echo "serie encode_${technique}_${size}"
            for k in $ks ; do
                for m in ${k2ms[$k]} ; do
                    bench $plugin $k $m encode $(($TOTAL_SIZE / $size)) $size 0 \
                        --parameter technique=$technique

                done
            done
        done
    done
    for technique in reed_sol_van cauchy ; do
        for size in $SIZES ; do
            echo "serie decode_${technique}_${size}"
            for k in $ks ; do
                for m in ${k2ms[$k]} ; do
                    echo
                    for erasures in $(seq 1 $m) ; do
                        bench $plugin $k $m decode $(($TOTAL_SIZE / $size)) $size $erasures \
                            --parameter technique=$technique
                    done
                done
            done
        done
    done
}
function fplot() {
    local plugin=$1
    local serie
    ${plugin}_test | while read seconds total plugin k m workload iteration size erasures rest ; do 
        if [ -z $seconds ] ; then
            echo null,
        elif [ $seconds = serie ] ; then
            if [ "$serie" ] ; then
                echo '];'
            fi
            local serie=`echo $total | sed 's/cauchy_\([0-9]\)/cauchy_good_\1/g'`
            echo "var $serie = ["
        else
            local x
            if [ $workload = encode ] ; then
                x=$k/$m
            else
                x=$k/$m/$erasures
            fi
            echo "[ '$x', " $(echo "( $total / 1024 / 1024 ) / $seconds" | bc -ql) " ], "
        fi
    done
    echo '];'
}

function main() {
    bench_header
    for plugin in ${PLUGINS} ; do
        ${plugin}_test || return 1
    done
}

if [ "$1" = TEST ] ; then
    set -x
    set -o functrace
    PS4=' ${FUNCNAME[0]}: $LINENO: '

    TOTAL_SIZE=1024
    SIZE=1024

    function run_test() {
        dir=/tmp/erasure-code
        rm -fr $dir
        mkdir $dir
        expected=$(cat <<EOF
plugin	k	m	work.	iter.	size	eras.
serie encode_reed_sol_van_4096
jerasure	2	1	encode	0	4096	0
jerasure	3	2	encode	0	4096	0
jerasure	4	2	encode	0	4096	0
jerasure	4	3	encode	0	4096	0
jerasure	6	2	encode	0	4096	0
jerasure	6	3	encode	0	4096	0
jerasure	6	4	encode	0	4096	0
jerasure	10	3	encode	0	4096	0
jerasure	10	4	encode	0	4096	0
serie encode_reed_sol_van_1048576
jerasure	2	1	encode	0	1048576	0
jerasure	3	2	encode	0	1048576	0
jerasure	4	2	encode	0	1048576	0
jerasure	4	3	encode	0	1048576	0
jerasure	6	2	encode	0	1048576	0
jerasure	6	3	encode	0	1048576	0
jerasure	6	4	encode	0	1048576	0
jerasure	10	3	encode	0	1048576	0
jerasure	10	4	encode	0	1048576	0
serie encode_cauchy_good_4096
jerasure	2	1	encode	0	4096	0
jerasure	3	2	encode	0	4096	0
jerasure	4	2	encode	0	4096	0
jerasure	4	3	encode	0	4096	0
jerasure	6	2	encode	0	4096	0
jerasure	6	3	encode	0	4096	0
jerasure	6	4	encode	0	4096	0
jerasure	10	3	encode	0	4096	0
jerasure	10	4	encode	0	4096	0
serie encode_cauchy_good_1048576
jerasure	2	1	encode	0	1048576	0
jerasure	3	2	encode	0	1048576	0
jerasure	4	2	encode	0	1048576	0
jerasure	4	3	encode	0	1048576	0
jerasure	6	2	encode	0	1048576	0
jerasure	6	3	encode	0	1048576	0
jerasure	6	4	encode	0	1048576	0
jerasure	10	3	encode	0	1048576	0
jerasure	10	4	encode	0	1048576	0
serie decode_reed_sol_van_4096

jerasure	2	1	decode	0	4096	1

jerasure	3	2	decode	0	4096	1
jerasure	3	2	decode	0	4096	2

jerasure	4	2	decode	0	4096	1
jerasure	4	2	decode	0	4096	2

jerasure	4	3	decode	0	4096	1
jerasure	4	3	decode	0	4096	2
jerasure	4	3	decode	0	4096	3

jerasure	6	2	decode	0	4096	1
jerasure	6	2	decode	0	4096	2

jerasure	6	3	decode	0	4096	1
jerasure	6	3	decode	0	4096	2
jerasure	6	3	decode	0	4096	3

jerasure	6	4	decode	0	4096	1
jerasure	6	4	decode	0	4096	2
jerasure	6	4	decode	0	4096	3
jerasure	6	4	decode	0	4096	4

jerasure	10	3	decode	0	4096	1
jerasure	10	3	decode	0	4096	2
jerasure	10	3	decode	0	4096	3

jerasure	10	4	decode	0	4096	1
jerasure	10	4	decode	0	4096	2
jerasure	10	4	decode	0	4096	3
jerasure	10	4	decode	0	4096	4
serie decode_reed_sol_van_1048576

jerasure	2	1	decode	0	1048576	1

jerasure	3	2	decode	0	1048576	1
jerasure	3	2	decode	0	1048576	2

jerasure	4	2	decode	0	1048576	1
jerasure	4	2	decode	0	1048576	2

jerasure	4	3	decode	0	1048576	1
jerasure	4	3	decode	0	1048576	2
jerasure	4	3	decode	0	1048576	3

jerasure	6	2	decode	0	1048576	1
jerasure	6	2	decode	0	1048576	2

jerasure	6	3	decode	0	1048576	1
jerasure	6	3	decode	0	1048576	2
jerasure	6	3	decode	0	1048576	3

jerasure	6	4	decode	0	1048576	1
jerasure	6	4	decode	0	1048576	2
jerasure	6	4	decode	0	1048576	3
jerasure	6	4	decode	0	1048576	4

jerasure	10	3	decode	0	1048576	1
jerasure	10	3	decode	0	1048576	2
jerasure	10	3	decode	0	1048576	3

jerasure	10	4	decode	0	1048576	1
jerasure	10	4	decode	0	1048576	2
jerasure	10	4	decode	0	1048576	3
jerasure	10	4	decode	0	1048576	4
serie decode_cauchy_good_4096

jerasure	2	1	decode	0	4096	1

jerasure	3	2	decode	0	4096	1
jerasure	3	2	decode	0	4096	2

jerasure	4	2	decode	0	4096	1
jerasure	4	2	decode	0	4096	2

jerasure	4	3	decode	0	4096	1
jerasure	4	3	decode	0	4096	2
jerasure	4	3	decode	0	4096	3

jerasure	6	2	decode	0	4096	1
jerasure	6	2	decode	0	4096	2

jerasure	6	3	decode	0	4096	1
jerasure	6	3	decode	0	4096	2
jerasure	6	3	decode	0	4096	3

jerasure	6	4	decode	0	4096	1
jerasure	6	4	decode	0	4096	2
jerasure	6	4	decode	0	4096	3
jerasure	6	4	decode	0	4096	4

jerasure	10	3	decode	0	4096	1
jerasure	10	3	decode	0	4096	2
jerasure	10	3	decode	0	4096	3

jerasure	10	4	decode	0	4096	1
jerasure	10	4	decode	0	4096	2
jerasure	10	4	decode	0	4096	3
jerasure	10	4	decode	0	4096	4
serie decode_cauchy_good_1048576

jerasure	2	1	decode	0	1048576	1

jerasure	3	2	decode	0	1048576	1
jerasure	3	2	decode	0	1048576	2

jerasure	4	2	decode	0	1048576	1
jerasure	4	2	decode	0	1048576	2

jerasure	4	3	decode	0	1048576	1
jerasure	4	3	decode	0	1048576	2
jerasure	4	3	decode	0	1048576	3

jerasure	6	2	decode	0	1048576	1
jerasure	6	2	decode	0	1048576	2

jerasure	6	3	decode	0	1048576	1
jerasure	6	3	decode	0	1048576	2
jerasure	6	3	decode	0	1048576	3

jerasure	6	4	decode	0	1048576	1
jerasure	6	4	decode	0	1048576	2
jerasure	6	4	decode	0	1048576	3
jerasure	6	4	decode	0	1048576	4

jerasure	10	3	decode	0	1048576	1
jerasure	10	3	decode	0	1048576	2
jerasure	10	3	decode	0	1048576	3

jerasure	10	4	decode	0	1048576	1
jerasure	10	4	decode	0	1048576	2
jerasure	10	4	decode	0	1048576	3
jerasure	10	4	decode	0	1048576	4
EOF
)
        test "$(PLUGINS=jerasure main | cut --fields=3-9 )" = "$expected" || return 1
    }

    run_test

elif [ "$1" = fplot ] ; then
    "$@"
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
