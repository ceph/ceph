#!/bin/bash 
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
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
#  CEPH_ERASURE_CODE_BENCHMARK=src/ceph_erasure_code_benchmark  \
#  PLUGIN_DIRECTORY=src/.libs \
#      qa/workunits/erasure-code/bench.sh fplot jerasure |
#      tee qa/workunits/erasure-code/bench.js
#
# This should start immediately and display:
#
# ...
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
#  TOTAL_SIZE=$((4 * 1024 * 1024 * 1024)) \
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
: ${PLUGINS:=isa jerasure_generic jerasure_sse4}
: ${TECHNIQUES:=vandermonde cauchy}
: ${TOTAL_SIZE:=$((1024 * 1024))}
: ${SIZE:=4096}
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
        --erasure-code-dir $PLUGIN_DIRECTORY)
    result=$($command "$@")
    echo -e "$result\t$plugin\t$k\t$m\t$workload\t$iterations\t$size\t$erasures\t$command ""$@"
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

function bench_run() {
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
    local isa2technique_vandermonde='reed_sol_van'
    local isa2technique_cauchy='cauchy'
    local jerasure_generic2technique_vandermonde='reed_sol_van'
    local jerasure_generic2technique_cauchy='cauchy_good'
    local jerasure_sse42technique_vandermonde='reed_sol_van'
    local jerasure_sse42technique_cauchy='cauchy_good'
    for technique in ${TECHNIQUES} ; do
        for plugin in ${PLUGINS} ; do
            eval technique_parameter=\$${plugin}2technique_${technique}
            echo "serie encode_${technique}_${plugin}"
            for k in $ks ; do
                for m in ${k2ms[$k]} ; do
                    bench $plugin $k $m encode $(($TOTAL_SIZE / $SIZE)) $SIZE 0 \
                        --parameter packetsize=$(packetsize $k $w $VECTOR_WORDSIZE $SIZE) \
                        ${PARAMETERS} \
                        --parameter technique=$technique_parameter

                done
            done
        done
    done
    for technique in ${TECHNIQUES} ; do
        for plugin in ${PLUGINS} ; do
            eval technique_parameter=\$${plugin}2technique_${technique}
            echo "serie decode_${technique}_${plugin}"
            for k in $ks ; do
                for m in ${k2ms[$k]} ; do
                    echo
                    for erasures in $(seq 1 $m) ; do
                        bench $plugin $k $m decode $(($TOTAL_SIZE / $SIZE)) $SIZE $erasures \
                            --parameter packetsize=$(packetsize $k $w $VECTOR_WORDSIZE  $SIZE) \
                            ${PARAMETERS} \
                            --parameter technique=$technique_parameter
                    done
                done
            done
        done
    done
}

function fplot() {
    local serie
    bench_run | while read seconds total plugin k m workload iteration size erasures rest ; do 
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
    bench_run
}

if [ "$1" = fplot ] ; then
    "$@"
else
    main
fi
# Local Variables:
# compile-command: "\
#   CEPH_ERASURE_CODE_BENCHMARK=../../../src/ceph_erasure_code_benchmark \
#   PLUGIN_DIRECTORY=../../../src/.libs \
#   ./bench.sh
# "
# End:
