#!/usr/bin/env bash
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
#  PLUGIN_DIRECTORY=build/lib \
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
#  PLUGIN_DIRECTORY=build/lib \
#      qa/workunits/erasure-code/bench.sh fplot jerasure |
#      tee qa/workunits/erasure-code/bench.js
#
set -e

export PATH=/sbin:$PATH

: ${VERBOSE:=false}
: ${CEPH_ERASURE_CODE_BENCHMARK:=ceph_erasure_code_benchmark}
: ${PLUGIN_DIRECTORY:=/usr/lib/ceph/erasure-code}
: ${PLUGINS:=isa jerasure}
: ${TECHNIQUES:=vandermonde cauchy liberation reed_sol_r6_op blaum_roth liber8tion}
: ${TOTAL_SIZE:=$((1024 * 1024))}
: ${SIZE:=4096}
: ${PARAMETERS:=--parameter jerasure-per-chunk-alignment=true}

declare -rA isa_techniques=(
    [vandermonde]="reed_sol_van"
    [cauchy]="cauchy"
)

declare -rA jerasure_techniques=(
    [vandermonde]="reed_sol_van"
    [cauchy]="cauchy_good"
    [reed_sol_r6_op]="reed_sol_r6_op"
    [blaum_roth]="blaum_roth"
    [liberation]="liberation"
    [liber8tion]="liber8tion"
)

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

function get_technique_name()
{
    local plugin=$1
    local technique=$2

    declare -n techniques="${plugin}_techniques"
    echo ${techniques["$technique"]}
}

function technique_is_raid6() {
    local technique=$1
    local r6_techniques="liberation reed_sol_r6_op blaum_roth liber8tion"

    if [[ $r6_techniques =~ $technique ]]; then
        return 0
    fi
    return 1
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

    for technique in ${TECHNIQUES} ; do
        for plugin in ${PLUGINS} ; do
            technique_parameter=$(get_technique_name $plugin $technique)
            if [[ -z $technique_parameter ]]; then continue; fi
            echo "serie encode_${technique}_${plugin}"
            for k in $ks ; do
                for m in ${k2ms[$k]} ; do
                    if [ $m -ne 2 ] && technique_is_raid6 $technique; then continue; fi
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
            technique_parameter=$(get_technique_name $plugin $technique)
            if [[ -z $technique_parameter ]]; then continue; fi
            echo "serie decode_${technique}_${plugin}"
            for k in $ks ; do
                for m in ${k2ms[$k]} ; do
                    if [ $m -ne 2 ] && technique_is_raid6 $technique; then continue; fi
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
    local serie=""
    local plot=""
    local encode_table="var encode_table = [\n"
    local decode_table="var decode_table = [\n"
    while read seconds total plugin k m workload iteration size erasures rest ; do
        if [ -z $seconds ] ; then
            plot="$plot  null,\n"
        elif [ $seconds = serie ] ; then
            if [ "$serie" ] ; then
                echo -e "$plot];\n"
            fi
            local serie=`echo $total | sed 's/cauchy_\([0-9]\)/cauchy_good_\1/g'`
            plot="var $serie = [\n"
        else
            local x
            local row
            local technique=`echo $rest | grep -Po "(?<=technique=)\w*"`
            local packetsize=`echo $rest | grep -Po "(?<=packetsize=)\w*"`
            if [ $workload = encode ] ; then
                x=$k/$m
                row="[ '$plugin', '$technique', $seconds, $total, $k, $m, $iteration, $packetsize ],"
                encode_table="$encode_table  $row\n"

            else
                x=$k/$m/$erasures
                row="[ '$plugin', '$technique', $seconds, $total, $k, $m, $iteration, $packetsize, $erasures ],"
                decode_table="$decode_table  $row\n"
            fi
            local out_time="$(echo "( $total / 1024 / 1024 ) / $seconds" | bc -ql)"
            plot="$plot  [ '$x', $out_time ],\n"
        fi
    done < <(bench_run)

    echo -e "$plot];\n"
    echo -e "$encode_table];\n"
    echo -e "$decode_table];\n"
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
#   PLUGIN_DIRECTORY=../../../build/lib \
#   ./bench.sh
# "
# End:
