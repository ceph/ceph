#!/usr/bin/env bash
#
# This script uses ceph_erasure_code_non_regression to encode
# data with a specified set of plugins, techniques and stripe widths.
# md5sums of each chunk are calculated and added to the output file
# so that the output of each plugin can be inspected and compared.
#
# An example invocation:
#
#  PLUGIN_DIRECTORY=/ceph/build/lib \
#  CEPH_ERASURE_CODE_NON_REGRESSION=/ceph/build/bin/ceph_erasure_code_non_regression \
#  K=3 M=1 STRIPE_WIDTHS="128 256" /ceph/qa/workunits/erasure-code/plugin-comparison.sh
#
# This will create an output directory called plugin_comparison and
# an output file called plugin_comparison/output
#

set -e

export PATH=/sbin:$PATH

: ${CEPH_ERASURE_CODE_NON_REGRESSION:=ceph_erasure_code_non_regression}
: ${PLUGIN_DIRECTORY:=/ceph/build/lib}
: ${OUTPUT_DIRECTORY:=plugin_comparison}
: ${OUTPUT_FILE:=output}
: ${PLUGINS:=isa jerasure}
#: ${TECHNIQUES:=vandermonde cauchy liberation reed_sol_r6_op blaum_roth liber8tion}
: ${TECHNIQUES:=vandermonde cauchy reed_sol_r6_op}
: ${STRIPE_WIDTHS:=4096 10000}
: ${K:=3}
: ${M:=1 2}

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

mkdir ${OUTPUT_DIRECTORY}

for stripe_width in ${STRIPE_WIDTHS} ; do
    for technique in ${TECHNIQUES} ; do
        for plugin in ${PLUGINS} ; do  
            technique_parameter=$(get_technique_name $plugin $technique)
            if [[ -z $technique_parameter ]]; then continue; fi
            for k in ${K} ; do
                for m in ${M} ; do
                    chunk_counter=$(($k+$m))
                    if [ $m -ne 2 ] && technique_is_raid6 $technique; then continue; fi
                    if [ $m -gt $k ]; then continue; fi
                    ${CEPH_ERASURE_CODE_NON_REGRESSION} --plugin $plugin --stripe-width $stripe_width --parameter k=$k \
                        --parameter m=$m --parameter technique=$technique_parameter --base ${OUTPUT_DIRECTORY} --create
                    for ((chunk = 0; chunk < $chunk_counter; chunk++)) do
                        md5sum ${OUTPUT_DIRECTORY}/plugin\=$plugin\ stripe-width\=$stripe_width\ k\=$k\ m\=$m\ technique\=$technique_parameter/$chunk >> ${OUTPUT_DIRECTORY}/${OUTPUT_FILE}
                    done
                    echo ' ' >> ${OUTPUT_DIRECTORY}/${OUTPUT_FILE}
                done
            done
        done
    done
done
