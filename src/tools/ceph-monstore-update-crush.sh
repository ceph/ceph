#!/bin/bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
# Author: Kefu Chai <kchai@redhat.com>
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

verbose=

test -d ../src && export PATH=$PATH:.

if type xmlstarlet > /dev/null 2>&1; then
    XMLSTARLET=xmlstarlet
elif type xml > /dev/null 2>&1; then
    XMLSTARLET=xml
else
    echo "Missing xmlstarlet binary!"
    exit 1
fi

function osdmap_get() {
    local store_path=$1
    local query=$2
    local epoch=${3:+-v $3}
    local osdmap=`mktemp`

    ceph-monstore-tool $store_path get osdmap -- \
                       $epoch -o $osdmap > /dev/null || return

    echo $(osdmaptool --dump xml $osdmap 2> /dev/null | \
           $XMLSTARLET sel -t -m "$query" -v .)

    rm -f $osdmap
}

function test_crush() {
    local store_path=$1
    local epoch=$2
    local max_osd=$3
    local crush=$4
    local osdmap=`mktemp`

    ceph-monstore-tool $store_path get osdmap -- \
                       -v $epoch -o $osdmap > /dev/null
    osdmaptool --export-crush $crush $osdmap &> /dev/null

    if crushtool --test --check $max_osd -i $crush > /dev/null; then
        good=true
    else
        good=false
    fi
    rm -f $osdmap
    $good || return 1
}

function die() {
    local retval=$?
    echo "$@" >&2
    exit $retval
}

function usage() {
    [ $# -gt 0 ] && echo -e "\n$@"
    cat <<EOF

Usage: $0 [options ...] <mon-store>

Search backward for a latest known-good epoch in monstore. Rewrite the osdmap
epochs after it with the crush map in the found epoch if asked to do so. By
default, print out the crush map in the good epoch.

  [-h|--help]            display this message
  [--out]                write the found crush map to given file (default: stdout)
  [--rewrite]            rewrite the monitor storage with the found crush map
  [--verbose]            be more chatty
EOF
    [ $# -gt 0 ] && exit 1
    exit 0
}

function main() {
    local temp
    temp=$(getopt -o h --long verbose,help,mon-store:,out:,rewrite -n $0 -- "$@") || return 1

    eval set -- "$temp"
    local rewrite
    while [ "$1" != "--" ]; do
        case "$1" in
            --verbose)
                verbose=true
                # set -xe
                # PS4='${FUNCNAME[0]}: $LINENO: '
                shift;;
            -h|--help)
                usage
                return 0;;
            --out)
                output=$2
                shift 2;;
            --osdmap-epoch)
                osdmap_epoch=$2
                shift 2;;
            --rewrite)
                rewrite=true
                shift;;
            *)
                usage "unexpected argument $1"
                shift;;
        esac
    done
    shift

    local store_path="$1"
    test $store_path || usage "I need the path to mon-store."

    # try accessing the store; if it fails, likely means a mon is running
    local last_osdmap_epoch
    local max_osd
    last_osdmap_epoch=$(osdmap_get $store_path "/osdmap/epoch") || \
        die "error accessing mon store at $store_path"
    # get the max_osd # in last osdmap epoch, crushtool will use it to check
    # the crush maps in previous osdmaps
    max_osd=$(osdmap_get $store_path "/osdmap/max_osd" $last_osdmap_epoch)

    local good_crush
    local good_epoch
    test $verbose && echo "the latest osdmap epoch is $last_osdmap_epoch"
    for epoch in `seq $last_osdmap_epoch -1 1`; do
        local crush_path=`mktemp`
        test $verbose && echo "checking crush map #$epoch"
        if test_crush $store_path $epoch $max_osd $crush_path; then
            test $verbose && echo "crush map version #$epoch works with osdmap epoch #$osdmap_epoch"
            good_epoch=$epoch
            good_crush=$crush_path
            break
        fi
        rm -f $crush_path
    done

    if test $good_epoch; then
        echo "good crush map found at epoch $epoch/$last_osdmap_epoch"
    else
        echo "Unable to find a crush map for osdmap version #$osdmap_epoch." 2>&1
        return 1
    fi

    if test $good_epoch -eq $last_osdmap_epoch; then
        echo "and mon store has no faulty crush maps."
    elif test $output; then
        crushtool --decompile $good_crush --outfn $output
    elif test $rewrite; then
        ceph-monstore-tool $store_path rewrite-crush --  \
                           --crush $good_crush      \
                           --good-epoch $good_epoch
    else
        echo
        crushtool --decompile $good_crush
    fi
    rm -f $good_crush
}

main "$@"
