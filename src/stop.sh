#!/bin/bash
#
# Copyright (C) 2013 Inktank <info@inktank.com>
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

test -d dev/osd0/. && test -e dev/sudo && SUDO="sudo"

[ -z "$CEPH_BIN" ] && CEPH_BIN=.

do_killall() {
    pg=`pgrep -f ceph-run.*$1`
    [ -n "$pg" ] && kill $pg
    $SUDO killall $1
}

usage="usage: $0 [all] [mon] [mds] [osd]\n"

stop_all=1
stop_mon=0
stop_mds=0
stop_osd=0
stop_rgw=0

while [ $# -ge 1 ]; do
    case $1 in
        all )
            stop_all=1
            ;;
        mon | ceph-mon )
            stop_mon=1
            stop_all=0
            ;;
        mds | ceph-mds )
            stop_mds=1
            stop_all=0
            ;;
        osd | ceph-osd )
            stop_osd=1
            stop_all=0
            ;;
        * )
            printf "$usage"
            exit
    esac
    shift
done

if [ $stop_all -eq 1 ]; then
    while read DEV; do
        # While it is currently possible to create an rbd image with
        # whitespace chars in its name, krbd will refuse mapping such
        # an image, so we can safely split on whitespace here.  (The
        # same goes for whitespace chars in names of the pools that
        # contain rbd images).
        DEV="$(echo "${DEV}" | tr -s '[:space:]' | awk '{ print $5 }')"
        sudo "${CEPH_BIN}"/rbd unmap "${DEV}"
    done < <("${CEPH_BIN}"/rbd showmapped | tail -n +2)

    if [ -n "$("${CEPH_BIN}"/rbd showmapped)" ]; then
        echo "WARNING: Some rbd images are still mapped!" >&2
    fi

    for p in ceph-mon ceph-mds ceph-osd radosgw lt-radosgw apache2 ; do
        for try in 0 1 1 1 1 ; do
            if ! pkill $p ; then
                break
            fi
            sleep $try
        done
    done

    pkill -f valgrind.bin.\*ceph-mon
    $SUDO pkill -f valgrind.bin.\*ceph-osd
    pkill -f valgrind.bin.\*ceph-mds
else
    [ $stop_mon -eq 1 ] && do_killall ceph-mon
    [ $stop_mds -eq 1 ] && do_killall ceph-mds
    [ $stop_osd -eq 1 ] && do_killall ceph-osd
    [ $stop_rgw -eq 1 ] && do_killall radosgw lt-radosgw apache2
fi
