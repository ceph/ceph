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


export CEPH_DIR="$PWD/"
export CEPH_DEV_DIR="$CEPH_DIR/test_dev"
export CEPH_OUT_DIR="$CEPH_DIR/test_out"

function vstart_teardown()
{
    ./stop.sh
}

function vstart_setup()
{
    rm -fr $CEPH_DEV_DIR $CEPH_OUT_DIR
    mkdir -p $CEPH_DEV_DIR
    trap "vstart_teardown ; rm -f $TMPFILE" EXIT
    export LC_ALL=C # some tests are vulnerable to i18n
    MON=1 OSD=3 ./vstart.sh \
        -o 'paxos propose interval = 0.01' \
        -n -X -l mon osd || return 1
    export PATH=.:$PATH
    export CEPH_CONF=ceph.conf

    crit=$(expr 100 - $(ceph-conf --show-config-value mon_data_avail_crit))
    if [ $(df . | perl -ne 'print if(s/.*\s(\d+)%.*/\1/)') -ge $crit ] ; then
        df . 
        cat <<EOF
error: not enough free disk space for mon to run
The mon will shutdown with a message such as 
 "reached critical levels of available space on local monitor storage -- shutdown!"
as soon as it finds the disk has is more than ${crit}% full. 
This is a limit determined by
 ceph-conf --show-config-value mon_data_avail_crit
EOF
        return 1
    fi
}

function main()
{
    if [[ $(pwd) =~ /src$ ]] && [ -d .libs ] && [ -d pybind ] ; then
        vstart_setup || return 1
    else
        trap "rm -f $TMPFILE" EXIT
    fi
    
    "$@" || return 1
}

main "$@"
