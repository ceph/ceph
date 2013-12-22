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
function vstart_teardown()
{
    ./stop.sh
}

function vstart_setup()
{
    rm -fr dev out
    mkdir -p dev
    trap "vstart_teardown ; rm -f $TMPFILE" EXIT
    export LC_ALL=C # some tests are vulnerable to i18n
    MON=1 OSD=3 ./vstart.sh -n -X -l mon osd
    export PATH=.:$PATH
    export CEPH_CONF=ceph.conf
}

function main()
{
    if [[ $(pwd) =~ /src$ ]] && [ -d .libs ] && [ -d pybind ] ; then
        vstart_setup
    else
        trap "rm -f $TMPFILE" EXIT
    fi
    
    "$@"
}

main "$@"
