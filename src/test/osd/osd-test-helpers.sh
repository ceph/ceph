#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

function run_osd() {
    local dir=$1
    shift
    local id=$1
    shift
    local osd_data=$dir/$id

    local ceph_disk_args
    ceph_disk_args+=" --statedir=$dir"
    ceph_disk_args+=" --sysconfdir=$dir"
    ceph_disk_args+=" --prepend-to-path="
    ceph_disk_args+=" --verbose"

    touch $dir/ceph.conf

    ./ceph-disk $ceph_disk_args \
        prepare $osd_data || return 1

    local ceph_args="$CEPH_ARGS"
    ceph_args+=" --osd-journal-size=100"
    ceph_args+=" --osd-data=$osd_data"
    ceph_args+=" --chdir="
    ceph_args+=" --run-dir=$dir"
    ceph_args+=" --debug-osd=20"
    ceph_args+=" --log-file=$dir/osd-\$id.log"
    ceph_args+=" --pid-file=$dir/osd-\$id.pidfile"
    ceph_args+=" "
    ceph_args+="$@"
    CEPH_ARGS="$ceph_args" ./ceph-disk $ceph_disk_args \
        activate \
        --mark-init=none \
        $osd_data || return 1

    [ "$id" = "$(cat $osd_data/whoami)" ] || return 1

    ./ceph osd crush create-or-move "$id" 1 root=default host=localhost
}
