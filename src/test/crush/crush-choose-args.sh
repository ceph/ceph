#!/bin/bash
#
# Copyright (C) 2017 Red Hat <contact@redhat.com>
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

source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7131" # git grep '\<7131\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_choose_args() {
    local dir=$1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1

    ceph osd getcrushmap > $dir/map || return 1
    crushtool -d $dir/map -o $dir/map.txt || return 1
    cat $dir/map.txt
    sed -i -e '/end crush map/d' $dir/map.txt
    cat >> $dir/map.txt <<EOF
# choose_args
choose_args 0 {
  {
    bucket_id -2
    weight_set [
      [ 1.000 ]
      [ 1.000 ]
    ]
    ids [ -20 ]
  }
}

# end crush map
EOF
    crushtool -c $dir/map.txt -o $dir/map-new || return 1
    ceph osd setcrushmap -i $dir/map-new || return 1

    run_osd $dir 1 || return 1
    ceph osd getcrushmap > $dir/map-one-more || return 1
    crushtool -d $dir/map-one-more -o $dir/map-one-more.txt || return 1
    cat $dir/map-one-more.txt
    diff -u $dir/map-one-more.txt $CEPH_ROOT/src/test/crush/crush-choose-args-expected-one-more.txt || return 1

    destroy_osd $dir 1 || return 1
    ceph osd getcrushmap > $dir/map-one-less || return 1
    crushtool -d $dir/map-one-less -o $dir/map-one-less.txt || return 1
    diff -u $dir/map-one-less.txt $dir/map.txt || return 1    
}

main crush-choose-args "$@"

# Local Variables:
# compile-command: "cd ../../../build ; ln -sf ../src/ceph-disk/ceph_disk/main.py bin/ceph-disk && make -j4 && ../src/test/crush/crush-choose-args.sh"
# End:
