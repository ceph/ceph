#!/bin/bash
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Sebastien Ponce <sebastien.ponce@cern.ch>
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

    export CEPH_MON="127.0.0.1:7116" # git grep '\<7116\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    # setup
    setup $dir || return 1

    # create a cluster with one monitor and three osds
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    # create toyfile
    dd if=/dev/urandom of=$dir/toyfile bs=1234 count=1
    
    # put a striped object
    rados --pool rbd --striper put toyfile $dir/toyfile || return 1
    
    # stat it, with and without striping
    rados --pool rbd --striper stat toyfile | cut -d ',' -f 2 > $dir/stripedStat || return 1
    rados --pool rbd stat toyfile.0000000000000000 | cut -d ',' -f 2 > $dir/stat || return 1
    echo ' size 1234' > $dir/refstat
    diff -w $dir/stripedStat $dir/refstat || return 1
    diff -w $dir/stat $dir/refstat || return 1
    rados --pool rbd stat toyfile >& $dir/staterror
    grep -q 'No such file or directory' $dir/staterror ||  return 1
    
    # get the file back with and without striping
    rados --pool rbd --striper get toyfile $dir/stripedGroup || return 1
    diff -w $dir/toyfile $dir/stripedGroup || return 1
    rados --pool rbd get toyfile.0000000000000000 $dir/nonSTripedGroup || return 1
    diff -w $dir/toyfile $dir/nonSTripedGroup || return 1
    
    # test truncate
    rados --pool rbd --striper truncate toyfile 12
    rados --pool rbd --striper stat toyfile | cut -d ',' -f 2 > $dir/stripedStat || return 1
    rados --pool rbd stat toyfile.0000000000000000 | cut -d ',' -f 2 > $dir/stat || return 1
    echo ' size 12' > $dir/reftrunc
    diff -w $dir/stripedStat $dir/reftrunc || return 1
    diff -w $dir/stat $dir/reftrunc || return 1
    
    # test xattrs

    rados --pool rbd --striper setxattr toyfile somexattr somevalue || return 1
    rados --pool rbd --striper getxattr toyfile somexattr > $dir/xattrvalue || return 1 
    rados --pool rbd getxattr toyfile.0000000000000000 somexattr > $dir/xattrvalue2 || return 1 
    echo 'somevalue' > $dir/refvalue
    diff -w $dir/xattrvalue $dir/refvalue || return 1
    diff -w $dir/xattrvalue2 $dir/refvalue || return 1
    rados --pool rbd --striper listxattr toyfile > $dir/xattrlist || return 1
    echo 'somexattr' > $dir/reflist
    diff -w $dir/xattrlist $dir/reflist || return 1
    rados --pool rbd listxattr toyfile.0000000000000000 | grep -v striper > $dir/xattrlist2 || return 1
    diff -w $dir/xattrlist2 $dir/reflist || return 1    
    rados --pool rbd --striper rmxattr toyfile somexattr || return 1

    local attr_not_found_str="No data available"
    [ `uname` = FreeBSD ] && \
        attr_not_found_str="Attribute not found"
    expect_failure $dir "$attr_not_found_str"  \
        rados --pool rbd --striper getxattr toyfile somexattr || return 1
    expect_failure $dir "$attr_not_found_str"  \
        rados --pool rbd getxattr toyfile.0000000000000000 somexattr || return 1
    
    # test rm
    rados --pool rbd --striper rm toyfile || return 1
    expect_failure $dir 'No such file or directory' \
        rados --pool rbd --striper stat toyfile  || return 1
    expect_failure $dir 'No such file or directory' \
        rados --pool rbd stat toyfile.0000000000000000 || return 1

    # cleanup
    teardown $dir || return 1
}

main rados-striper "$@"
