#!/bin/bash -ex

what="$1"
[ -z "$what" ] && what=/etc/udev/rules.d
ceph-post-file -d ceph-test-workunit $what

echo OK
