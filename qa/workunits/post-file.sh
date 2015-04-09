#!/bin/bash -ex

what="$1"
[ -z "$what" ] && what=/etc/default
ceph-post-file -d ceph-test-workunit $what

echo OK
