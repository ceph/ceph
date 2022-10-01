#!/usr/bin/env bash
set -ex

what="$1"
[ -z "$what" ] && what=/etc/udev/rules.d
sudo ceph-post-file -d ceph-test-workunit $what

echo OK
