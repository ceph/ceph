#!/bin/sh

suite=$1
ceph=$2
kernel=$3
email=$4

if [ -z "$email" ]; then
    echo "usage: $0 <suite> <ceph branch> <kernel branch> <email>"
    exit 1
fi

stamp=`date +%Y-%m-%d_%H:%M:%S`

fn="/tmp/schedule.suite.$$"

cat <<EOF > $fn
kernel:
  branch: $kernel
nuke-on-error: true
overrides:
  ceph:
    btrfs: 1
    log-whitelist:
    - clocks not synchronized
    - slow request
    branch: $ceph
tasks:
- chef:
EOF

bin/teuthology-suite -v $fn --collections /home/sage/src/ceph-qa-suite/suites/$suite/* --email $email --timeout 21600 --name $ceph-$stamp 

rm $fn
