#!/usr/bin/bash
set -e

let -i actual=$(ls $CEPH_MNT/$1/.snap | wc -l)
let -i min_expected=$2

[ $actual -ge $min_expected ] && echo "OK." || false
