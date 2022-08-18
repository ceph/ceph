#!/bin/bash -ex

ceph osd pool autoscale-status

for f in `ceph osd pool ls`
do
  echo $f
  ceph osd pool autoscale-status | grep $f | grep -o -m 1 'on\|off\|warn' || true
done
