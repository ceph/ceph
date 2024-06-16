#!/bin/sh -ex

bin/ceph mon rm `hostname`
for f in `bin/ceph orch ls | grep -v NAME | awk '{print $1}'` ; do
    bin/ceph orch rm $f --force
done
