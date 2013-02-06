#!/bin/sh -x

set -e

ceph osd crush dump
ceph osd crush rule dump
ceph osd crush rule ls
ceph osd crush rule list

ceph osd crush rule create-simple foo default host
ceph osd crush rule create-simple foo default host
ceph osd crush rule create-simple bar default host

ceph osd crush rule ls | grep foo

ceph osd crush rule rm foo
ceph osd crush rule rm foo  # idempotent
ceph osd crush rule rm bar

# can't delete in-use rules, tho:
ceph osd crush rule rm data && exit 1 || true

echo OK
