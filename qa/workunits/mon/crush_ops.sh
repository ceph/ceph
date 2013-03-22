#!/bin/sh -x

set -e

ceph osd crush dump

# rules
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

# build a simple map
ceph osd crush add-bucket foo root
o1=`ceph osd create`
o2=`ceph osd create`
ceph osd crush add $o1 1 host=host1 root=foo
ceph osd crush add $o2 1 host=host2 root=foo
ceph osd crush add-bucket bar root
ceph osd crush link host1 root=bar
ceph osd crush link host2 root=bar

ceph osd tree | grep -c osd.$o1 | grep -q 2
ceph osd tree | grep -c host1 | grep -q 2
ceph osd tree | grep -c osd.$o2 | grep -q 2
ceph osd tree | grep -c host2 | grep -q 2
ceph osd crush rm host1 foo && exit 1 || true   # not empty
ceph osd crush unlink host1 foo
ceph osd crush unlink host1 foo && exit 1 || true # not there anymore
ceph osd tree | grep -c host1 | grep -q 1

ceph osd crush rm foo && exit 1 || true
ceph osd crush rm bar && exit 1 || true
ceph osd crush unlink host1 bar
ceph osd tree | grep -c host1 | grep -q 1   # now an orphan
ceph osd crush rm osd.1 host1
ceph osd crush rm host1
ceph osd tree | grep -c host1 | grep -q 0

ceph osd crush rm bar && exit 1 || true
ceph osd crush unlink host2
ceph osd crush rm bar
ceph osd crush rm foo
ceph osd crush rm osd.$o2 host2
ceph osd crush rm host2
ceph osd crush rm osd.$o1
ceph osd crush rm osd.$o2
ceph osd crush rm osd.$o2 && exti 1 || true

echo OK
