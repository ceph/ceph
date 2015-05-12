#!/bin/bash -x

set -e

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

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
expect_false ceph osd crush rule rm replicated_ruleset

# build a simple map
expect_false ceph osd crush add-bucket foo osd
ceph osd crush add-bucket foo root
o1=`ceph osd create`
o2=`ceph osd create`
ceph osd crush add $o1 1 host=host1 root=foo
ceph osd crush add $o1 1 host=host1 root=foo  # idemptoent
ceph osd crush add $o2 1 host=host2 root=foo
ceph osd crush add $o2 1 host=host2 root=foo  # idempotent
ceph osd crush add-bucket bar root
ceph osd crush add-bucket bar root  # idempotent
ceph osd crush link host1 root=bar
ceph osd crush link host1 root=bar  # idempotent
ceph osd crush link host2 root=bar
ceph osd crush link host2 root=bar  # idempotent

ceph osd tree | grep -c osd.$o1 | grep -q 2
ceph osd tree | grep -c host1 | grep -q 2
ceph osd tree | grep -c osd.$o2 | grep -q 2
ceph osd tree | grep -c host2 | grep -q 2
expect_false ceph osd crush rm host1 foo   # not empty
ceph osd crush unlink host1 foo
ceph osd crush unlink host1 foo
ceph osd tree | grep -c host1 | grep -q 1

expect_false ceph osd crush rm foo  # not empty
expect_false ceph osd crush rm bar  # not empty
ceph osd crush unlink host1 bar
ceph osd tree | grep -c host1 | grep -q 1   # now an orphan
ceph osd crush rm osd.$o1 host1
ceph osd crush rm host1
ceph osd tree | grep -c host1 | grep -q 0

expect_false ceph osd crush rm bar   # not empty
ceph osd crush unlink host2

# reference foo and bar with a rule
ceph osd crush rule create-simple foo-rule foo host firstn
expect_false ceph osd crush rm foo
ceph osd crush rule rm foo-rule

ceph osd crush rm bar
ceph osd crush rm foo
ceph osd crush rm osd.$o2 host2
ceph osd crush rm host2

ceph osd crush rm osd.$o1
ceph osd crush rm osd.$o2

ceph osd crush add-bucket foo host
ceph osd crush move foo root=default rack=localrack
ceph osd crush rm foo

# test reweight
o3=`ceph osd create`
ceph osd crush add $o3 123 root=default
ceph osd tree | grep osd.$o3 | grep 123
ceph osd crush reweight osd.$o3 113
ceph osd tree | grep osd.$o3 | grep 113
ceph osd crush rm osd.$o3
ceph osd rm osd.$o3

echo OK
