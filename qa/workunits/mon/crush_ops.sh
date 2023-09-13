#!/usr/bin/env bash

set -ex

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

ceph osd crush rm-device-class all
ceph osd crush set-device-class ssd osd.0
ceph osd crush set-device-class hdd osd.1
ceph osd crush rule create-replicated foo-ssd default host ssd
ceph osd crush rule create-replicated foo-hdd default host hdd
ceph osd crush rule ls-by-class ssd | grep 'foo-ssd'
ceph osd crush rule ls-by-class ssd | expect_false grep 'foo-hdd'
ceph osd crush rule ls-by-class hdd | grep 'foo-hdd'
ceph osd crush rule ls-by-class hdd | expect_false grep 'foo-ssd'

ceph osd erasure-code-profile set ec-foo-ssd crush-device-class=ssd m=2 k=2
ceph osd pool create ec-foo 2 erasure ec-foo-ssd
ceph osd pool rm ec-foo ec-foo --yes-i-really-really-mean-it

ceph osd crush rule ls | grep foo

ceph osd crush rule rename foo foo-asdf
ceph osd crush rule rename foo foo-asdf # idempotent
ceph osd crush rule rename bar bar-asdf
ceph osd crush rule ls | grep 'foo-asdf'
ceph osd crush rule ls | grep 'bar-asdf'
ceph osd crush rule rm foo 2>&1 | grep 'does not exist'
ceph osd crush rule rm bar 2>&1 | grep 'does not exist'
ceph osd crush rule rename foo-asdf foo
ceph osd crush rule rename foo-asdf foo # idempotent
ceph osd crush rule rename bar-asdf bar
ceph osd crush rule ls | expect_false grep 'foo-asdf'
ceph osd crush rule ls | expect_false grep 'bar-asdf'
ceph osd crush rule rm foo
ceph osd crush rule rm foo  # idempotent
ceph osd crush rule rm bar

# can't delete in-use rules, tho:
ceph osd pool create pinning_pool 1
expect_false ceph osd crush rule rm replicated_rule
ceph osd pool rm pinning_pool pinning_pool --yes-i-really-really-mean-it

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
expect_false ceph osd tree-from host1
ceph osd tree-from host2
expect_false ceph osd tree-from osd.$o2

expect_false ceph osd crush rm bar   # not empty
ceph osd crush unlink host2

ceph osd crush add-bucket host-for-test host root=root-for-test rack=rack-for-test
ceph osd tree | grep host-for-test
ceph osd tree | grep rack-for-test
ceph osd tree | grep root-for-test
ceph osd crush rm host-for-test
ceph osd crush rm rack-for-test
ceph osd crush rm root-for-test

# reference foo and bar with a rule
ceph osd crush rule create-simple foo-rule foo host firstn
expect_false ceph osd crush rm foo
ceph osd crush rule rm foo-rule

ceph osd crush rm bar
ceph osd crush rm foo
ceph osd crush rm osd.$o2 host2
ceph osd crush rm host2

ceph osd crush add-bucket foo host
ceph osd crush move foo root=default rack=localrack

ceph osd crush create-or-move osd.$o1 1.0 root=default
ceph osd crush move osd.$o1 host=foo
ceph osd find osd.$o1 | grep host | grep foo

ceph osd crush rm osd.$o1
ceph osd crush rm osd.$o2

ceph osd crush rm foo

# test reweight
o3=`ceph osd create`
ceph osd crush add $o3 123 root=default
ceph osd tree | grep osd.$o3 | grep 123
ceph osd crush reweight osd.$o3 113
expect_false ceph osd crush reweight osd.$o3 123456
ceph osd tree | grep osd.$o3 | grep 113
ceph osd crush rm osd.$o3
ceph osd rm osd.$o3

# test reweight-subtree
o4=`ceph osd create`
o5=`ceph osd create`
ceph osd crush add $o4 123 root=default host=foobaz
ceph osd crush add $o5 123 root=default host=foobaz
ceph osd tree | grep osd.$o4 | grep 123
ceph osd tree | grep osd.$o5 | grep 123
ceph osd crush reweight-subtree foobaz 155
expect_false ceph osd crush reweight-subtree foobaz 123456
ceph osd tree | grep osd.$o4 | grep 155
ceph osd tree | grep osd.$o5 | grep 155
ceph osd crush rm osd.$o4
ceph osd crush rm osd.$o5
ceph osd rm osd.$o4
ceph osd rm osd.$o5

# weight sets
# make sure we require luminous before testing weight-sets
ceph osd set-require-min-compat-client luminous
ceph osd crush weight-set dump
ceph osd crush weight-set ls
expect_false ceph osd crush weight-set reweight fooset osd.0 .9
ceph osd pool create fooset 8
ceph osd pool create barset 8
ceph osd pool set barset size 3
expect_false ceph osd crush weight-set reweight fooset osd.0 .9
ceph osd crush weight-set create fooset flat
ceph osd crush weight-set create barset positional
ceph osd crush weight-set ls | grep fooset
ceph osd crush weight-set ls | grep barset
ceph osd crush weight-set dump
ceph osd crush weight-set reweight fooset osd.0 .9
expect_false ceph osd crush weight-set reweight fooset osd.0 .9 .9
expect_false ceph osd crush weight-set reweight barset osd.0 .9
ceph osd crush weight-set reweight barset osd.0 .9 .9 .9
ceph osd crush weight-set ls | grep -c fooset | grep -q 1
ceph osd crush weight-set rm fooset
ceph osd crush weight-set ls | grep -c fooset | grep -q 0
ceph osd crush weight-set ls | grep barset
ceph osd crush weight-set rm barset
ceph osd crush weight-set ls | grep -c barset | grep -q 0
ceph osd crush weight-set create-compat
ceph osd crush weight-set ls | grep '(compat)'
ceph osd crush weight-set rm-compat

# weight set vs device classes
ceph osd pool create cool 2
ceph osd pool create cold 2
ceph osd pool set cold size 2
ceph osd crush weight-set create-compat
ceph osd crush weight-set create cool flat
ceph osd crush weight-set create cold positional
ceph osd crush rm-device-class osd.0
ceph osd crush weight-set reweight-compat osd.0 10.5
ceph osd crush weight-set reweight cool osd.0 11.5
ceph osd crush weight-set reweight cold osd.0 12.5 12.4
ceph osd crush set-device-class fish osd.0
ceph osd crush tree --show-shadow | grep osd\\.0 | grep fish | grep 10\\.
ceph osd crush tree --show-shadow | grep osd\\.0 | grep fish | grep 11\\.
ceph osd crush tree --show-shadow | grep osd\\.0 | grep fish | grep 12\\.
ceph osd crush rm-device-class osd.0
ceph osd crush set-device-class globster osd.0
ceph osd crush tree --show-shadow | grep osd\\.0 | grep globster | grep 10\\.
ceph osd crush tree --show-shadow | grep osd\\.0 | grep globster | grep 11\\.
ceph osd crush tree --show-shadow | grep osd\\.0 | grep globster | grep 12\\.
ceph osd crush weight-set reweight-compat osd.0 7.5
ceph osd crush weight-set reweight cool osd.0 8.5
ceph osd crush weight-set reweight cold osd.0 6.5 6.6
ceph osd crush tree --show-shadow | grep osd\\.0 | grep globster | grep 7\\.
ceph osd crush tree --show-shadow | grep osd\\.0 | grep globster | grep 8\\.
ceph osd crush tree --show-shadow | grep osd\\.0 | grep globster | grep 6\\.
ceph osd crush rm-device-class osd.0
ceph osd pool rm cool cool --yes-i-really-really-mean-it
ceph osd pool rm cold cold --yes-i-really-really-mean-it
ceph osd crush weight-set rm-compat

# weight set vs device classes vs move
ceph osd crush weight-set create-compat
ceph osd crush add-bucket fooo host
ceph osd crush move fooo root=default
ceph osd crush add-bucket barr rack
ceph osd crush move barr root=default
ceph osd crush move fooo rack=barr
ceph osd crush rm fooo
ceph osd crush rm barr
ceph osd crush weight-set rm-compat

# this sequence would crash at one point
ceph osd crush weight-set create-compat
ceph osd crush add-bucket r1 rack root=default
for f in `seq 1 32`; do
    ceph osd crush add-bucket h$f host rack=r1
done
for f in `seq 1 32`; do
    ceph osd crush rm h$f
done
ceph osd crush rm r1
ceph osd crush weight-set rm-compat

echo OK
