#!/bin/bash -x

set -e

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

# note: we need to pass the other args or ceph_argparse.py will take
# 'invalid' that is not replicated|erasure and assume it is the next
# argument, which is a string.
expect_false ceph osd pool create foo 123 123 invalid foo-profile foo-ruleset

ceph osd pool create foo 123 123 replicated
ceph osd pool create fooo 123 123 erasure default
ceph osd pool create foooo 123

ceph osd pool create foo 123 # idempotent

ceph osd pool set foo size 1
ceph osd pool set foo size 4
ceph osd pool set foo size 10
expect_false ceph osd pool set foo size 0
expect_false ceph osd pool set foo size 20

# should fail due to safety interlock
expect_false ceph osd pool delete foo
expect_false ceph osd pool delete foo foo
expect_false ceph osd pool delete foo foo --force
expect_false ceph osd pool delete foo fooo --yes-i-really-mean-it
expect_false ceph osd pool delete foo --yes-i-really-mean-it foo

ceph osd pool delete foooo foooo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it
ceph osd pool delete foo foo --yes-i-really-really-mean-it

# idempotent
ceph osd pool delete foo foo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it

# non-existent pool
ceph osd pool delete fuggg fuggg --yes-i-really-really-mean-it

echo OK


