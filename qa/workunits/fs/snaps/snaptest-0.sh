#!/bin/sh -x

expect_failure() {
	if "$@"; then return 1; else return 0; fi
}
set -e

ceph mds set allow_new_snaps false
expect_failure mkdir .snap/foo
ceph mds set allow_new_snaps true --yes-i-really-mean-it

echo asdf > foo
mkdir .snap/foo
grep asdf .snap/foo/foo
rmdir .snap/foo

echo asdf > bar
mkdir .snap/bar
rm bar
grep asdf .snap/bar/bar
rmdir .snap/bar
rm foo

ceph mds set allow_new_snaps false
expect_failure mkdir .snap/baz

echo OK
