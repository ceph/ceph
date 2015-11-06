#!/bin/sh -x

expect_failure() {
	if [ `"$@"` -e 0 ]; then
		return 1
	fi
	return 0
}
set -e

ceph mds set allow_new_snaps true --yes-i-really-mean-it

mkdir -p d1/d2
mkdir -p d1/d3
mkdir d1/.snap/foo
mkdir d1/d2/.snap/foo
mkdir d1/d3/.snap/foo
mkdir d1/d3/.snap/bar
mv d1/d2/.snap/foo d1/d2/.snap/bar
# snapshot name can't start with _
expect_failure mv d1/d2/.snap/bar d1/d2/.snap/_bar
# can't rename parent snapshot
expect_failure mv d1/d2/.snap/_foo_* d1/d2/.snap/foo
expect_failure mv d1/d2/.snap/_foo_* d1/d2/.snap/_foo_1
# can't rename snapshot to different directroy
expect_failure mv d1/d2/.snap/bar d1/.snap/
# can't overwrite existing snapshot
expect_failure python -c "import os; os.rename('d1/d3/.snap/foo', 'd1/d3/.snap/bar')"
# can't move snaphost out of snapdir
expect_failure python -c "import os; os.rename('d1/.snap/foo', 'd1/foo')"

rmdir d1/.snap/foo
rmdir d1/d2/.snap/bar
rmdir d1/d3/.snap/foo
rmdir d1/d3/.snap/bar
rm -rf d1

echo OK
