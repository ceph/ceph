#!/bin/bash -ex

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

mkdir quota-test
cd quota-test

# bytes
setfattr . -n ceph.quota.max_bytes -v 100000000  # 100m
expect_false dd if=/dev/zero of=big bs=1M count=1000     # 1g
expect_false dd if=/dev/zero of=second bs=1M count=10
setfattr . -n ceph.quota.max_bytes -v 0
dd if=/dev/zero of=third bs=1M count=10
dd if=/dev/zero of=big2 bs=1M count=100

# files
#addme

cd ..
rm -rf quota-test

echo OK
