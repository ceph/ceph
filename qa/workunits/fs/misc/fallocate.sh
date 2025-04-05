#!/bin/sh -x

# fallocate with mode 0 should fail with EOPNOTSUPP
set -e
mkdir -p testdir
cd testdir

expect_failure() {
	if "$@"; then return 1; else return 0; fi
}

expect_failure fallocate -l 1M preallocated.txt
rm -f preallocated.txt

cd ..
rmdir testdir
echo OK
