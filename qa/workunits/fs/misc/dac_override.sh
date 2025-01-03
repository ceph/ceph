#!/bin/sh -x

expect_failure() {
	if "$@"; then return 1; else return 0; fi
}

set -e

mkdir -p testdir
file=test_chmod.$$

echo "foo" > testdir/${file}
sudo chmod 600 testdir

# only root can read
expect_failure cat testdir/${file}

# directory read/write DAC override for root should allow read
sudo cat testdir/${file}
