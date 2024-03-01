#!/bin/sh -ex

# Running as root because the filesystem root directory will be
# owned by uid 0, and that's where we're writing.
sudo python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py
exit 0
