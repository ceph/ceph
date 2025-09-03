#!/bin/sh -ex

# Running as root because the filesystem root directory will be
# owned by uid 0, and that's where we're writing.
python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py -k test_create_and_rm_2000_subdir_levels_close_v3
exit 0
