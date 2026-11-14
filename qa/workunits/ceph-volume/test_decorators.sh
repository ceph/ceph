#!/usr/bin/env bash
set -ex

# Locate the installed ceph_volume package and run its decorator unit tests.
CV_TEST=$(python3 -c "import ceph_volume.tests; import os; print(os.path.dirname(ceph_volume.tests.__file__))")
python3 -m pytest -v "$CV_TEST/test_decorators.py"
