#!/usr/bin/env bash
set -ex

CV_TEST=$(python3 -c "import ceph_volume.tests; import os; print(os.path.dirname(ceph_volume.tests.__file__))")
python3 -m pytest -v "$CV_TEST/test_ceph_volume.py"
