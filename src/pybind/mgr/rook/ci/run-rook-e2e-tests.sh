#!/usr/bin/env bash

set -ex

export PATH=$PATH:~/.local/bin # behave is installed on this directory

# Execute tests
: ${CEPH_DEV_FOLDER:=${PWD}}
${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci/scripts/bootstrap-rook-cluster.sh
cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci/tests
pip install --upgrade --force-reinstall -r ../requirements.txt
behave
