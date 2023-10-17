#!/usr/bin/env bash

set -ex

# Execute tests
: ${CEPH_DEV_FOLDER:=${PWD}}
${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci/scripts/bootstrap-rook-cluster.sh
cd ${CEPH_DEV_FOLDER}/src/pybind/mgr/rook/ci/tests
behave
