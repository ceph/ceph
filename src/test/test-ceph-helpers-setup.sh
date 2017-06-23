#!/bin/bash
source $(dirname $0)/detect-build-env-vars.sh

$CEPH_ROOT/qa/workunits/ceph-helpers.sh TESTS test_setup test_teardown
