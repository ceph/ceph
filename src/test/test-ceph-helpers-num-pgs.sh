#!/bin/bash
source $(dirname $0)/detect-build-env-vars.sh

$CEPH_ROOT/qa/workunits/ceph-helpers.sh TESTS \
	test_get_num_active_clean test_get_num_pgs
