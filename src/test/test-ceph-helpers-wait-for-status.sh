#!/bin/bash
source $(dirname $0)/detect-build-env-vars.sh

$CEPH_ROOT/qa/workunits/ceph-helpers.sh TESTS \
	test_wait_for_clean test_wait_for_health_ok test_wait_for_scrub
