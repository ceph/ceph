#!/bin/bash
source $(dirname $0)/detect-build-env-vars.sh

$CEPH_ROOT/qa/workunits/ceph-helpers.sh TESTS test_run_osd test_destroy_osd \
	test_activate_osd test_wait_for_osd test_get_osds
