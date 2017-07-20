#!/bin/bash
source $(dirname $0)/detect-build-env-vars.sh

$CEPH_ROOT/qa/workunits/ceph-helpers.sh TESTS \
	test_objectstore_tool test_get_is_making_recovery_progress \
	test_get_last_scrub_stamp \
	test_is_clean test_get_timeout_delays \
	test_repair test_pg_scrub test_expect_failure \
	test_erasure_code_plugin_exists test_display_logs
