#!/bin/bash
source $(dirname $0)/detect-build-env-vars.sh

$CEPH_ROOT/qa/workunits/ceph-helpers.sh TESTS test_get_pg \
	test_get_primary test_get_not_primary test_flush_pg_stats
