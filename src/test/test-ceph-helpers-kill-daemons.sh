#!/bin/bash
source $(dirname $0)/detect-build-env-vars.sh

$CEPH_ROOT/qa/workunits/ceph-helpers.sh TESTS test_kill_daemon test_kill_daemons
