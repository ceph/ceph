#!/bin/bash

source $(dirname $0)/../detect-build-env-vars.sh
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh
$CEPH_ROOT/src/test/osd/osd-scrub-repair.sh \
    TEST_unfound_erasure_coded_appends \
    TEST_unfound_erasure_coded_overwrites
