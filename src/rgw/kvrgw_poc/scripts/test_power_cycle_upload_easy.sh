#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/usr/bin/env bash
# Power-cycle easy: complete 64K upload, restart one instance, verify both buckets.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/test_power_cycle_common.sh
source "${ROOT}/scripts/test_power_cycle_common.sh"

pcycle_init_env

pcycle_step "power-cycle EASY (restart after bucketA, verify both buckets)"

pcycle_run "upload ${KVRGW_PCYCLE_TARGET} -> ${BUCKET_A}" \
  pcycle_sync_bucket "${BUCKET_A}"

pcycle_run "restart instance ${KVRGW_PCYCLE_RESTART_INDEX}" \
  pcycle_restart_configured

pcycle_upload_bucket_b
pcycle_verify_both

pcycle_ok "power-cycle upload easy PASS"
