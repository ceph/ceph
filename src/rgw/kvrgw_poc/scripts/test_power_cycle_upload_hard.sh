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
# Power-cycle hard: kill instance during aws list page2; finish upload; verify both buckets.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/test_power_cycle_common.sh
source "${ROOT}/scripts/test_power_cycle_common.sh"

pcycle_init_env

pcycle_step "power-cycle HARD (page2 list + kill, then s5cmd ls; verify both buckets)"

pcycle_sync_bucket_bg "${BUCKET_A}"
UPLOAD_PID=$!
pcycle_ok "background upload bucketA pid=${UPLOAD_PID}"

pcycle_wait_list_page1 "${BUCKET_A}"
pcycle_page2_list_and_kill_parallel "${BUCKET_A}" "${PCYCLE_PAGE1_TOKEN}"

pcycle_run "restart instance ${KVRGW_PCYCLE_RESTART_INDEX}" \
  pcycle_restart_configured

pcycle_wait_upload_s5cmd "${BUCKET_A}" "${UPLOAD_PID}" || {
  pcycle_run "retry sync bucketA" pcycle_retry_sync_if_needed "${BUCKET_A}"
}

if kill -0 "${UPLOAD_PID}" 2>/dev/null; then
  pcycle_wait_sync_pid "${UPLOAD_PID}" || \
    pcycle_run "retry sync bucketA" pcycle_retry_sync_if_needed "${BUCKET_A}"
fi

pcycle_upload_bucket_b
pcycle_verify_both

pcycle_ok "power-cycle upload hard PASS"
pcycle_print_total
