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
# Shared helpers for power-cycle upload chaos tests.
set -euo pipefail

: "${ROOT:?ROOT required}"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

KVRGW_PCYCLE_TARGET="${KVRGW_PCYCLE_TARGET:-65536}"
KVRGW_PCYCLE_KVRGW="${KVRGW_KVRGW_DIR:-/tmp/kvrgw}"
KVRGW_PCYCLE_RESTART_INDEX="${KVRGW_PCYCLE_RESTART_INDEX:-1}"
KVRGW_PCYCLE_TRIGGER_COUNT="${KVRGW_PCYCLE_TRIGGER_COUNT:-8192}"
KVRGW_PCYCLE_WORKERS="${KVRGW_PCYCLE_WORKERS:-8}"

BUCKET_A="kv-pcycle-a-$$"
BUCKET_B="kv-pcycle-b-$$"
PCYCLE_PAGE1_TOKEN=""
PCYCLE_STEP_T0="${SECONDS}"

pcycle_ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

pcycle_step() {
  PCYCLE_STEP_T0="${SECONDS}"
  echo "[$(pcycle_ts)] STEP: $*"
}

pcycle_ok() {
  echo -e "[$(pcycle_ts)] ${GREEN}OK:${NC} $* ($((SECONDS - PCYCLE_STEP_T0))s)"
}

pcycle_fail() {
  echo -e "[$(pcycle_ts)] ${RED}FAIL:${NC} $*" >&2
}

pcycle_run() {
  local name="$1"
  shift
  pcycle_step "${name}"
  if "$@"; then
    pcycle_ok "${name}"
  else
    pcycle_fail "${name} ($((SECONDS - PCYCLE_STEP_T0))s)"
    return 1
  fi
}

pcycle_require_n3() {
  [[ "${KVRGW_INSTANCES}" -eq 3 ]] || {
    echo "SKIP: power-cycle tests require KVRGW_INSTANCES=3 (got ${KVRGW_INSTANCES})" >&2
    exit 0
  }
  kvrgw_refresh_live_indices
  [[ "${KVRGW_LIVE_COUNT}" -eq 3 ]] || {
    pcycle_fail "need 3 live instances before test (live=${KVRGW_LIVE_COUNT})"
    exit 1
  }
}

pcycle_require_tools() {
  command -v aws >/dev/null || { pcycle_fail "missing aws"; exit 1; }
  command -v s5cmd >/dev/null || { pcycle_fail "missing s5cmd"; exit 1; }
  command -v python3 >/dev/null || { pcycle_fail "missing python3"; exit 1; }
}

pcycle_prepare_kvrgw() {
  local n="${KVRGW_PCYCLE_TARGET}"
  if [[ ! -d "${KVRGW_PCYCLE_KVRGW}" ]] || [[ "$(find "${KVRGW_PCYCLE_KVRGW}" -maxdepth 1 -type f | wc -l)" -ne "${n}" ]]; then
    pcycle_step "prepare kvrgw n=${n} dir=${KVRGW_PCYCLE_KVRGW}"
    if [[ "${n}" -eq 65536 ]]; then
      "${ROOT}/scripts/mk_random_files.sh" -n 64K -s 1 "${KVRGW_PCYCLE_KVRGW}" >/dev/null
    else
      "${ROOT}/scripts/mk_random_files.sh" -n "${n}" -s 1 "${KVRGW_PCYCLE_KVRGW}" >/dev/null
    fi
  fi
  local cnt
  cnt="$(find "${KVRGW_PCYCLE_KVRGW}" -maxdepth 1 -type f | wc -l)"
  [[ "${cnt}" -eq "${n}" ]] || { pcycle_fail "kvrgw has ${cnt} files, want ${n}"; exit 1; }
  pcycle_ok "kvrgw ready count=${cnt}"
}

pcycle_suspend_gc() {
  pcycle_run "suspend GC on live instances" \
    bash -c '"$1" set-gc-config suspended=1 interval_sec=3600 max_objects_per_sec=0 max_mb_per_sec=0 >/dev/null' \
    _ "${ROOT}/scripts/gc_ctl.sh"
}

pcycle_cleanup() {
  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET_A}" --force >/dev/null 2>&1 || true
  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${BUCKET_B}" --force >/dev/null 2>&1 || true
}

pcycle_sync_bucket() {
  local bucket="$1"
  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${bucket}" --force >/dev/null 2>&1 || true
  aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${bucket}" >/dev/null
  kvrgw_s5cmd_log "pcycle-sync-${bucket}" --numworkers "${KVRGW_PCYCLE_WORKERS}" --endpoint-url "${ENDPOINT}" \
    sync "${KVRGW_PCYCLE_KVRGW}/" "s3://${bucket}/"
}

pcycle_sync_bucket_bg() {
  local bucket="$1"
  aws --endpoint-url "${ENDPOINT}" s3 rb "s3://${bucket}" --force >/dev/null 2>&1 || true
  aws --endpoint-url "${ENDPOINT}" s3 mb "s3://${bucket}" >/dev/null
  kvrgw_s5cmd_bg "pcycle-sync-bg-${bucket}" --numworkers "${KVRGW_PCYCLE_WORKERS}" --endpoint-url "${ENDPOINT}" \
    sync "${KVRGW_PCYCLE_KVRGW}/" "s3://${bucket}/"
}

pcycle_s5cmd_list_count() {
  local bucket="$1"
  kvrgw_s5cmd_ls_count "pcycle-ls-${bucket}" "${bucket}"
}

pcycle_wait_list_page1() {
  local bucket="$1" page_size="${2:-1000}"
  pcycle_step "wait aws list page1 bucket=${bucket} (>=${page_size} keys + continuation token)"
  PCYCLE_PAGE1_TOKEN="$(
    python3 - "${ENDPOINT}" "${bucket}" "${page_size}" <<'PY'
import json, subprocess, sys, time
endpoint, bucket, page_size = sys.argv[1], sys.argv[2], int(sys.argv[3])
while True:
    data = json.loads(subprocess.check_output([
        "aws", "--endpoint-url", endpoint, "s3api", "list-objects-v2",
        "--bucket", bucket, "--max-keys", str(page_size), "--output", "json",
    ], text=True))
    n = len(data.get("Contents") or [])
    token = data.get("NextContinuationToken") or ""
    if n >= page_size and token:
        print(token)
        sys.exit(0)
    time.sleep(1)
PY
  )"
  pcycle_ok "page1 ready (>=${page_size} keys, truncated)"
}

pcycle_page2_list_and_kill_parallel() {
  local bucket="$1" token="$2"
  local page2_out="${ROOT}/.logs/pcycle-page2-$$.json"
  pcycle_step "aws list page2 + kill instance ${KVRGW_PCYCLE_RESTART_INDEX} in parallel"
  aws --endpoint-url "${ENDPOINT}" s3api list-objects-v2 \
    --bucket "${bucket}" --max-keys 1000 \
    --continuation-token "${token}" --output json \
    >"${page2_out}" 2>/dev/null &
  local page2_pid=$!
  kvrgw_kill_instance "${KVRGW_PCYCLE_RESTART_INDEX}"
  kvrgw_refresh_live_indices
  kvrgw_refresh_gw_live || true
  if wait "${page2_pid}"; then
    pcycle_ok "page2 list finished (during kill)"
  else
    pcycle_ok "page2 list exited non-zero (ok during kill)"
  fi
  rm -f "${page2_out}"
  pcycle_ok "kill instance ${KVRGW_PCYCLE_RESTART_INDEX} (live=${KVRGW_LIVE_COUNT})"
}

pcycle_wait_upload_s5cmd() {
  local bucket="$1" upload_pid="$2"
  local target="${KVRGW_PCYCLE_TARGET}" poll=0 c=0
  pcycle_step "wait upload bucket=${bucket} target=${target} (s5cmd ls)"
  while [[ "${poll}" -lt 600 ]]; do
    poll=$((poll + 1))
    c="$(pcycle_s5cmd_list_count "${bucket}")"
    if [[ $((poll % 5)) -eq 0 || "${c}" -ge "${target}" ]]; then
      echo "[$(pcycle_ts)] s5cmd poll ${poll}: count=${c}/${target}"
    fi
    if [[ "${c}" -ge "${target}" ]]; then
      pcycle_ok "s5cmd count ${c} >= ${target} (${poll} polls)"
      return 0
    fi
    if ! kill -0 "${upload_pid}" 2>/dev/null; then
      if [[ "${c}" -ge "${target}" ]]; then
        pcycle_ok "upload pid exited; s5cmd count=${c}"
        return 0
      fi
      kvrgw_s5cmd_wait "pcycle-sync-wait-pid=${upload_pid}" "${upload_pid}" || return 1
      c="$(kvrgw_verify_count_after_sync "${bucket}" "${target}" "pcycle-sync-bg-${bucket}")" || {
        pcycle_fail "upload pid exited early (s5cmd count=${c}/${target})"
        return 1
      }
      pcycle_ok "upload finished; verified count=${c}"
      return 0
    fi
    sleep 2
  done
  pcycle_fail "timed out (s5cmd count=${c}/${target})"
  return 1
}

pcycle_list_count() {
  local bucket="$1"
  python3 - "${ENDPOINT}" "${bucket}" <<'PY'
import json, subprocess, sys
endpoint, bucket = sys.argv[1:3]
token, n = "", 0
while True:
    cmd = [
        "aws", "--endpoint-url", endpoint, "s3api", "list-objects-v2",
        "--bucket", bucket, "--output", "json", "--max-keys", "1000",
    ]
    if token:
        cmd += ["--continuation-token", token]
    data = json.loads(subprocess.check_output(cmd, text=True))
    n += len(data.get("Contents") or [])
    token = data.get("NextContinuationToken") or ""
    if not token:
        break
print(n)
PY
}

pcycle_wait_upload_count() {
  local bucket="$1" min="$2" max_polls="${3:-600}"
  local poll=0 c=0
  pcycle_step "wait upload count bucket=${bucket} trigger>=${min}"
  while [[ "${poll}" -lt "${max_polls}" ]]; do
    poll=$((poll + 1))
    c="$(pcycle_list_count "${bucket}" 2>/dev/null || echo 0)"
    if [[ $((poll % 5)) -eq 0 || "${c}" -ge "${min}" ]]; then
      echo "[$(pcycle_ts)] poll ${poll}: count=${c}/${KVRGW_PCYCLE_TARGET} (trigger>=${min})"
    fi
    if [[ "${c}" -ge "${min}" ]]; then
      pcycle_ok "upload count ${c} >= ${min} (${poll} polls)"
      return 0
    fi
    sleep 1
  done
  pcycle_fail "timed out waiting for count >= ${min} (last=${c})"
  return 1
}

pcycle_wait_sync_pid() {
  local pid="$1"
  kvrgw_s5cmd_wait "pcycle-sync-wait-pid=${pid}" "${pid}"
}

pcycle_retry_sync_if_needed() {
  local bucket="$1"
  if pcycle_sync_bucket "${bucket}"; then
    return 0
  fi
  pcycle_step "retry sync bucket=${bucket}"
  pcycle_sync_bucket "${bucket}"
}

pcycle_verify_bucket() {
  local bucket="$1"
  "${ROOT}/scripts/verify_bucket_all.sh" "${bucket}" \
    --kvrgw "${KVRGW_PCYCLE_KVRGW}" --expected "${KVRGW_PCYCLE_TARGET}"
}

pcycle_verify_both() {
  # bucketA: uploaded before restart — proves restarted instance + RR read old data.
  # bucketB: uploaded after restart — proves full cluster write/read path.
  pcycle_run "verify bucketA (pre-restart upload) bucket=${BUCKET_A}" \
    pcycle_verify_bucket "${BUCKET_A}"
  pcycle_run "verify bucketB (post-restart upload) bucket=${BUCKET_B}" \
    pcycle_verify_bucket "${BUCKET_B}"
}

pcycle_restart_configured() {
  kvrgw_restart_instance "${KVRGW_PCYCLE_RESTART_INDEX}"
  kvrgw_wait_live_count 3 120
}

pcycle_upload_bucket_b() {
  pcycle_run "upload ${KVRGW_PCYCLE_TARGET} -> ${BUCKET_B}" \
    pcycle_sync_bucket "${BUCKET_B}"
}

pcycle_init_env() {
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
  export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
  if [[ "${KVRGW_S5CMD_LOG_APPEND:-}" != 1 ]]; then
    kvrgw_s5cmd_init_log
  fi
  export KVRGW_S5CMD_LOG
  PCYCLE_TEST_T0="${SECONDS}"
  pcycle_require_n3
  pcycle_require_tools
  trap pcycle_cleanup EXIT
  pcycle_prepare_kvrgw
  pcycle_suspend_gc
}

pcycle_print_total() {
  echo -e "[$(pcycle_ts)] ${GREEN}OK:${NC} TOTAL elapsed=$((SECONDS - PCYCLE_TEST_T0))s"
}
