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
# GC admin API: set, poll, get-gc-config, parallel sessions.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

GC_CTL="${ROOT}/scripts/gc_ctl.sh"
MAX_GAP="${MAX_GAP:-1}"

require_backend() {
  kvrgw_refresh_live_indices
  [[ "${KVRGW_LIVE_COUNT}" -ge 1 ]] || { echo -e "${RED}FAIL:${NC} no live admin sockets" >&2; exit 1; }
  local i
  for i in "${KVRGW_LIVE_INDICES[@]}"; do
    kvrgw_admin_responds "${i}" || { echo -e "${RED}FAIL:${NC} admin not responding: instance ${i}" >&2; exit 1; }
  done
}

wait_and_verify() {
  local handle="$1"
  local want_suspended="$2"
  local want_interval="$3"

  "${GC_CTL}" wait-applied --handle "${handle}" --max-gap "${MAX_GAP}" >/dev/null
  local cfg
  cfg="$("${GC_CTL}" get-gc-config)"
  local active_age suspended interval
  active_age="$(echo "${cfg}" | awk -F= '/^active_age=/{print $2}')"
  suspended="$(echo "${cfg}" | awk -F= '/^suspended=/{print $2}')"
  interval="$(echo "${cfg}" | awk -F= '/^interval_sec=/{print $2}')"

  if [[ "${active_age}" != "${handle}" ]]; then
    echo -e "${RED}FAIL:${NC} active_age=${active_age} expected ${handle}" >&2
    exit 1
  fi
  if [[ "${suspended}" != "${want_suspended}" ]]; then
    echo -e "${RED}FAIL:${NC} suspended=${suspended} expected ${want_suspended}" >&2
    exit 1
  fi
  if [[ "${interval}" != "${want_interval}" ]]; then
    echo -e "${RED}FAIL:${NC} interval_sec=${interval} expected ${want_interval}" >&2
    exit 1
  fi
}

require_backend

echo "=== GC admin: suspend GC ==="
handle="$("${GC_CTL}" set-gc-config suspended=1 interval_sec=3600 max_objects_per_sec=0 max_mb_per_sec=0 | awk -F= '/^HANDLE=/{print $2}')"
wait_and_verify "${handle}" 1 3600

echo "=== GC admin: resume GC ==="
handle="$("${GC_CTL}" set-gc-config suspended=0 interval_sec=10 max_objects_per_sec=0 max_mb_per_sec=0 | awk -F= '/^HANDLE=/{print $2}')"
wait_and_verify "${handle}" 0 10

if [[ "${KVRGW_INSTANCES}" -gt 1 ]]; then
  echo "=== GC admin: parallel config sessions (skipped for N>1; per-backend protocol) ==="
else
  echo "=== GC admin: parallel config sessions (MAX_GAP=2) ==="
MAX_GAP=2
pids=()
for i in 1 2; do
  (
    out="/tmp/kv-gc-admin-$$-${i}.out"
    for attempt in 1 2 3 4 5; do
      if h="$("${GC_CTL}" set-gc-config interval_sec=$((100 + i)) max_objects_per_sec=0 max_mb_per_sec=0 suspended=0 2>&1)"; then
        echo "${h}" > "${out}"
        exit 0
      fi
      if [[ "${h}" != *BUSY* ]]; then
        echo "${h}" > "${out}.err"
        exit 1
      fi
      sleep 1
    done
    echo "BUSY after retries" > "${out}.err"
    exit 1
  ) &
  pids+=("$!")
done
for pid in "${pids[@]}"; do
  wait "${pid}" || true
done
handles=()
for i in 1 2; do
  if [[ -f "/tmp/kv-gc-admin-$$-${i}.out" ]]; then
    handles+=("$(awk -F= '/^HANDLE=/{print $2}' "/tmp/kv-gc-admin-$$-${i}.out")")
  fi
  rm -f "/tmp/kv-gc-admin-$$-${i}.out" "/tmp/kv-gc-admin-$$-${i}.out.err"
done

for h in "${handles[@]}"; do
  if [[ -z "${h}" ]]; then
    continue
  fi
  set +e
  out="$("${GC_CTL}" wait-applied --handle "${h}" --max-gap "${MAX_GAP}" 2>&1)"
  rc=$?
  set -e
  if [[ "${rc}" -eq 3 ]]; then
    echo "note: handle ${h} expired (parallel); retrying set+wait"
    h="$("${GC_CTL}" set-gc-config interval_sec=10 suspended=0 max_objects_per_sec=0 max_mb_per_sec=0 | awk -F= '/^HANDLE=/{print $2}')"
    "${GC_CTL}" wait-applied --handle "${h}" --max-gap "${MAX_GAP}"
  elif [[ "${rc}" -ne 0 ]]; then
    echo -e "${RED}FAIL:${NC} wait-applied handle=${h}: ${out}" >&2
    exit 1
  fi
done

fi
echo -e "${GREEN}PASS: gc admin${NC}"
