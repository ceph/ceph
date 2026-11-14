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
# gc_ctl wrapper: fan-out admin to all backends when KVRGW_INSTANCES>1.
#
# Multi-instance rules:
#   set-gc-config  — apply to all; save per-instance HANDLE; print instance 0 HANDLE
#   wait-applied   — wait on each instance using its own handle from last set
#   get-gc-config  — verify policy fields match all instances; each instance
#                    internally consistent (pending_age in {active_age, active_age+1});
#                    print instance 0 config
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

GC_CTL_BIN="${ROOT}/build/gc_ctl"
GC_HANDLES_FILE="${RUN_DIR}/gc-handles.last"

kvrgw_gc_live_indices() {
  kvrgw_refresh_live_indices
  if [[ "${KVRGW_LIVE_COUNT}" -lt 1 ]]; then
    echo -e "${RED}FAIL:${NC} no live backend admin sockets" >&2
    return 1
  fi
}

if [[ ! -x "${GC_CTL_BIN}" ]]; then
  echo "gc_ctl not built; building backend..." >&2
  cmake -S "${ROOT}/backend" -B "${ROOT}/build" >/dev/null
  cmake --build "${ROOT}/build" --target gc_ctl -j"$(nproc)"
fi

gc_ctl_one() {
  local admin="$1"
  shift
  KVRGW_DATA="${DATA_ROOT}" KVRGW_ADMIN_SOCKET="${admin}" "${GC_CTL_BIN}" "$@"
}

gc_cfg_value() {
  local cfg="$1" key="$2"
  echo "${cfg}" | awk -F= -v k="${key}" '$1 == k { print $2; exit }'
}

gc_cfg_policy_fingerprint() {
  local cfg="$1"
  echo "suspended=$(gc_cfg_value "${cfg}" suspended)"
  echo "interval_sec=$(gc_cfg_value "${cfg}" interval_sec)"
  echo "max_objects_per_sec=$(gc_cfg_value "${cfg}" max_objects_per_sec)"
  echo "max_mb_per_sec=$(gc_cfg_value "${cfg}" max_mb_per_sec)"
}

gc_verify_instance_consistency() {
  local instance="$1" cfg="$2"
  local active pending
  active="$(gc_cfg_value "${cfg}" active_age)"
  pending="$(gc_cfg_value "${cfg}" pending_age)"
  if [[ -z "${active}" || -z "${pending}" ]]; then
    echo -e "${RED}FAIL:${NC} instance ${instance}: missing active_age/pending_age" >&2
    return 1
  fi
  if [[ "${pending}" != "${active}" && "${pending}" != $((active + 1)) ]]; then
    echo -e "${RED}FAIL:${NC} instance ${instance}: pending_age=${pending} not in {${active}, $((active + 1))}" >&2
    return 1
  fi
}

gc_verify_policy_match() {
  local ref="$1"
  shift
  local cfg
  for cfg in "$@"; do
    if [[ "$(gc_cfg_policy_fingerprint "${cfg}")" != "${ref}" ]]; then
      return 1
    fi
  done
}

if [[ "${KVRGW_INSTANCES}" -le 1 ]]; then
  kvrgw_refresh_live_indices
  if [[ "${KVRGW_LIVE_COUNT}" -lt 1 ]]; then
    echo -e "${RED}FAIL:${NC} backend instance 0 not live" >&2
    exit 1
  fi
  gc_ctl_one "$(kvrgw_instance_admin_socket 0)" "$@"
  exit $?
fi

cmd="${1:-}"

case "${cmd}" in
  set-gc-config)
    kvrgw_gc_live_indices || exit 1
    mkdir -p "${RUN_DIR}"
    : > "${GC_HANDLES_FILE}"
    first_handle=""
    for i in "${KVRGW_LIVE_INDICES[@]}"; do
      out="$(gc_ctl_one "$(kvrgw_instance_admin_socket "${i}")" "$@")"
      handle="$(echo "${out}" | awk -F= '/^HANDLE=/{print $2}')"
      [[ -n "${handle}" ]] || { echo -e "${RED}FAIL:${NC} instance ${i}: no HANDLE from set-gc-config" >&2; exit 1; }
      echo "${i}=${handle}" >> "${GC_HANDLES_FILE}"
      [[ -z "${first_handle}" ]] && first_handle="${handle}"
    done
    echo "HANDLE=${first_handle}"
    ;;

  wait-applied)
    if [[ ! -f "${GC_HANDLES_FILE}" ]]; then
      echo -e "${RED}FAIL:${NC} no ${GC_HANDLES_FILE}; run set-gc-config first" >&2
      exit 1
    fi
    wait_args=()
    skip=0
    for arg in "${@:2}"; do
      if [[ "${skip}" -eq 1 ]]; then
        skip=0
        continue
      fi
      if [[ "${arg}" == "--handle" ]]; then
        skip=1
        continue
      fi
      wait_args+=("${arg}")
    done
    while IFS='=' read -r inst handle; do
      [[ -n "${inst}" ]] || continue
      kvrgw_instance_is_live "${inst}" || { echo -e "${RED}FAIL:${NC} instance ${inst} not live for wait-applied" >&2; exit 1; }
      gc_ctl_one "$(kvrgw_instance_admin_socket "${inst}")" wait-applied \
        --handle "${handle}" "${wait_args[@]}" || exit 1
    done < "${GC_HANDLES_FILE}"
    ;;

  get-gc-config)
    kvrgw_gc_live_indices || exit 1
    cfgs=()
    for i in "${KVRGW_LIVE_INDICES[@]}"; do
      cfgs[i]="$(gc_ctl_one "$(kvrgw_instance_admin_socket "${i}")" get-gc-config)"
      gc_verify_instance_consistency "${i}" "${cfgs[i]}" || exit 1
    done
    ref="$(gc_cfg_policy_fingerprint "${cfgs[${KVRGW_LIVE_INDICES[0]}]}")"
    for i in "${KVRGW_LIVE_INDICES[@]:1}"; do
      if [[ "$(gc_cfg_policy_fingerprint "${cfgs[i]}")" != "${ref}" ]]; then
        echo -e "${RED}FAIL:${NC} get-gc-config policy mismatch instance ${i}" >&2
        echo "instance ${KVRGW_LIVE_INDICES[0]}:" >&2
        echo "${ref}" >&2
        echo "instance ${i}:" >&2
        gc_cfg_policy_fingerprint "${cfgs[i]}" >&2
        exit 1
      fi
    done
    echo "${cfgs[${KVRGW_LIVE_INDICES[0]}]}"
    ;;

  *)
    kvrgw_gc_live_indices || exit 1
    gc_ctl_one "$(kvrgw_instance_admin_socket "${KVRGW_LIVE_INDICES[0]}")" "$@"
    ;;
esac
