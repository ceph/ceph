#!/usr/bin/env bash
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

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"
CLEAN=0
PERF=0
INSTANCES="${KVRGW_INSTANCES}"
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS] [N]
  N  frontend instances (default 1). GW :9080, frontends :9081+
Options: --clean|-c  --perf  --instances N  --help|-h
EOF
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean|-c) CLEAN=1; shift ;;
    --perf) PERF=1; shift ;;
    --instances) INSTANCES="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *)
      if [[ "$1" =~ ^[0-9]+$ ]]; then INSTANCES="$1"; shift
      else echo "Unknown: $1" >&2; exit 1; fi ;;
  esac
done
[[ "${INSTANCES}" -ge 1 ]] || { echo "N>=1 required" >&2; exit 1; }
kvrgw_register_instances "${INSTANCES}"
kvrgw_ensure_dirs
kvrgw_stop_servers "${INSTANCES}"
if [[ "${CLEAN}" -eq 1 ]]; then
  kvrgw_clean_fdb
  rm -rf "${DATA_ROOT}" && mkdir -p "${DATA_ROOT}"
fi
kvrgw_start_fdb
kvrgw_build
if [[ "${PERF}" -eq 1 ]]; then
  echo "Perf mode: FDB healthy, binary ready at ${ROOT}/build/kv-rgw-backend"
  exit 0
fi
kvrgw_start_servers "${INSTANCES}"
kvrgw_add_tenant
kvrgw_verify
kvrgw_run_fast_tests
kvrgw_print_report
