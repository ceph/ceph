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
# Start N frontend instances + GW without rebuild (for dev iteration).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

INSTANCES="${KVRGW_INSTANCES}"
CLEAN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean|-c) CLEAN=1; shift ;;
    --instances) INSTANCES="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $(basename "$0") [--clean] [N]"
      exit 0
      ;;
    *)
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        INSTANCES="$1"
        shift
      else
        echo "Unknown option: $1" >&2
        exit 1
      fi
      ;;
  esac
done

kvrgw_register_instances "${INSTANCES}"
kvrgw_ensure_dirs
kvrgw_stop_servers "${INSTANCES}"
kvrgw_start_fdb
[[ "${CLEAN}" -eq 1 ]] && kvrgw_clean_data
kvrgw_start_servers "${INSTANCES}"
kvrgw_add_tenant
kvrgw_print_report
