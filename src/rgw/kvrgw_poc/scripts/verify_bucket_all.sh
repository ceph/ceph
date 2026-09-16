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
# List (aws + s5cmd) + bulk download (s5cmd sync) + diff vs kvrgw dir.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

BUCKET=""
KVRGW="${KVRGW_KVRGW_DIR:-/tmp/kvrgw}"
EXPECTED="${KVRGW_VERIFY_EXPECTED:-65536}"

usage() {
  echo "Usage: $(basename "$0") BUCKET [--kvrgw DIR] [--expected N]"
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kvrgw) KVRGW="$2"; shift 2 ;;
    --expected) EXPECTED="$2"; shift 2 ;;
    -h|--help) usage ;;
    *)
      if [[ -z "${BUCKET}" ]]; then BUCKET="$1"; shift
      else echo "Unknown: $1" >&2; exit 2; fi ;;
  esac
done

[[ -n "${BUCKET}" ]] || usage
[[ -d "${KVRGW}" ]] || { echo -e "${RED}FAIL:${NC} kvrgw dir missing: ${KVRGW}" >&2; exit 1; }

command -v s5cmd >/dev/null || { echo -e "${RED}FAIL:${NC} s5cmd required" >&2; exit 1; }
command -v diff >/dev/null || { echo -e "${RED}FAIL:${NC} diff required" >&2; exit 1; }

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export KVRGW_S5CMD_LOG="${KVRGW_S5CMD_LOG:-${ROOT}/.logs/s5cmd.log}"

python3 "${ROOT}/scripts/verify_bucket_all.py" \
  --endpoint "${ENDPOINT}" \
  --bucket "${BUCKET}" \
  --kvrgw "${KVRGW}" \
  --expected "${EXPECTED}"
