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
# Start nginx GW on :9080 for N frontend upstreams (frontends must already be listening).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kvrgw-common.sh
source "${ROOT}/scripts/kvrgw-common.sh"

N="${KVRGW_INSTANCES}"
if [[ $# -ge 1 && "$1" =~ ^[0-9]+$ ]]; then
  N="$1"
fi

kvrgw_ensure_dirs
if ! command -v nginx >/dev/null 2>&1; then
  echo "nginx not found; run: ${ROOT}/scripts/install_lb.sh" >&2
  exit 1
fi

kvrgw_start_gw "${N}"
