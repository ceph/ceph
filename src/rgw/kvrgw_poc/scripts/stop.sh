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
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/scripts/kvrgw-common.sh"
INSTANCES="${KVRGW_INSTANCES}"
[[ $# -ge 1 && "$1" =~ ^[0-9]+$ ]] && INSTANCES="$1"
kvrgw_stop_servers "${INSTANCES}"
echo "kv-rgw stopped (${INSTANCES} instance(s) + GW)."
