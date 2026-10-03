#!/usr/bin/env bash
#
# Needs a running cluster (vstart or teuthology). Local example:
#
#   cd /var/test/ceph/build-local
#   ../src/vstart.sh -n -d
#   export PATH=$PWD/bin:$PATH
#   export CEPH_CONF=$PWD/ceph.conf
#   ../qa/workunits/rgw/run-sync-status-json.sh
#
# Teuthology sets PATH via the workunit task; no extra setup needed there.
#
set -ex

mydir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$mydir/../../.." && pwd)

if [ -n "${CEPH_BIN:-}" ]; then
  export PATH="${CEPH_BIN}:$PATH"
elif [ -x "$repo_root/build-local/bin/radosgw-admin" ]; then
  export PATH="$repo_root/build-local/bin:$PATH"
elif [ -x "$repo_root/build/bin/radosgw-admin" ]; then
  export PATH="$repo_root/build/bin:$PATH"
fi

if [ -z "${CEPH_CONF:-}" ]; then
  for conf in \
      "$repo_root/build-local/ceph.conf" \
      "$repo_root/build/ceph.conf" \
      "$repo_root/build-local/out/ceph.conf" \
      "$repo_root/build/out/ceph.conf"
  do
    if [ -f "$conf" ]; then
      export CEPH_CONF="$conf"
      break
    fi
  done
fi

if ! command -v radosgw-admin >/dev/null 2>&1; then
  echo "ERROR: radosgw-admin not found in PATH." >&2
  echo "  PATH=$repo_root/build-local/bin:\$PATH $0" >&2
  exit 1
fi

python3 "$mydir/test_rgw_sync_status_json.py"

echo OK.
