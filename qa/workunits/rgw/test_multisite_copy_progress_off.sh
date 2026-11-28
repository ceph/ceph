#!/usr/bin/env bash
# Local reproducer: cross-zonegroup CopyObject with rgw_copy_obj_progress=false.
# Requires a built tree and mstart (src/test/rgw/test_multi.md). Teuthology:
# qa/suites/rgw/multisite/copy_obj_progress_off.yaml
set -euo pipefail

BUILD_DIR="${BUILD_DIR:-build}"
CEPH_SRC="${CEPH_SRC:-$(cd "$(dirname "$0")/../../.." && pwd)}"
TEST_RGW="${CEPH_SRC}/src/test/rgw"

if [[ ! -x "${BUILD_DIR}/bin/radosgw" ]]; then
  echo "ERROR: missing ${BUILD_DIR}/bin/radosgw (set BUILD_DIR)" >&2
  exit 2
fi

export LD_LIBRARY_PATH="${BUILD_DIR}/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export BUILD_DIR
export RGW_TEST_COPY_PROGRESS_OFF=1

CONF="$(mktemp)"
trap 'rm -f "$CONF"' EXIT
cat >"$CONF" <<'EOF'
[DEFAULT]
num_zonegroups=2
num_zones=1
gateways_per_zone=1
no_bootstrap=false
log_level=5
EOF
export RGW_MULTI_TEST_CONF="$CONF"

for id in c1 c2 c3 c4; do
  "${CEPH_SRC}/src/mstop.sh" "$id" 2>/dev/null || true
done

cd "${BUILD_DIR}"
export MON=1 OSD=1 MDS=0 MGR=1 RGW=0

TEST_TARGET="${TEST_RGW}/test_multi.py:test_copy_obj_between_zonegroups"
if command -v nosetests3 &>/dev/null; then
  nosetests3 -v "$TEST_TARGET"
elif python3 -m nose --version &>/dev/null 2>&1; then
  python3 -m nose -v "$TEST_TARGET"
else
  nosetests -v "$TEST_TARGET"
fi

for id in c1 c2 c3 c4; do
  "${CEPH_SRC}/src/mstop.sh" "$id" 2>/dev/null || true
done

echo OK