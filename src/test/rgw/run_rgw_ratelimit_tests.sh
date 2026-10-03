#!/usr/bin/env bash
#
# Run all RGW rate-limit unit and integration tests.
#
# Unit tests need no cluster.  Integration tests (CLS + RADOS store) need a
# running vstart cluster with cls_rgw_ratelimit.so built into lib/rados-classes.
#
# Usage (from anywhere):
#   src/test/rgw/run_rgw_ratelimit_tests.sh
#   src/test/rgw/run_rgw_ratelimit_tests.sh --build
#   CEPH_BUILD_DIR=build-local src/test/rgw/run_rgw_ratelimit_tests.sh
#
# Options:
#   -b, --build           Build test targets with ninja before running
#   -B, --build-dir DIR   CMake build directory (default: auto-detect)
#   --unit-only           Skip integration tests
#   --integration-only    Skip unit tests
#   --no-vstart           Do not start vstart; require an existing cluster
#   --keep-cluster        Do not stop vstart on exit (when we started it)
#   --bench               Also run bench_rgw_ratelimit* (informational)
#   -v, --verbose         Print commands before executing them
#   -h, --help            Show this help
#
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CEPH_ROOT=$(cd "${SCRIPT_DIR}/../../.." && pwd)

DO_BUILD=0
BUILD_DIR=""
UNIT_ONLY=0
INTEGRATION_ONLY=0
NO_VSTART=0
KEEP_CLUSTER=0
RUN_BENCH=0
VERBOSE=0

UNIT_TESTS=(
  unittest_rgw_ratelimit_distributed
  unittest_rgw_ratelimit
)

INTEGRATION_TESTS=(
  ceph_test_cls_rgw_ratelimit
  ceph_test_rgw_ratelimit_rados_store
)

BENCH_TESTS=(
  bench_rgw_ratelimit
  bench_rgw_ratelimit_gc
)

BUILD_TARGETS=(
  cls_rgw_ratelimit
  unittest_rgw_ratelimit_distributed
  unittest_rgw_ratelimit
  ceph_test_cls_rgw_ratelimit
  ceph_test_rgw_ratelimit_rados_store
)

STARTED_VSTART=0
PASSED=()
FAILED=()
SKIPPED=()

usage() {
  sed -n '3,22p' "$0" | sed 's/^# \?//'
}

log() {
  printf '[rgw-ratelimit-tests] %s\n' "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

vrun() {
  if [[ "$VERBOSE" -eq 1 ]]; then
    log "+ $*"
  fi
  "$@"
}

detect_build_dir() {
  if [[ -n "${CEPH_BUILD_DIR:-}" ]]; then
    BUILD_DIR=$(cd "$CEPH_BUILD_DIR" && pwd)
    return
  fi
  if [[ -n "$BUILD_DIR" ]]; then
    BUILD_DIR=$(cd "$BUILD_DIR" && pwd)
    return
  fi

  local candidates=(
    "${CEPH_ROOT}/build-local"
    "${CEPH_ROOT}/build"
    "${CEPH_ROOT}/../build"
    "${PWD}"
  )
  for dir in "${candidates[@]}"; do
    if [[ -f "${dir}/CMakeCache.txt" && -d "${dir}/bin" ]]; then
      BUILD_DIR=$(cd "$dir" && pwd)
      return
    fi
  done
  die "Could not find a CMake build directory. Set CEPH_BUILD_DIR or use --build-dir."
}

setup_test_env() {
  export CEPH_ROOT
  export CEPH_BUILD_DIR="$BUILD_DIR"
  export CEPH_BIN="${BUILD_DIR}/bin"
  export CEPH_LIB="${BUILD_DIR}/lib"
  export LD_LIBRARY_PATH="${BUILD_DIR}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
  export PATH="${CEPH_BIN}:${CEPH_ROOT}/src:${PATH}"
  export PYTHONPATH="${BUILD_DIR}/lib/cython_modules/lib.3:${CEPH_ROOT}/src/pybind${PYTHONPATH:+:${PYTHONPATH}}"
  export LC_ALL=C
}

build_targets() {
  if [[ ! -f "${BUILD_DIR}/build.ninja" ]]; then
    die "No build.ninja in ${BUILD_DIR}; configure the tree first (cmake + ninja)."
  fi
  log "Building targets in ${BUILD_DIR} ..."
  vrun ninja -C "$BUILD_DIR" "${BUILD_TARGETS[@]}"
}

require_binary() {
  local name=$1
  if [[ ! -x "${CEPH_BIN}/${name}" ]]; then
    return 1
  fi
  return 0
}

run_one_test() {
  local name=$1
  local timeout_secs=${2:-0}
  if ! require_binary "$name"; then
    SKIPPED+=("$name (binary missing; rebuild with --build)")
    log "SKIP: ${name} (not found in ${CEPH_BIN})"
    return 0
  fi

  log "RUN: ${name}"
  local cmd=("${CEPH_BIN}/${name}")
  if [[ "$timeout_secs" -gt 0 ]]; then
    cmd=(timeout "$timeout_secs" "${cmd[@]}")
  fi
  if vrun "${cmd[@]}"; then
    PASSED+=("$name")
    log "PASS: ${name}"
    return 0
  fi

  FAILED+=("$name")
  log "FAIL: ${name}"
  return 1
}

cluster_is_up() {
  local conf=${CEPH_CONF:-}
  if [[ -z "$conf" || ! -f "$conf" ]]; then
    return 1
  fi
  vrun timeout 15 "${CEPH_BIN}/ceph" --conf "$conf" status >/dev/null 2>&1
}

cluster_has_osds() {
  local conf=${CEPH_CONF:-}
  if [[ -z "$conf" || ! -f "$conf" ]]; then
    return 1
  fi
  local osd_stat
  osd_stat=$(timeout 15 "${CEPH_BIN}/ceph" --conf "$conf" osd stat 2>/dev/null) || return 1
  [[ "$osd_stat" =~ ^[[:space:]]*[1-9][0-9]*[[:space:]]+osds ]]
}

wait_for_cluster_ready() {
  local timeout_secs=${1:-120}
  local deadline=$((SECONDS + timeout_secs))
  log "Waiting for OSDs (timeout ${timeout_secs}s) ..."
  while (( SECONDS < deadline )); do
    if cluster_has_osds; then
      vrun "${CEPH_BIN}/ceph" --conf "$CEPH_CONF" osd stat
      return 0
    fi
    sleep 2
  done
  return 1
}

vstart_mgr_count() {
  if [[ -x "${CEPH_BIN}/ceph-mgr" ]]; then
    echo 1
  else
    log "ceph-mgr not built; starting vstart with MGR=0"
    echo 0
  fi
}

pick_ceph_conf() {
  if [[ -n "${CEPH_CONF:-}" && -f "${CEPH_CONF}" ]]; then
    return
  fi
  local candidates=(
    "${BUILD_DIR}/ceph.conf"
    "${BUILD_DIR}/out/ceph.conf"
  )
  for conf in "${candidates[@]}"; do
    if [[ -f "$conf" ]]; then
      export CEPH_CONF="$conf"
      return
    fi
  done
}

clean_vstart_state() {
  log "Cleaning stale vstart state in ${BUILD_DIR} ..."
  (
    cd "$BUILD_DIR"
    export CEPH_BIN
    if [[ -x "${CEPH_ROOT}/src/stop.sh" ]]; then
      "${CEPH_ROOT}/src/stop.sh" >/dev/null 2>&1 || true
    fi
    pkill -u "$(id -un)" -f "${BUILD_DIR}/(bin/ceph-|dev/)" >/dev/null 2>&1 || true
    rm -rf dev out asok ceph.conf keyring logrotate.conf logrotate.state
  )
}

start_vstart() {
  pick_ceph_conf
  if cluster_is_up; then
    log "Cluster already running (CEPH_CONF=${CEPH_CONF})"
    return
  fi

  if [[ "$NO_VSTART" -eq 1 ]]; then
    die "No running cluster found and --no-vstart was set. Start vstart first, e.g.:
  cd ${BUILD_DIR}
  ${CEPH_ROOT}/src/vstart.sh -n -d -l --short
  export CEPH_CONF=${BUILD_DIR}/ceph.conf"
  fi

  # vstart -n cannot remove a corrupt ceph.conf (ceph-conf parse fails first).
  clean_vstart_state

  log "Starting vstart cluster in ${BUILD_DIR} ..."
  (
    cd "$BUILD_DIR"
    export CEPH_BIN CEPH_LIB
    MGR=$(vstart_mgr_count)
    export MGR RGW=0 MDS=0 NFS=0
    vrun "${CEPH_ROOT}/src/vstart.sh" \
      --short \
      --without-dashboard \
      -o 'paxos propose interval = 0.01' \
      -n -d -l
  )
  pick_ceph_conf
  STARTED_VSTART=1

  if ! cluster_is_up; then
    die "vstart finished but 'ceph status' failed (CEPH_CONF=${CEPH_CONF:-unset}).
Common causes:
  - stale erasure-code plugins after partial rebuild (run: ninja -C ${BUILD_DIR})
  - corrupt ceph.conf from a previous failed vstart (re-run; script now cleans it)
  - mon/osd failed to start (check ${BUILD_DIR}/out/mon.a.log)"
  fi
  if ! wait_for_cluster_ready 120; then
    die "Cluster has no OSDs after vstart (CEPH_CONF=${CEPH_CONF}).
Integration tests need at least one ceph-osd. Check:
  ${CEPH_BIN}/ceph --conf ${CEPH_CONF} osd stat
  ${BUILD_DIR}/out/osd.0.log
If ceph-mgr was missing, sync the updated test script (MGR=0) or run: ninja ceph-mgr"
  fi
  log "Cluster is up (CEPH_CONF=${CEPH_CONF})"
}

stop_vstart() {
  if [[ "$STARTED_VSTART" -eq 0 || "$KEEP_CLUSTER" -eq 1 ]]; then
    return
  fi
  log "Stopping vstart cluster ..."
  (
    cd "$BUILD_DIR"
    export CEPH_BIN
    if [[ -x "${CEPH_ROOT}/src/stop.sh" ]]; then
      vrun "${CEPH_ROOT}/src/stop.sh" || true
    fi
  )
}

find_cls_plugin() {
  local so
  for so in \
    "${CEPH_LIB}/libcls_rgw_ratelimit.so" \
    "${CEPH_LIB}/rados-classes/libcls_rgw_ratelimit.so"; do
    if [[ -f "$so" ]]; then
      printf '%s\n' "$so"
      return 0
    fi
  done
  for so in \
    "${CEPH_LIB}"/libcls_rgw_ratelimit.so.* \
    "${CEPH_LIB}/rados-classes"/libcls_rgw_ratelimit.so.*; do
    if [[ -f "$so" ]]; then
      printf '%s\n' "$so"
      return 0
    fi
  done
  return 1
}

ensure_cls_plugin() {
  local so
  if so=$(find_cls_plugin); then
    log "Found CLS plugin: ${so}"
    return
  fi

  if [[ ! -f "${BUILD_DIR}/build.ninja" ]]; then
    die "cls_rgw_ratelimit plugin not found. Build it first:
  ninja -C ${BUILD_DIR} cls_rgw_ratelimit

Dev builds place the .so in ${CEPH_LIB}/libcls_rgw_ratelimit.so
(make install uses ${CEPH_LIB}/rados-classes/)"
  fi

  log "CLS plugin missing; building cls_rgw_ratelimit ..."
  vrun ninja -C "$BUILD_DIR" cls_rgw_ratelimit

  if so=$(find_cls_plugin); then
    log "Found CLS plugin: ${so}"
    return
  fi

  die "cls_rgw_ratelimit plugin still missing after build. Expected:
  ${CEPH_LIB}/libcls_rgw_ratelimit.so
  ${CEPH_LIB}/rados-classes/libcls_rgw_ratelimit.so

Try: ninja -C ${BUILD_DIR} cls_rgw_ratelimit"
}

run_unit_tests() {
  local rc=0
  for t in "${UNIT_TESTS[@]}"; do
    run_one_test "$t" || rc=1
  done
  return "$rc"
}

run_integration_tests() {
  ensure_cls_plugin
  start_vstart

  local rc=0
  local integration_timeout=300
  for t in "${INTEGRATION_TESTS[@]}"; do
    run_one_test "$t" "$integration_timeout" || rc=1
  done
  return "$rc"
}

run_bench_tests() {
  local rc=0
  for t in "${BENCH_TESTS[@]}"; do
    run_one_test "$t" || rc=1
  done
  return "$rc"
}

print_summary() {
  log "========== summary =========="
  if ((${#PASSED[@]})); then
    log "passed (${#PASSED[@]}):"
    for t in "${PASSED[@]}"; do
      log "  + ${t}"
    done
  fi
  if ((${#SKIPPED[@]})); then
    log "skipped (${#SKIPPED[@]}):"
    for t in "${SKIPPED[@]}"; do
      log "  ~ ${t}"
    done
  fi
  if ((${#FAILED[@]})); then
    log "failed (${#FAILED[@]}):"
    for t in "${FAILED[@]}"; do
      log "  - ${t}"
    done
  fi
}

cleanup() {
  stop_vstart
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -b|--build) DO_BUILD=1 ;;
      -B|--build-dir)
        shift
        BUILD_DIR=$1
        ;;
      --unit-only) UNIT_ONLY=1 ;;
      --integration-only) INTEGRATION_ONLY=1 ;;
      --no-vstart) NO_VSTART=1 ;;
      --keep-cluster) KEEP_CLUSTER=1 ;;
      --bench) RUN_BENCH=1 ;;
      -v|--verbose) VERBOSE=1 ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "Unknown option: $1 (try --help)"
        ;;
    esac
    shift
  done

  if [[ "$UNIT_ONLY" -eq 1 && "$INTEGRATION_ONLY" -eq 1 ]]; then
    die "--unit-only and --integration-only are mutually exclusive"
  fi

  detect_build_dir
  setup_test_env
  trap cleanup EXIT

  log "CEPH_ROOT=${CEPH_ROOT}"
  log "CEPH_BUILD_DIR=${BUILD_DIR}"

  if [[ "$DO_BUILD" -eq 1 ]]; then
    build_targets
  fi

  local rc=0

  if [[ "$INTEGRATION_ONLY" -eq 0 ]]; then
    log "---- unit tests ----"
    run_unit_tests || rc=1
  fi

  if [[ "$UNIT_ONLY" -eq 0 ]]; then
    log "---- integration tests ----"
    run_integration_tests || rc=1
  fi

  if [[ "$RUN_BENCH" -eq 1 ]]; then
    log "---- benchmarks ----"
    run_bench_tests || rc=1
  fi

  print_summary

  if [[ "$rc" -ne 0 ]]; then
    exit 1
  fi
  log "All requested tests passed."
}

main "$@"
