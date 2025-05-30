#!/bin/bash
#
# An extreme case used for testing snaptrim optimization.
# Start three OSDs with vstart and create one pg. This script is designed to test the performance of snaptrim.
# TAfter the test is completed, the perf metric 'l_osd_snap_trim_get_raw_object_lat' can be viewed
#
# Usage: $0 [options] <pool_name> <rbd_name>
# Options:
#   --dry-run: Only show commands that would be executed, but do not execute them.
#   --verbose: Show detailed output.

set -euo pipefail  # Exit immediately if a pipeline returns non-zero status.
LOGFILE="extreme_snaptrim_test.log"
exec > >(tee -a "${LOGFILE}") 2>&1  # Log all output to file.

usage() {
    echo "Usage: $0 [options] <pool_name> <rbd_name>"
    echo "Options:"
    echo "  --dry-run: Only show commands that would be executed, but do not execute them."
    echo "  --verbose: Show detailed output."
    echo "Example: $0 snap-test-pool rbd-test-0"
    exit 1
}

DRY_RUN=0
VERBOSE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --verbose) VERBOSE=1 ;;
        *) break ;;
    esac
    shift
done

if [ $# -ne 2 ]; then
    usage
fi

pool_name=${1:-snap-test-pool}
rbd_name=${2:-rbd-test-0}

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

run_cmd() {
    if [ $DRY_RUN -eq 1 ]; then
        log "[DRY-RUN] $*"
    else
        if [ $VERBOSE -eq 1 ]; then
            log "Executing: $*"
        fi
        eval "$@"
    fi
}

cleanup() {
    log "Cleaning up resources..."
    run_cmd "rbd -p ${pool_name} rm ${rbd_name}"
    run_cmd "ceph osd pool rm ${pool_name} ${pool_name} --yes-i-really-really-mean-it"
}

trap cleanup EXIT

log "Stopping existing Ceph services..."
run_cmd "../src/stop.sh all"

log "Starting Ceph cluster with 3 OSDs..."
run_cmd "MON=1 OSD=3 MGR=1 ../src/vstart.sh -n --without-dashboard"

log "Creating pool and RBD image..."
run_cmd "ceph osd pool create ${pool_name} 1 1"
run_cmd "rbd pool init -p ${pool_name}"
run_cmd "rbd -p ${pool_name} create --image ${rbd_name} --size 100G"
run_cmd "rbd -p ${pool_name} bench --image ${rbd_name} --io-size 4M --io-total 100G --io-type write --io-pattern seq"
run_cmd "rbd -p ${pool_name} snap create ${rbd_name}@snap0"
run_cmd "rbd -p ${pool_name} bench --image ${rbd_name} --io-size 4M --io-total 100G --io-type write --io-pattern seq"

log "Waiting for PG to become active+clean..."
while ! ceph pg dump | grep -q "active+clean"; do
    sleep 5
done

osd_ID=$(ceph pg dump | grep active+clean | awk '{print $17}' | awk -F '[' '{print $2}' | awk -F ',' '{print $1}')
pid=$(ps -aux | grep "osd.*-i ${osd_ID}" | grep -v grep | awk '{print $2}')
log "OSD ID: ${osd_ID}, PID: ${pid}"

log "Deleting snapshot: ${snap}"
run_cmd "rbd -p ${pool_name} snap rm ${rbd_name}@snap0"


log "Test completed successfully!"