#!/bin/bash -e

# Validate the per-module perf counters (mgr_module_<name>) exposed by the
# active mgr. Always-on and already enabled modules are checked in place,
# the rest are enabled, checked and disabled again.
#
# Usage: test_mgr_modules_perf_counters.sh [module ...]
# With no arguments, DEFAULT_MODULES is used.

# Modules that can be enabled on a plain test cluster without extra config,
# health warnings or side effects (pools, orchestrator backends, ...)
DEFAULT_MODULES="hello insights iostat mds_autoscaler osd_perf_query osd_support selftest snap_schedule stats"

TIMEOUT=120
CURRENT_MODULE=""

cleanup() {
    if [[ -n "$CURRENT_MODULE" ]]; then
        echo "[CLEANUP] Disabling '$CURRENT_MODULE' due to unexpected exit" >&2
        ceph mgr module disable "$CURRENT_MODULE" || true
    fi
}
trap cleanup EXIT

get_active_gid() {
    ceph mgr dump -f json | jq -r '.active_gid'
}

# Changing the enabled module set makes the active mgr respawn. Wait for a new
# active mgr (different gid) that reports itself available.
wait_for_mgr_respawn() {
    local OLD_GID=$1
    local GID AVAILABLE
    for ((i = 0; i < TIMEOUT; i++)); do
        read -r GID AVAILABLE < <(ceph mgr dump -f json | jq -r '"\(.active_gid) \(.available)"')
        if [[ "$GID" != "$OLD_GID" && "$GID" != "0" && "$AVAILABLE" == "true" ]]; then
            echo "[INFO] mgr is available again (gid $OLD_GID -> $GID)"
            return 0
        fi
        sleep 1
    done
    echo "[FAIL] mgr did not come back after $TIMEOUT seconds (old gid $OLD_GID)"
    return 1
}

get_enabled_modules() {
    ceph mgr module ls -f json | jq -r '
        ((.always_on_modules // []) - (.force_disabled_modules // [])) +
        (.enabled_modules // []) | unique | .[]'
}

get_can_run_modules() {
    ceph mgr module ls -f json | jq -r '
        (.disabled_modules // []) | map(select(.can_run == true) | .name) | .[]'
}

# Only the active mgr loads Python modules, standbys never have
# mgr_module_* perf keys. "ceph tell mgr" always goes to the active one.
validate_perf_counters() {
    local MODULE=$1
    local CMD_OUTPUT ALIVE

    for ((i = 0; i < TIMEOUT; i += 2)); do
        if CMD_OUTPUT=$(timeout 30 ceph tell mgr perf dump); then
            ALIVE=$(echo "$CMD_OUTPUT" | jq -r --arg k "mgr_module_${MODULE}" '.[$k].alive // empty')
            if [[ "$ALIVE" == "1" ]]; then
                echo "[INFO] Perf counters validated for module '$MODULE', alive=1"
                return 0
            fi
            echo "[DEBUG] '$MODULE' alive='$ALIVE', retrying"
        else
            echo "[DEBUG] perf dump failed, retrying"
        fi
        sleep 2
    done

    echo "[FAIL] Module '$MODULE' has no perf counters or is not alive after $TIMEOUT seconds"
    echo "$CMD_OUTPUT" | jq -r --arg k "mgr_module_${MODULE}" '.[$k] // "missing"' || true
    return 1
}

echo "Starting Ceph mgr module perf counter test"
echo "-------------------------------------------"

if [[ $# -gt 0 ]]; then
    MODULES="$*"
else
    MODULES="$DEFAULT_MODULES"
fi

INITIALLY_ENABLED=$(get_enabled_modules)
CAN_RUN=$(get_can_run_modules)
echo "[DEBUG] Initially enabled modules: $(echo $INITIALLY_ENABLED)"
echo "[DEBUG] Modules to cycle: $MODULES"

PASS=0
FAIL=0
SKIP=0

# Modules that are already enabled are validated in place, never disabled
for MODULE in $INITIALLY_ENABLED; do
    echo "[INFO] Testing enabled module: $MODULE"
    if validate_perf_counters "$MODULE"; then
        PASS=$((PASS+1))
    else
        FAIL=$((FAIL+1))
    fi
done

for MODULE in $MODULES; do
    if echo "$INITIALLY_ENABLED" | grep -qx "$MODULE"; then
        continue
    fi
    if ! echo "$CAN_RUN" | grep -qx "$MODULE"; then
        echo "[WARNING] Module '$MODULE' is not available or can't run, skipping"
        SKIP=$((SKIP+1))
        continue
    fi

    echo "[INFO] Testing module: $MODULE"
    CURRENT_MODULE="$MODULE"

    GID=$(get_active_gid)
    ceph mgr module enable "$MODULE"
    wait_for_mgr_respawn "$GID"

    if validate_perf_counters "$MODULE"; then
        PASS=$((PASS+1))
    else
        FAIL=$((FAIL+1))
    fi

    GID=$(get_active_gid)
    ceph mgr module disable "$MODULE"
    CURRENT_MODULE=""
    wait_for_mgr_respawn "$GID"
done

echo "-------------------------------------------"
echo "[SUMMARY] Passed: $PASS | Failed: $FAIL | Skipped: $SKIP"
if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
