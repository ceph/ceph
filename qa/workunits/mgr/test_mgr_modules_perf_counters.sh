#!/bin/bash
set -e

trap 'echo "[ERROR] Script failed at line $LINENO."' ERR

CURRENT_MODULE=""

cleanup() {
    if [[ -n "$CURRENT_MODULE" ]]; then
        echo "[CLEANUP] Disabling '$CURRENT_MODULE' due to unexpected exit" >&2
        ceph mgr module disable "$CURRENT_MODULE" 2>/dev/null || true
    fi
}
trap cleanup EXIT

get_modules() {
    local RAW_OUTPUT
    echo "[DEBUG] Fetching module list from mgr" >&2
    RAW_OUTPUT=$(ceph mgr module ls -f json 2>/dev/null)

    if ! echo "$RAW_OUTPUT" | jq empty > /dev/null 2>&1; then
        echo "[ERROR] Output is not valid JSON. Output:" >&2
        echo "$RAW_OUTPUT" >&2
        exit 1
    fi

    local MODULES
    # Skip disabled modules with can_run=false - they can't be enabled (missing deps)
    MODULES=$(echo "$RAW_OUTPUT" | jq -r '
        ((.enabled_modules // []) | map(if type == "string" then . else .name end)) +
        ((.disabled_modules // []) | map(select(.can_run == true) | .name)) |
        unique | .[]')

    if [[ -z "$MODULES" ]]; then
        echo "[ERROR] Failed to parse modules. Check JSON output." >&2
        echo "$RAW_OUTPUT" >&2
        exit 1
    fi

    echo "$MODULES"
}

get_enabled_modules() {
    ceph mgr module ls -f json 2>/dev/null | jq -r '
        (.enabled_modules // []) | map(if type == "string" then . else .name end) | .[]'
}

is_initially_enabled() {
    echo "$INITIALLY_ENABLED" | grep -qx "$1"
}

validate_perf_counters() {
    local MODULE=$1
    local CMD_OUTPUT
    local RETRIES=3
    local DELAY=2

    # Only the active mgr loads Python modules - standbys run MgrStandby and never
    # have mgr_module_* perf keys.
    local ACTIVE_MGR
    ACTIVE_MGR=$(ceph mgr dump --format json 2>/dev/null | jq -r '.active_name')

    if [[ -z "$ACTIVE_MGR" || "$ACTIVE_MGR" == "null" ]]; then
        echo "[ERROR] No active mgr daemon found."
        return 1
    fi

    echo "[DEBUG] Checking perf counters on mgr.$ACTIVE_MGR"

    local FOUND=0
    for ((i = 1; i <= RETRIES; i++)); do
        CMD_OUTPUT=$(ceph daemon mgr.$ACTIVE_MGR perf dump 2>/dev/null)
        if [[ -z "$CMD_OUTPUT" ]]; then
            echo "[ERROR] Failed to fetch perf dump from mgr.$ACTIVE_MGR (attempt $i)"
            sleep $DELAY
            continue
        fi

        local KEYS
        KEYS=$(echo "$CMD_OUTPUT" | jq -r 'keys[]')

        if echo "$KEYS" | grep -qE "^mgr_module_${MODULE}(_|$)"; then
            local ALIVE
            ALIVE=$(echo "$CMD_OUTPUT" | jq -r ".\"mgr_module_${MODULE}\".alive // empty")
            if [[ "$ALIVE" == "1" ]]; then
                echo "[INFO] '$MODULE' is alive"
            elif [[ "$ALIVE" == "0" ]]; then
                echo "[WARNING] '$MODULE' is NOT alive (alive=0)"
            fi
            echo "[INFO] Perf counters validated for module '$MODULE' on mgr.$ACTIVE_MGR"
            FOUND=1
            break
        else
            echo "[DEBUG] Module '$MODULE' not found in perf keys on mgr.$ACTIVE_MGR (attempt $i)"
            sleep $DELAY
        fi
    done

    if [[ $FOUND -eq 0 ]]; then
        echo "[FAIL] No perf counters found for module '$MODULE' on mgr.$ACTIVE_MGR after $RETRIES attempts"
        return 1
    fi
}

# Main Script Logic
echo "Starting Ceph mgr enable/disable test for all modules with perf counter validation"
echo "-------------------------------------------"

if [[ $# -gt 0 ]]; then
    MODULES="$*"
    echo "[DEBUG] Testing specified modules: $MODULES"
else
    MODULES=$(get_modules)
    echo "[DEBUG] List of modules: $MODULES"
fi

# Snapshot which modules are enabled now so we restore state correctly
INITIALLY_ENABLED=$(get_enabled_modules)

PASS=0
FAIL=0
SKIP=0

for MODULE in $MODULES; do
    echo "[INFO] Testing module: $MODULE"

    if is_initially_enabled "$MODULE"; then
        # Module was already enabled before the test - validate in place, don't disable it
        echo "[DEBUG] '$MODULE' was already enabled, validating without enable/disable cycle"
        if validate_perf_counters "$MODULE"; then
            PASS=$((PASS+1))
        else
            FAIL=$((FAIL+1))
        fi
        continue
    fi

    CURRENT_MODULE="$MODULE"

    if ! ceph mgr module enable "$MODULE" 2>/dev/null; then
        echo "[WARNING] Failed to enable module '$MODULE', skipping."
        CURRENT_MODULE=""
        SKIP=$((SKIP+1))
        continue
    fi
    echo "[INFO] Module '$MODULE' enabled successfully."

    sleep 5

    if validate_perf_counters "$MODULE"; then
        PASS=$((PASS+1))
    else
        FAIL=$((FAIL+1))
    fi

    if ceph mgr module disable "$MODULE" 2>/dev/null; then
        echo "[INFO] Module '$MODULE' disabled successfully."
    else
        echo "[WARNING] Failed to disable module '$MODULE'."
    fi
    CURRENT_MODULE=""
done

echo "-------------------------------------------"
echo "[SUMMARY] Passed: $PASS | Failed: $FAIL | Skipped: $SKIP"
if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
