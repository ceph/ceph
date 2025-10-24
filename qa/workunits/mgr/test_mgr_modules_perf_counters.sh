#!/bin/bash
set -e

trap 'echo "[ERROR] Script failed at line $LINENO."' ERR

# Function to retrieve modules
function get_modules() {
    local RAW_OUTPUT
    RAW_OUTPUT=$(ceph mgr module ls -f json 2>&1 | grep -v 'WARNING' | grep -v '^202')

    # Verify JSON structure
    echo "$RAW_OUTPUT" | jq empty > /dev/null 2>&1
    if [[ $? -ne 0 ]]; then
        echo "[ERROR] Output is not valid JSON. Output:"
        echo "$RAW_OUTPUT"
        exit 1
    fi

    # Extract module names
    MODULES=$(echo "$RAW_OUTPUT" | jq -r '
        (.enabled_modules // []) + 
        ((.disabled_modules // []) | map(select(.name != null) | .name)) |
        unique | .[]')

    if [[ -z "$MODULES" ]]; then
        echo "[ERROR] Failed to parse modules. Check JSON output."
        echo "$RAW_OUTPUT"
        exit 1
    fi

    echo "$MODULES"
}

# Function to validate perf counters
validate_perf_counters() {
    local MODULE=$1
    local MGR_IDS
    local CMD_OUTPUT
    local MGR
    local RETRIES=3
    local DELAY=2

    # Retrieve all active mgr daemon IDs
    MGR_IDS=$(ceph mgr dump --format json | jq -r '.active_name, .standbys[].name')

    if [[ -z "$MGR_IDS" ]]; then
        echo "[ERROR] No mgr daemons found."
        return 1
    fi

    # Loop over all mgr daemons
    for MGR in $MGR_IDS; do
        echo "[DEBUG] Checking perf counters on mgr.$MGR"

        local FOUND=0
        for ((i = 1; i <= RETRIES; i++)); do
            # Run the perf dump command
            CMD_OUTPUT=$(ceph daemon mgr.$MGR perf dump 2>/dev/null)
            if [[ -z "$CMD_OUTPUT" ]]; then
                echo "[ERROR] Failed to fetch perf dump from mgr.$MGR (attempt $i)"
                sleep $DELAY
                continue
            fi

            # Debug: Print available keys for clarity
            local KEYS=$(echo "$CMD_OUTPUT" | jq -r 'keys[]')
            echo "[DEBUG] Available perf keys: $KEYS"

            # Check for the presence of the module
            if echo "$KEYS" | grep -E "^mgr_module_${MODULE}(_|$)"; then
                ALIVE=$(echo "$CMD_OUTPUT" | jq -r ".mgr[\"$MODULE_KEY\"].alive")
                if [[ "$ALIVE" == "1" ]]; then
                    echo "[INFO] '$MODULE' is alive ✅"
                elif [[ "$ALIVE" == "0" ]]; then
                    echo "[WARNING] '$MODULE' is NOT alive (alive=0)"
                fi
                echo "[INFO] Perf counters validated for module '$MODULE' on mgr.$MGR"
                FOUND=1
                break
            else
                echo "[DEBUG] Module '$MODULE' not found in perf keys on mgr.$MGR (attempt $i)"
                sleep $DELAY
            fi
        done

        if [[ $FOUND -eq 0 ]]; then
            echo "[WARNING] No perf counters found for module '$MODULE' on mgr.$MGR after $RETRIES attempts"
        fi
    done
}



# Main Script Logic
echo "Starting Ceph mgr enable/disable test for all modules with perf counter validation"
echo "-------------------------------------------"

# call function to get list of modules
MODULES=$(get_modules)
echo "[DEBUG] List of modules: $MODULES"

for MODULE in $MODULES; do
    echo "[INFO] Testing module: $MODULE"

    # Enable the module
    echo "[DEBUG] Enabling module: $MODULE"
    if ceph mgr module enable "$MODULE" 2>/dev/null; then
        echo "[INFO] Module '$MODULE' enabled successfully."
    else
        echo "[WARNING] Failed to enable module '$MODULE'."
        continue
    fi
    sleep 5  # Allow some time for the mgr to initialize the module

    # Validate perf counters
    echo "[DEBUG] Validating perf counters for module: $MODULE"
    validate_perf_counters "$MODULE"

    # Disable the module
    echo "[DEBUG] Disabling module: $MODULE"
    if ceph mgr module disable "$MODULE" 2>/dev/null; then
        echo "[INFO] Module '$MODULE' disabled successfully."
    else
        echo "[WARNING] Failed to disable module '$MODULE'."
    fi
    
done

echo "[INFO] All modules tested with perf counter validation."
