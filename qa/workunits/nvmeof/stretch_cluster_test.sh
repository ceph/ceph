#!/bin/bash -e

# Stretch cluster (site-location) test.
#
# Exercises the NVMe-oF gateway/namespace "location" (siteA/siteB) feature, the
# location-aware ANA balancer, and the site disaster-recovery flow
# (disaster-set / disaster-clear + ANA failover/failback).
#
# Flow:
#   (0) Wait for all gateways to be AVAILABLE.
#   (1) Reset to default state: clear every gateway and namespace location and
#       wait for namespaces to redistribute evenly across all gateways.
#   (2) Change N gateway locations to siteA (last N, N random in 1..num_gws-1
#       unless overridden).
#   (3) Relocate half+1 of all namespaces to siteA.
#   (4) Verify the siteA namespaces are balanced across the siteA gateways
#       (each within +/- tolerance of expected = count / num_siteA_gateways).
#   (5+) siteA disaster-recovery: stop each siteA gateway one at a time and
#       verify ANA failover, then disaster-set siteA, restart the gateways (they
#       stay STANDBY), disaster-clear siteA, and wait for the ANA groups to fail
#       back to their home gateways (ACTIVE/OPTIMIZED).
#   (siteB) Move the first gateway to siteB; if a gateway with empty location
#       still exists, relocate the remaining namespaces to siteB, add a 2nd siteB
#       gateway, verify balanced siteB distribution, and run the same
#       disaster-recovery scenario for siteB.
#   Final: log the final gateway status, then reset to default state and wait for
#       all gateways AVAILABLE, leaving the cluster as it was at the start.
#
# Gateway daemons are stopped/started via 'ceph orch daemon stop/start <daemon>'.
# The orchestrator daemon name is the gw-id with the leading 'client.' stripped
# (cephadm builds gw-id as 'client.' + daemon_name); see gw_daemon_name().

source /etc/ceph/nvmeof.env

POOL="${RBD_POOL:-mypool}"
GROUP="${NVMEOF_GROUP:-mygroup0}"
LOCATION="${STRETCH_LOCATION:-siteA}"
TOLERANCE="${STRETCH_BALANCE_TOLERANCE:-2}"
NVMEOF_NATIVE_CLI="${NVMEOF_NATIVE_CLI:-false}"

# Retry knobs (seconds = retries * delay)
AVAIL_RETRIES="${STRETCH_AVAIL_RETRIES:-60}"
REDIST_RETRIES="${STRETCH_REDIST_RETRIES:-180}"
RETRY_DELAY="${STRETCH_RETRY_DELAY:-5}"

# Optional: override the number of gateways to move to siteA (default: random).
NUM_SITEA_GATEWAYS="${NUM_SITEA_GATEWAYS:-}"

if [ "$NVMEOF_NATIVE_CLI" = "true" ]; then
    SUBSYSTEM_FLAG="--nqn"
else
    SUBSYSTEM_FLAG="--subsystem"
fi

# Banner/formatting helpers (mirror the reference test's console style).
EQ_LINE="$(printf '=%.0s' $(seq 1 100))"
HASH_LINE="$(printf '#%.0s' $(seq 1 100))"
LINE_SIGN="$(printf '=%.0s' $(seq 1 130))"
ORDINALS=(1st 2nd 3rd 4th 5th 6th 7th 8th 9th 10th)

# Content of the previous 'ceph nvme-gw show' (epoch stripped) so show_gw_full can
# skip re-printing an identical dump when nothing changed between two call sites.
LAST_GW_SHOW_KEY=""

# Running step counter for the disaster-recovery / siteB phase (steps 0..4 are
# explicit; everything after uses bump_step so the numbering stays sequential).
STEP=0
bump_step () { STEP=$(( STEP + 1 )); }

# A major step banner: two lines of '=', the title, two more lines of '='.
step_banner () {
    echo
    echo "$EQ_LINE"
    echo "$EQ_LINE"
    echo "STRETCH CLUSTER - $*"
    echo "$EQ_LINE"
    echo "$EQ_LINE"
}

# A cycle/section banner using '#'.
section_banner () {
    echo
    echo "$HASH_LINE"
    echo "$HASH_LINE"
    echo "STRETCH CLUSTER - $*"
    echo "$HASH_LINE"
    echo "$HASH_LINE"
    echo
}

# A sub-step marker.
substep () {
    echo
    echo "==> $*"
}

# Transliterate Unicode box-drawing characters (used by the nvmeof-cli Rich
# tables and some ceph tables) into ASCII. Teuthology captures workunit stdout
# as latin-1, so raw UTF-8 box glyphs mojibake into garbled sequences in the
# log; converting them to '-', '|', '+' keeps the tables readable. ASCII passes
# through unchanged; any other non-ASCII byte becomes '?'.
ascii_box () {
    perl -CSD -pe '
        s/[\x{2500}\x{2550}]/-/g;
        s/[\x{2502}\x{2551}]/|/g;
        s/[\x{2500}-\x{257F}]/+/g;
        s/[^\x00-\x7F]/?/g;
    ' 2>/dev/null || cat
}

nvmeof_cli () {
    local server_ip="$1"
    shift
    for attempt in 1 2 3; do
        # The container CLI writes its (JSON) result to stderr; the '-t' pty merges
        # it onto stdout so $() can capture it (same pattern as setup_subsystem.sh).
        # 2>/dev/null drops only podman's own "input device is not a TTY" warning.
        if [ "$NVMEOF_NATIVE_CLI" = "true" ]; then
            ceph nvmeof --server-address "$server_ip" "$@" && return 0
        else
            sudo podman run -it "$NVMEOF_CLI_IMAGE" --server-address "$server_ip" --server-port "$NVMEOF_SRPORT" "$@" 2>/dev/null && return 0
        fi
        echo "    [WARN] cli attempt $attempt failed, retrying..." >&2
        sleep 2
    done
    return 1
}

gw_show () {
    ceph nvme-gw show "$POOL" "$GROUP" --format json
}

# Print the full 'ceph nvme-gw show' table (human-readable, non-JSON) for
# context, wrapped like the reference test's get_ceph_nvme_gw_show output.
# Takes an optional context label (shown next to the command) and an optional
# "force" flag; "force" prints the full table even if it is unchanged since the
# last call (used for the final status so it is always visible before the PASSED
# banner).
show_gw_full () {
    local ctx="$1" force="$2" out key
    out=$( { ceph nvme-gw show "$POOL" "$GROUP" 2>&1 || true; } | ascii_box )
    # Compare ignoring the (frequently-bumping) epoch lines so we only collapse
    # genuinely unchanged state, not epoch-only churn.
    key=$(printf '%s\n' "$out" | grep -vE '"(epoch|GW-epoch)"' || true)
    echo
    echo "$LINE_SIGN"
    echo "ceph nvme-gw show $POOL $GROUP${ctx:+ -- $ctx}"
    echo "$LINE_SIGN"
    if [ "$force" != "force" ] && [ -n "$key" ] && [ "$key" = "$LAST_GW_SHOW_KEY" ]; then
        echo "[unchanged since previous 'ceph nvme-gw show' -- output omitted]"
    else
        printf '%s\n' "$out"
        LAST_GW_SHOW_KEY="$key"
    fi
    echo
}

# Render the exact CLI command (as it will be run) as a string, so it can be
# echoed to the console like the reference test does for every command.
cli_command_str () {
    if [ "$NVMEOF_NATIVE_CLI" = "true" ]; then
        echo "ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS $*"
    else
        echo "sudo podman run -it $NVMEOF_CLI_IMAGE --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS --server-port $NVMEOF_SRPORT $*"
    fi
}

# Print the full namespace list (human-readable, non-JSON) for every subsystem,
# wrapped like show_gw_full. Takes an optional context label.
show_ns_full () {
    local ctx="$1" i nqn
    echo
    echo "$LINE_SIGN"
    echo "namespace list ($NVMEOF_SUBSYSTEMS_COUNT subsystems)${ctx:+ -- $ctx}"
    echo "$LINE_SIGN"
    for i in $(seq 1 "$NVMEOF_SUBSYSTEMS_COUNT"); do
        nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
        echo "-- subsystem $i ($nqn):"
        echo "    $(cli_command_str namespace list "$SUBSYSTEM_FLAG" "$nqn")"
        { nvmeof_cli "$NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" namespace list "$SUBSYSTEM_FLAG" "$nqn" || true; } | ascii_box
    done
    echo
}

# Change one namespace's location, emitting a single concise status line.
# The CLI's verbose JSON response is captured and only shown on failure.
ns_change_location () {
    local nqn="$1" nsid="$2" loc="$3" out
    # Echo the exact command being run (like the reference test), then execute.
    echo "    $(cli_command_str namespace change_location "$SUBSYSTEM_FLAG" "$nqn" --nsid "$nsid" --location "\"$loc\"")"
    if out=$(nvmeof_cli "$NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" namespace change_location "$SUBSYSTEM_FLAG" "$nqn" --nsid "$nsid" --location "$loc" 2>&1); then
        echo "    - $nqn nsid=$nsid -> ${loc:-<none>}"
    else
        echo "    [FAIL] $nqn nsid=$nsid -> ${loc:-<none>} FAILED:" >&2
        echo "$out" | sed 's/^/       /' >&2
    fi
    return 0
}

# (0) Poll 'nvme-gw show' until every gateway reports Availability=AVAILABLE.
wait_for_gateways_available () {
    local out total avail unavail attempt
    substep "Waiting for all gateways to become AVAILABLE (poll only, no action)"
    for ((attempt=1; attempt<=AVAIL_RETRIES; attempt++)); do
        out=$(gw_show 2>/dev/null || true)
        total=$(echo "$out" | jq '.["Created Gateways:"] | length' 2>/dev/null || echo 0)
        avail=$(echo "$out" | jq '[.["Created Gateways:"][] | select(.Availability=="AVAILABLE")] | length' 2>/dev/null || echo 0)
        if [ "$total" -gt 0 ] && [ "$avail" -eq "$total" ]; then
            echo "    [OK] All $total gateways are AVAILABLE after $attempt attempt(s)"
            return 0
        fi
        unavail=$(( total - avail ))
        if (( attempt % 10 == 0 )); then
            echo "    Attempt $attempt/$AVAIL_RETRIES: $unavail gateway(s) still UNAVAILABLE, waiting..."
            show_gw_full "availability poll, attempt $attempt/$AVAIL_RETRIES"
        fi
        sleep "$RETRY_DELAY"
    done
    echo "    [WARN] Timeout: Not all gateways became AVAILABLE after $(( AVAIL_RETRIES * RETRY_DELAY )) seconds" >&2
    show_gw_full "at availability timeout"
    return 1
}

# (1)/(5) Clear every gateway and namespace location, then wait for namespaces to
# redistribute evenly across all gateways. Takes the step label, e.g. "(1)".
reset_to_default_state () {
    local step_label="$1"
    local out i nqn list nsids nsid gw_id loc expected_even sum balanced attempt

    step_banner "$step_label Resetting all gateway and namespace locations to default state"
    show_gw_full "$step_label state before reset"

    substep "Reset gateway locations"
    out=$(gw_show)
    echo "$out" | jq -r '.["Created Gateways:"][] | select((.location // "") != "") | .["gw-id"] + " " + .location' | \
    while read -r gw_id loc; do
        echo "    - $gw_id: location '$loc' -> <none>"
        echo "    ceph nvme-gw set-location $gw_id $POOL $GROUP \"\""
        ceph nvme-gw set-location "$gw_id" "$POOL" "$GROUP" '' >/dev/null 2>&1 || true
    done

    substep "Reset namespace locations"
    for i in $(seq 1 "$NVMEOF_SUBSYSTEMS_COUNT"); do
        nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
        echo "    $(cli_command_str --format json namespace list "$SUBSYSTEM_FLAG" "$nqn")"
        list=$(nvmeof_cli "$NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" --format json namespace list "$SUBSYSTEM_FLAG" "$nqn")
        nsids=$(echo "$list" | jq -r '.namespaces[] | select((.location // "") != "" and (.location // "") != "<N/A>") | .nsid')
        for nsid in $nsids; do
            ns_change_location "$nqn" "$nsid" ""
        done
    done

    substep "Wait for namespace redistribution to complete"
    expected_even=$(( TOTAL_NS / NUM_GWS ))
    echo "    Total namespaces: $TOTAL_NS, Gateways: $NUM_GWS, Expected per gateway: $expected_even"
    for ((attempt=1; attempt<=REDIST_RETRIES; attempt++)); do
        out=$(gw_show 2>/dev/null || true)
        sum=$(echo "$out" | jq '[.["Created Gateways:"][] | (.["num-namespaces"] // 0)] | add // 0' 2>/dev/null || echo 0)
        balanced=$(echo "$out" | jq --argjson exp "$expected_even" --argjson tol "$TOLERANCE" \
            '[.["Created Gateways:"][] | (.["num-namespaces"] // 0) | ((. - $exp) | if . < 0 then -. else . end) <= $tol] | all' 2>/dev/null || echo false)
        if [ "$balanced" = "true" ] && [ "$sum" -eq "$TOTAL_NS" ]; then
            echo "    [OK] Even distribution reached (~$expected_even per gateway) after $attempt attempt(s)"
            show_gw_full "after even redistribution"
            substep "Cleanup completed"
            return 0
        fi
        if (( attempt % 10 == 0 )); then
            echo "    Attempt $attempt/$REDIST_RETRIES: sum=$sum/$TOTAL_NS balanced=$balanced, waiting..."
            show_gw_full "redistribution poll, attempt $attempt/$REDIST_RETRIES"
        fi
        sleep "$RETRY_DELAY"
    done
    echo "    [WARN] Namespace redistribution did not complete within timeout, but continuing test..." >&2
    show_gw_full "after redistribution timeout"
    substep "Cleanup completed"
    return 0
}

# Strict final assertion run right before the PASSED banner. reset_to_default_state
# only WARNs (and returns 0) if redistribution times out, so an unbalanced or
# degraded end state would otherwise slip through as PASSED. This re-checks the
# restored-to-default state and FAILS the test unless EVERY gateway is AVAILABLE,
# has no location set, and namespaces are evenly distributed (each within
# +/-TOLERANCE of TOTAL_NS/NUM_GWS, summing to TOTAL_NS).
verify_final_state () {
    local out total avail sum balanced badloc expected_even gid status cnt loc diff absdiff ok=1
    expected_even=$(( TOTAL_NS / NUM_GWS ))
    substep "Verifying final restored-to-default state (strict)"
    echo "    Expecting: $NUM_GWS gateways AVAILABLE, no location, ~$expected_even ns each (+/-$TOLERANCE), sum $TOTAL_NS"
    out=$(gw_show 2>/dev/null || true)
    total=$(echo "$out" | jq '.["Created Gateways:"] | length' 2>/dev/null || echo 0)
    avail=$(echo "$out" | jq '[.["Created Gateways:"][] | select(.Availability=="AVAILABLE")] | length' 2>/dev/null || echo 0)
    sum=$(echo "$out" | jq '[.["Created Gateways:"][] | (.["num-namespaces"] // 0)] | add // 0' 2>/dev/null || echo 0)
    balanced=$(echo "$out" | jq --argjson exp "$expected_even" --argjson tol "$TOLERANCE" \
        '[.["Created Gateways:"][] | (.["num-namespaces"] // 0) | ((. - $exp) | if . < 0 then -. else . end) <= $tol] | all' 2>/dev/null || echo false)
    badloc=$(echo "$out" | jq '[.["Created Gateways:"][] | select((.location // "") != "")] | length' 2>/dev/null || echo 0)

    echo "$out" | jq -r '.["Created Gateways:"][] | "\(.["gw-id"]) \(.Availability) \(.["num-namespaces"] // 0) \(.location // "")"' | \
    while read -r gid status cnt loc; do
        diff=$(( cnt - expected_even )); absdiff=${diff#-}
        if [ "$status" = AVAILABLE ] && [ "$absdiff" -le "$TOLERANCE" ] && [ -z "$loc" ]; then
            printf '    [OK] %s: avail=%s ns=%s loc=%s\n' "$gid" "$status" "$cnt" "${loc:-<none>}"
        else
            printf '    [xx] %s: avail=%s ns=%s loc=%s\n' "$gid" "$status" "$cnt" "${loc:-<none>}"
        fi
    done

    if [ "${total:-0}" -ne "$NUM_GWS" ] || [ "${avail:-0}" -ne "${total:-0}" ]; then
        echo "[FAIL] TEST FAILED: expected $NUM_GWS gateways all AVAILABLE, got $avail/$total AVAILABLE" >&2
        ok=0
    fi
    if [ "${badloc:-0}" -ne 0 ]; then
        echo "[FAIL] TEST FAILED: $badloc gateway(s) still have a location set (expected none)" >&2
        ok=0
    fi
    if [ "$balanced" != "true" ] || [ "${sum:-0}" -ne "$TOTAL_NS" ]; then
        echo "[FAIL] TEST FAILED: namespaces not evenly balanced (sum=$sum/$TOTAL_NS, within-tolerance=$balanced)" >&2
        ok=0
    fi
    [ "$ok" -eq 1 ] || return 1
    echo "    [OK] Final state verified: $avail/$total AVAILABLE, no locations, balanced (sum $sum)"
    return 0
}

# Relocate 'to_change' namespaces that currently have no location to 'loc', walking
# subsystems from last to first (matches the reference test). Emits the exact CLI
# command for every 'namespace list' and a concise line per relocated namespace.
relocate_namespaces_to () {
    local loc="$1" to_change="$2"
    local i nqn list nsids nsid n_here changed=0
    for ((i=NVMEOF_SUBSYSTEMS_COUNT; i>=1; i--)); do
        [ "$changed" -ge "$to_change" ] && break
        nqn="${NVMEOF_SUBSYSTEMS_PREFIX}${i}"
        echo "    $(cli_command_str --format json namespace list "$SUBSYSTEM_FLAG" "$nqn")"
        list=$(nvmeof_cli "$NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" --format json namespace list "$SUBSYSTEM_FLAG" "$nqn")
        nsids=$(echo "$list" | jq -r '.namespaces[] | select((.location // "") == "" or (.location // "") == "<N/A>") | .nsid')
        n_here=0
        for nsid in $nsids; do
            [ "$changed" -ge "$to_change" ] && break
            ns_change_location "$nqn" "$nsid" "$loc"
            changed=$(( changed + 1 ))
            n_here=$(( n_here + 1 ))
        done
        [ "$n_here" -gt 0 ] && echo "    Subsystem $i ($nqn): changed $n_here namespace(s) to $loc"
    done
    if [ "$changed" -ne "$to_change" ]; then
        echo "[FAIL] TEST FAILED: relocated $changed namespaces to $loc, expected $to_change" >&2
        return 1
    fi
    echo
    echo "[OK] All $changed namespace(s) location changed to '$loc'"
    return 0
}

# Poll until the namespaces with location 'loc' are balanced across the 'num_site'
# gateways holding that location: each within +/- TOLERANCE of expected =
# expected_total / num_site, and the per-location total equals expected_total.
# Returns non-zero on timeout.
verify_site_balance () {
    local loc="$1" expected_total="$2" num_site="$3"
    local expected_per_gw=$(( expected_total / num_site ))
    local attempt out sum balanced gid cnt diff absdiff status
    substep "Verifying balanced distribution of namespaces with location '$loc'"
    echo "    Target gateways: $num_site"
    echo "    Expected total: $expected_total namespaces"
    echo "    Expected per gateway: $expected_per_gw (+/-$TOLERANCE)"
    for ((attempt=1; attempt<=REDIST_RETRIES; attempt++)); do
        out=$(gw_show 2>/dev/null || true)
        sum=$(echo "$out" | jq --arg loc "$loc" \
            '[.["Created Gateways:"][] | select(.location==$loc) | (.["num-namespaces"] // 0)] | add // 0' 2>/dev/null || echo 0)
        balanced=$(echo "$out" | jq --arg loc "$loc" --argjson exp "$expected_per_gw" --argjson tol "$TOLERANCE" \
            '[.["Created Gateways:"][] | select(.location==$loc) | (.["num-namespaces"] // 0) | ((. - $exp) | if . < 0 then -. else . end) <= $tol] | all' 2>/dev/null || echo false)
        if [ "$balanced" = "true" ] && [ "$sum" -eq "$expected_total" ]; then
            echo
            echo "[OK] Balanced distribution achieved after $attempt attempt(s)"
            echo "   Total namespaces with location '$loc': $sum"
            echo "$out" | jq -r --arg loc "$loc" '.["Created Gateways:"][] | select(.location==$loc) | "\(.["gw-id"]) \(.["num-namespaces"] // 0)"' | \
            while read -r gid cnt; do
                echo "   - $gid: $cnt namespaces (within $expected_per_gw+/-$TOLERANCE)"
            done
            return 0
        fi
        if (( attempt % 10 == 0 )); then
            echo "  Attempt $attempt/$REDIST_RETRIES: Waiting for balanced distribution"
            echo "    Total: $sum/$expected_total"
            echo "$out" | jq -r --arg loc "$loc" '.["Created Gateways:"][] | select(.location==$loc) | "\(.["gw-id"]) \(.["num-namespaces"] // 0)"' | \
            while read -r gid cnt; do
                diff=$(( cnt - expected_per_gw )); absdiff=${diff#-}
                if [ "$absdiff" -le "$TOLERANCE" ]; then status="[ok]"; else status="[xx]"; fi
                echo "    $status $gid: $cnt (expected $expected_per_gw+/-$TOLERANCE)"
            done
            show_gw_full "$loc balance poll, attempt $attempt/$REDIST_RETRIES"
        fi
        sleep "$RETRY_DELAY"
    done
    echo "[FAIL] TEST FAILED: $loc namespaces are not evenly balanced across $loc gateways" >&2
    show_gw_full "at $loc balance failure"
    return 1
}

# --- Disaster-recovery helpers ------------------------------------------------

# The orchestrator daemon name is the gw-id with the leading 'client.' stripped
# (cephadm builds gw-id as 'client.' + daemon_name).
gw_daemon_name () { echo "${1#client.}"; }

# Count ACTIVE/OPTIMIZED ANA groups in a gateway's "ana states" string.
# Used via jq below; kept here as documentation of the field format, e.g.
#   " 1: ACTIVE ,  2: STANDBY ,  3: ACTIVE ,..."

# Poll 'ceph orch ps' until the daemon for the given gw-id reaches the wanted state
# (running|stopped). Best-effort: warns (does not fail) on timeout.
wait_daemon_state () {
    local gw_id="$1" want="$2" dname attempt state
    dname=$(gw_daemon_name "$gw_id")
    for ((attempt=1; attempt<=AVAIL_RETRIES; attempt++)); do
        state=$(ceph orch ps --daemon-type nvmeof --refresh --format json 2>/dev/null \
            | jq -r --arg d "$dname" '.[] | select(.daemon_name==$d) | .status_desc' 2>/dev/null | head -1)
        if [ "$want" = "stopped" ] && { [ "$state" = "stopped" ] || [ "$state" = "error" ]; }; then
            echo "    [ok] daemon $dname is $state"
            return 0
        fi
        if [ "$want" = "running" ] && [ "$state" = "running" ]; then
            echo "    [ok] daemon $dname is running"
            return 0
        fi
        sleep "$RETRY_DELAY"
    done
    echo "    [WARN] daemon $dname did not reach state '$want' (last: ${state:-unknown})" >&2
    return 1
}

# Stop the gateway daemon backing the given gw-id and wait until it is stopped.
stop_gw_daemon () {
    local gw_id="$1" dname
    dname=$(gw_daemon_name "$gw_id")
    if ! ceph orch ps --daemon-type nvmeof --format json 2>/dev/null \
        | jq -e --arg d "$dname" 'any(.[]; .daemon_name==$d)' >/dev/null 2>&1; then
        echo "    [WARN] derived daemon '$dname' not found in 'ceph orch ps' for gw-id $gw_id" >&2
    fi
    echo "    ceph orch daemon stop $dname"
    ceph orch daemon stop "$dname"
    # Best-effort: the ANA-failover wait below is the real assertion.
    wait_daemon_state "$gw_id" stopped || true
}

# Start the gateway daemon backing the given gw-id and wait until it is running.
start_gw_daemon () {
    local gw_id="$1" dname
    dname=$(gw_daemon_name "$gw_id")
    echo "    ceph orch daemon start $dname"
    ceph orch daemon start "$dname"
    wait_daemon_state "$gw_id" running || true
}

# Sanity check that the top-level namespace count equals the sum of per-gateway
# counts. Best-effort: warns (does not fail) on mismatch.
verify_gw_ns_count () {
    local out total sum
    out=$(gw_show 2>/dev/null || true)
    total=$(echo "$out" | jq '.["num-namespaces"] // 0' 2>/dev/null || echo 0)
    sum=$(echo "$out" | jq '[.["Created Gateways:"][] | (.["num-namespaces"] // 0)] | add // 0' 2>/dev/null || echo 0)
    if [ "$total" = "$sum" ]; then
        echo "    [ok] Namespace count consistent: total=$total == sum(per-gw)=$sum"
        return 0
    fi
    echo "    [WARN] Namespace count mismatch: total=$total sum(per-gw)=$sum" >&2
    return 1
}

# Wait until exactly 'expected_count' AVAILABLE gateways hold >= 'expected_ana'
# ACTIVE/OPTIMIZED ANA groups. 'filter' (optional) is a space-separated allow-list
# of gw-ids to count (others are ignored); empty means count all AVAILABLE
# gateways. 'desc' is a description.
wait_for_ana_failover () {
    local expected_count="$1" expected_ana="$2" filter="$3" desc="$4"
    local attempt out matches gid avail cnt
    substep "Waiting for ANA group failover ($desc)"
    echo "    Expecting exactly $expected_count gateway(s) with >= $expected_ana ACTIVE/OPTIMIZED ANA group(s)"
    [ -n "$filter" ] && echo "    Counting only among gateways: $filter"
    for ((attempt=1; attempt<=REDIST_RETRIES; attempt++)); do
        out=$(gw_show 2>/dev/null || true)
        matches=0
        while IFS=$'\t' read -r gid avail cnt; do
            [ "$avail" = "AVAILABLE" ] || continue
            if [ -n "$filter" ]; then
                case " $filter " in *" $gid "*) ;; *) continue ;; esac
            fi
            [ "${cnt:-0}" -ge "$expected_ana" ] && matches=$(( matches + 1 ))
        done < <(echo "$out" | jq -r '.["Created Gateways:"][] | [.["gw-id"], .Availability, (.["ana states"] // "" | [scan("ACTIVE|OPTIMIZED")] | length)] | @tsv' 2>/dev/null)
        if [ "$matches" -eq "$expected_count" ]; then
            echo "    [OK] Failover complete: $matches gateway(s) with >= $expected_ana ACTIVE/OPTIMIZED (attempt $attempt)"
            return 0
        fi
        if (( attempt % 10 == 0 )); then
            echo "    Attempt $attempt/$REDIST_RETRIES: $matches/$expected_count gateway(s) hold >= $expected_ana ANA group(s), waiting..."
            show_gw_full "ANA failover poll ($desc), attempt $attempt/$REDIST_RETRIES"
        fi
        sleep "$RETRY_DELAY"
    done
    echo "    [FAIL] TEST FAILED: expected $expected_count gateway(s) with >= $expected_ana ANA groups ($desc)" >&2
    show_gw_full "at ANA failover timeout ($desc)"
    return 1
}

# Stop the given gateway and verify ANA failover to 'expected_count' gateway(s)
# (allow-list 'filter', description 'desc').
stop_gw_and_verify_failover () {
    local gw_id="$1" expected_count="$2" filter="$3" desc="$4"
    stop_gw_daemon "$gw_id"
    echo "    [OK] $gw_id stopped"
    verify_gw_ns_count || true
    wait_for_ana_failover "$expected_count" 2 "$filter" "$desc"
}

# Start a gateway during a disaster (after disaster-set): it should come back
# AVAILABLE but its ANA groups stay STANDBY until disaster-clear, so we do NOT
# wait for failback here.
start_gw_disaster () {
    local gw_id="$1"
    substep "Starting gateway $gw_id (disaster state: ANA groups stay STANDBY until disaster-clear)"
    start_gw_daemon "$gw_id"
    sleep 10
    echo "$(gw_show 2>/dev/null || true)" | jq -r --arg g "$gw_id" \
        '.["Created Gateways:"][] | select(.["gw-id"]==$g) | "    - \(.["gw-id"]): Availability=\(.Availability) num-namespaces=\(.["num-namespaces"] // 0) ana=\(.["ana states"] // "")"' 2>/dev/null || true
    verify_gw_ns_count || true
}

# After disaster-clear, wait until every site gateway (space-separated gw-ids in
# $*) has at least one ACTIVE/OPTIMIZED ANA group (groups returned home).
wait_for_ana_recovery () {
    local site_ids="$*"
    local attempt out id cnt allok
    substep "Waiting for ANA groups to return to home gateways after disaster-clear"
    echo "    Home gateways: $site_ids"
    for ((attempt=1; attempt<=REDIST_RETRIES; attempt++)); do
        out=$(gw_show 2>/dev/null || true)
        allok=true
        for id in $site_ids; do
            cnt=$(echo "$out" | jq -r --arg g "$id" \
                '.["Created Gateways:"][] | select(.["gw-id"]==$g) | (.["ana states"] // "" | [scan("ACTIVE|OPTIMIZED")] | length)' 2>/dev/null | head -1)
            [ "${cnt:-0}" -ge 1 ] || { allok=false; break; }
        done
        if [ "$allok" = "true" ]; then
            echo "    [OK] ANA recovery complete: all home gateways hold ACTIVE/OPTIMIZED ANA groups (attempt $attempt)"
            return 0
        fi
        if (( attempt % 10 == 0 )); then
            echo "    Attempt $attempt/$REDIST_RETRIES: waiting for ANA groups to return home..."
            show_gw_full "ANA recovery poll, attempt $attempt/$REDIST_RETRIES"
        fi
        sleep "$RETRY_DELAY"
    done
    echo "    [FAIL] TEST FAILED: ANA groups did not return to home gateways within timeout" >&2
    show_gw_full "at ANA recovery timeout"
    return 1
}

# Poll until every listed gw-id is reported UNAVAILABLE (not AVAILABLE) in
# 'ceph nvme-gw show'. The mon only accepts 'disaster-set' once all gateways in
# the location have left the AVAILABLE state, which lags stopping the daemon by
# the beacon-grace period. Waiting here avoids the disaster-set race that fails
# with EINVAL ("command cannot be executed") when a site gateway is still
# AVAILABLE. Returns non-zero on timeout.
wait_gws_unavailable () {
    local ids=("$@")
    local attempt out still gw_id st
    substep "Waiting for ${#ids[@]} gateway(s) to be reported UNAVAILABLE by the mon"
    for ((attempt=1; attempt<=AVAIL_RETRIES; attempt++)); do
        out=$(gw_show 2>/dev/null || true)
        still=0
        for gw_id in "${ids[@]}"; do
            st=$(echo "$out" | jq -r --arg g "$gw_id" '.["Created Gateways:"][] | select(.["gw-id"]==$g) | .Availability' 2>/dev/null | head -1)
            [ "$st" = "AVAILABLE" ] && still=$(( still + 1 ))
        done
        if [ "$still" -eq 0 ]; then
            echo "    [OK] All ${#ids[@]} gateway(s) are UNAVAILABLE after $attempt attempt(s)"
            return 0
        fi
        if (( attempt % 5 == 0 )); then
            echo "    Attempt $attempt/$AVAIL_RETRIES: $still gateway(s) still AVAILABLE, waiting..."
        fi
        sleep "$RETRY_DELAY"
    done
    echo "    [FAIL] TEST FAILED: not all gateways became UNAVAILABLE before disaster-set" >&2
    show_gw_full "at unavailable-wait timeout"
    return 1
}

# Full disaster-recovery scenario for a location. Takes the location followed by
# the gw-ids currently holding that location. Stops each site gateway (verifying
# ANA failover), disaster-set, restarts them (STANDBY), disaster-clear, and waits
# for ANA groups to fail back home.
run_site_dr () {
    local loc="$1"; shift
    local site_ids=("$@")
    local num_site=${#site_ids[@]}
    local expected_remaining=$(( NUM_GWS - num_site ))
    local i j gw_id others remaining_site stopped_so_far expected_count

    bump_step
    step_banner "($STEP) $loc disaster-recovery scenario ($num_site $loc gateway(s), $expected_remaining non-$loc)"
    show_gw_full "($STEP) $loc DR: state before stopping gateways"

    # Stop each site gateway one at a time, verifying ANA failover after each.
    for ((i=0; i<num_site; i++)); do
        gw_id="${site_ids[$i]}"
        stopped_so_far=$(( i + 1 ))
        others=""; remaining_site=0
        for ((j=i+1; j<num_site; j++)); do
            others="$others ${site_ids[$j]}"
            remaining_site=$(( remaining_site + 1 ))
        done
        if [ "$remaining_site" -gt 0 ]; then
            # Other same-location gateways are still up: groups fail over to them.
            expected_count=$(( remaining_site < stopped_so_far ? remaining_site : stopped_so_far ))
            substep "Stop $loc gateway $gw_id and verify ANA failover to remaining $loc gateway(s) [$stopped_so_far/$num_site]"
            stop_gw_and_verify_failover "$gw_id" "$expected_count" "$others" "after stopping $gw_id"
        else
            # Last same-location gateway: groups fail over to non-$loc gateways.
            if [ "$i" -eq 0 ]; then
                expected_count=1
            else
                expected_count=$(( expected_remaining < stopped_so_far ? expected_remaining : stopped_so_far ))
            fi
            substep "Stop last $loc gateway $gw_id and verify ANA failover to non-$loc gateway(s) [$stopped_so_far/$num_site]"
            stop_gw_and_verify_failover "$gw_id" "$expected_count" "" "after stopping last $loc gw $gw_id"
        fi
    done

    bump_step
    step_banner "($STEP) Setting disaster-set for $loc"
    # The mon rejects disaster-set while any gateway in the location is still
    # AVAILABLE, so wait for them all to age out to UNAVAILABLE first.
    wait_gws_unavailable "${site_ids[@]}" || exit 1
    echo "    ceph nvme-gw disaster-set $POOL $GROUP $loc"
    ceph nvme-gw disaster-set "$POOL" "$GROUP" "$loc"
    show_gw_full "after disaster-set $loc"

    bump_step
    step_banner "($STEP) Starting each $loc gateway (ANA groups stay STANDBY until disaster-clear)"
    for gw_id in "${site_ids[@]}"; do
        start_gw_disaster "$gw_id"
    done

    bump_step
    step_banner "($STEP) Execute disaster-clear to trigger ANA group recovery for $loc"
    echo "    ceph nvme-gw disaster-clear $POOL $GROUP $loc"
    ceph nvme-gw disaster-clear "$POOL" "$GROUP" "$loc"
    show_gw_full "after disaster-clear $loc"

    bump_step
    step_banner "($STEP) Wait for $loc ANA groups to return to home gateways after disaster-clear"
    wait_for_ana_recovery "${site_ids[@]}"
    echo "    [OK] disaster-clear complete, ANA groups returned home for all $num_site $loc gateway(s)"
    show_gw_full "$loc DR complete"
}

# Poll until the given gateway reports exactly 'expected' namespaces (after a
# location change).
wait_gw_ns_redistribution () {
    local gw_id="$1" expected="$2" attempt out cnt
    substep "Waiting for namespace redistribution ($gw_id -> $expected namespaces)"
    for ((attempt=1; attempt<=REDIST_RETRIES; attempt++)); do
        out=$(gw_show 2>/dev/null || true)
        cnt=$(echo "$out" | jq -r --arg g "$gw_id" \
            '.["Created Gateways:"][] | select(.["gw-id"]==$g) | (.["num-namespaces"] // 0)' 2>/dev/null | head -1)
        if [ "${cnt:-x}" = "$expected" ]; then
            echo "    [OK] $gw_id reached $expected namespace(s) after $attempt attempt(s)"
            return 0
        fi
        if (( attempt % 10 == 0 )); then
            echo "    Attempt $attempt/$REDIST_RETRIES: $gw_id has ${cnt:-?} namespaces (expected $expected), waiting..."
            show_gw_full "ns redistribution poll ($gw_id), attempt $attempt/$REDIST_RETRIES"
        fi
        sleep "$RETRY_DELAY"
    done
    echo "    [FAIL] TEST FAILED: $gw_id did not reach $expected namespaces within timeout" >&2
    show_gw_full "at ns redistribution timeout ($gw_id)"
    return 1
}

# Poll until the given gateway reports a namespace count in the inclusive range
# [lo,hi].
wait_gw_ns_in_range () {
    local gw_id="$1" lo="$2" hi="$3" attempt out cnt
    substep "Waiting for namespace rebalancing ($gw_id -> $lo..$hi namespaces)"
    for ((attempt=1; attempt<=REDIST_RETRIES; attempt++)); do
        out=$(gw_show 2>/dev/null || true)
        cnt=$(echo "$out" | jq -r --arg g "$gw_id" \
            '.["Created Gateways:"][] | select(.["gw-id"]==$g) | (.["num-namespaces"] // 0)' 2>/dev/null | head -1)
        if [ -n "${cnt:-}" ] && [ "$cnt" -ge "$lo" ] && [ "$cnt" -le "$hi" ]; then
            echo "    [OK] $gw_id reached $cnt namespace(s) (range $lo-$hi) after $attempt attempt(s)"
            return 0
        fi
        if (( attempt % 10 == 0 )); then
            echo "    Attempt $attempt/$REDIST_RETRIES: $gw_id has ${cnt:-?} namespaces (expected $lo-$hi), waiting..."
            show_gw_full "ns rebalancing poll ($gw_id), attempt $attempt/$REDIST_RETRIES"
        fi
        sleep "$RETRY_DELAY"
    done
    echo "    [FAIL] TEST FAILED: $gw_id did not reach $lo-$hi namespaces within timeout" >&2
    show_gw_full "at ns rebalancing timeout ($gw_id)"
    return 1
}

# Assign a gateway to a location and block until its namespace count settles at
# the value that new location implies.
#
# Idea: moving a gateway into a location changes how many namespaces it should
# serve, and the mon takes time to redistribute. This helper applies the change
# and then polls until the count matches the expectation, so callers can treat
# the relocation as a single synchronous, assertable step.
#
# Returns the wait's exit status so callers can chain '|| exit 1'.
set_gw_location_and_wait () {
    local gw_id="$1" loc="$2" total_at_loc="${3:-}"
    local out cur all_others_site expected_ns rc n_at_loc exp lo hi loc_lc

    loc_lc=$(printf '%s' "$loc" | tr '[:upper:]' '[:lower:]')
    out=$(gw_show 2>/dev/null || true)
    cur=$(echo "$out" | jq -r --arg g "$gw_id" '.["Created Gateways:"][] | select(.["gw-id"]==$g) | (.["num-namespaces"] // 0)' 2>/dev/null | head -1)

    echo "    ceph nvme-gw set-location $gw_id $POOL $GROUP $loc"
    ceph nvme-gw set-location "$gw_id" "$POOL" "$GROUP" "$loc"

    if [ -n "$total_at_loc" ]; then
        # REBALANCING: count the gateways now at this location (this one included).
        n_at_loc=$(gw_show 2>/dev/null | jq --arg loc "$loc" '[.["Created Gateways:"][] | select(.location==$loc)] | length' 2>/dev/null || echo 1)
        [ "${n_at_loc:-0}" -lt 1 ] && n_at_loc=1
        exp=$(( total_at_loc / n_at_loc ))
        lo=$(( exp - 1 )); [ "$lo" -lt 0 ] && lo=0
        hi=$(( exp + 1 ))
        echo "    REBALANCING expected: $total_at_loc '$loc' namespace(s) shared across $n_at_loc '$loc' gateway(s); this gateway should hold ~$exp (range $lo-$hi)"
        wait_gw_ns_in_range "$gw_id" "$lo" "$hi"
        rc=$?
    else
        expected_ns=0
        if [ "$loc_lc" = "siteb" ]; then
            all_others_site=$(echo "$out" | jq -r --arg g "$gw_id" '[.["Created Gateways:"][] | select(.["gw-id"]!=$g) | (.location // "")] | all(. == "siteA")' 2>/dev/null || echo false)
            if [ "$all_others_site" = "true" ]; then
                expected_ns="${cur:-0}"
                echo "    [WARN] siteB workaround: all other gateways are siteA; namespaces cannot redistribute, expecting count to stay $expected_ns"
            else
                echo "    DRAINAGE expected: this gateway should drain to 0"
            fi
        else
            echo "    DRAINAGE expected: this gateway should drain to 0"
        fi
        wait_gw_ns_redistribution "$gw_id" "$expected_ns"
        rc=$?
    fi

    verify_gw_ns_count || true
    return "$rc"
}

TOTAL_NS=$(( NVMEOF_SUBSYSTEMS_COUNT * NVMEOF_NAMESPACES_COUNT ))

section_banner "Starting stretch cluster test"

# Discover gateways.
mapfile -t GW_IDS < <(gw_show | jq -r '.["Created Gateways:"][] | .["gw-id"]')
NUM_GWS=${#GW_IDS[@]}
echo "==> Discovered $NUM_GWS gateways, $TOTAL_NS namespaces ($NVMEOF_SUBSYSTEMS_COUNT subsystems x $NVMEOF_NAMESPACES_COUNT)"

# Requirements matching the reference stretch cluster test.
if [ "$NUM_GWS" -lt 3 ]; then
    echo "[FAIL] TEST FAILED: requires at least 3 gateways, found $NUM_GWS" >&2
    exit 1
fi
if [ "$TOTAL_NS" -lt 12 ]; then
    echo "[FAIL] TEST FAILED: requires at least 12 namespaces, found $TOTAL_NS" >&2
    exit 1
fi

#
# (0) Wait for all gateways AVAILABLE
#
step_banner "(0) Waiting for all gateways to be AVAILABLE"
show_gw_full "(0) initial state"
wait_for_gateways_available

#
# (1) Reset to default state
#
reset_to_default_state "(1)"

#
# (2) Change N gateway locations to siteA
#
if [ -n "$NUM_SITEA_GATEWAYS" ]; then
    num_siteA="$NUM_SITEA_GATEWAYS"
else
    num_siteA=$(( RANDOM % (NUM_GWS - 1) + 1 ))
fi
if [ "$num_siteA" -lt 1 ] || [ "$num_siteA" -gt $(( NUM_GWS - 1 )) ]; then
    echo "[FAIL] TEST FAILED: num_siteA=$num_siteA out of range 1..$(( NUM_GWS - 1 ))" >&2
    exit 1
fi

step_banner "(2) Changing $num_siteA of $NUM_GWS gateway location(s) to $LOCATION"
show_gw_full "(2) state before tagging gateways"
SITEA_GW_IDS=()
for ((k=0; k<num_siteA; k++)); do
    idx=$(( NUM_GWS - 1 - k ))
    gw_id="${GW_IDS[$idx]}"
    SITEA_GW_IDS+=("$gw_id")
    ordinal="${ORDINALS[$k]:-$((k+1))th}"
    substep "Setting location '$LOCATION' on gateway $gw_id ($ordinal gw out of $num_siteA)"
    echo "    ceph nvme-gw set-location $gw_id $POOL $GROUP $LOCATION"
    ceph nvme-gw set-location "$gw_id" "$POOL" "$GROUP" "$LOCATION"
    echo "    [OK] Location set"
done
show_gw_full "after tagging $num_siteA gateway(s) to $LOCATION"

#
# (3) Relocate half+1 of the namespaces to siteA
#
to_change=$(( TOTAL_NS / 2 + 1 ))
step_banner "(3) Relocation of $to_change namespaces to $LOCATION"
show_gw_full "(3) state before relocating namespaces"
show_ns_full "(3) namespaces before relocation"
echo "Changing $to_change namespaces (half+1 of $TOTAL_NS) to $LOCATION"
relocate_namespaces_to "$LOCATION" "$to_change" || exit 1
show_ns_full "(3) namespaces after relocation"

#
# (4) Verify balanced distribution across siteA gateways
#
step_banner "(4) Verify balanced distribution of $to_change $LOCATION namespaces across $num_siteA $LOCATION gateways"
show_gw_full "(4) state before verifying balance"
verify_site_balance "$LOCATION" "$to_change" "$num_siteA" || exit 1

#
# (5+) siteA disaster-recovery scenario
#
STEP=4
run_site_dr "$LOCATION" "${SITEA_GW_IDS[@]}"

#
# (siteB) Move the first gateway to siteB. If a gateway with empty location
# remains afterward, run the full siteB flow (relocate the remaining namespaces,
# add a 2nd siteB gateway, verify balance, run siteB disaster-recovery).
#
gw0="${GW_IDS[0]}"
bump_step
step_banner "($STEP) Change first gateway ($gw0) location to 'siteB' and verify namespace redistribution"
show_gw_full "($STEP) state before moving first gateway to siteB"
set_gw_location_and_wait "$gw0" "siteB" || exit 1

siteb_out=$(gw_show 2>/dev/null || true)
empty_gw=$(echo "$siteb_out" | jq -r '[.["Created Gateways:"][] | select((.location // "")=="") | .["gw-id"]] | .[0] // ""')
if [ -n "$empty_gw" ]; then
    remaining=$(( TOTAL_NS - to_change ))

    bump_step
    step_banner "($STEP) Relocation of $remaining namespaces to siteB (before 2nd gateway location change)"
    show_ns_full "($STEP) namespaces before siteB relocation"
    echo "Changing $remaining namespaces (all remaining of $TOTAL_NS) to siteB"
    relocate_namespaces_to "siteB" "$remaining" || exit 1
    show_ns_full "($STEP) namespaces after siteB relocation"

    bump_step
    step_banner "($STEP) Change empty-location gateway ($empty_gw) to siteB (2nd siteB gateway)"
    show_gw_full "($STEP) state before adding 2nd siteB gateway"
    # This gateway JOINS siteB, so the $remaining siteB-located namespaces rebalance
    # across all siteB gateways (it will hold ~remaining/count, not drain to 0).
    set_gw_location_and_wait "$empty_gw" "siteB" "$remaining" || exit 1

    SITEB_GW_IDS=("$gw0" "$empty_gw")
    num_siteB=${#SITEB_GW_IDS[@]}
    bump_step
    step_banner "($STEP) Verify balanced distribution of $remaining siteB namespaces across $num_siteB siteB gateways"
    show_gw_full "($STEP) state before verifying siteB balance"
    verify_site_balance "siteB" "$remaining" "$num_siteB" || exit 1

    run_site_dr "siteB" "${SITEB_GW_IDS[@]}"
else
    substep "No gateway with empty location - skipping siteB 2nd-gateway setup (keeping only 1 siteB gateway)"
fi

#
# Final gateway status + cleanup (leave the cluster as it was at the start)
#
bump_step
step_banner "($STEP) Final gateway status"
show_gw_full "final gateway status (end of cycle)"

step_banner "Cleanup: waiting for all gateways to be AVAILABLE before reset"
wait_for_gateways_available
reset_to_default_state "Cleanup:"
step_banner "Cleanup: waiting for all gateways to be AVAILABLE"
wait_for_gateways_available

# Always print the final status (force, bypassing the dedup) right before PASSED.
step_banner "Final gateway status before PASSED (restored to default)"
show_gw_full "final (restored to default)" force

# Hard-assert the restored-to-default state so an unbalanced or degraded end
# state can no longer print PASSED.
verify_final_state || exit 1

section_banner "Stretch cluster test PASSED"
