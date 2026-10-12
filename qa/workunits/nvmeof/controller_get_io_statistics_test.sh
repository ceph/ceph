#!/bin/bash -e

# Controller Get IO Statistics Test:
#     Test the NVMe-oF connection IO statistics get/reset commands with various
#     filters (subsystem NQN, host NQN) and the verbose variant, against both the
#     containerized nvmeof-cli and the native `ceph nvmeof` CLI.
#
#     Both CLIs expose the same capability under different names:
#
#       |           | nvmeof-cli (container)         | ceph nvmeof (native)                                |
#       | get       | connection get_io_statistics   | --format json connection get_extended_io_statistics |
#       | reset     | connection reset_io_statistics | connection reset_extended_io_statistics             |
#       | subsystem | --subsystem <NQN>              | --nqn <NQN>                                         |
#       | host      | --host-nqn <NQN>               | --host-nqn <NQN>                                    |
#       | verbose   | --verbose                      | --verbose_statistics_mode                           |
#
#     `--format json` is required for the native get: without it the CLI only
#     prints "Please use JSON format to see the statistics".
#
#     Steps:
#         1. Get all subsystem NQNs and host NQN
#         2. Part 1: get IO statistics before reset - verify each returns statistics,
#            for every filter combination, with and without verbose:
#            - (no filter)
#            - --subsystem/--nqn <NQN>
#            - --host-nqn <NQN>
#            - --subsystem/--nqn <NQN> --host-nqn <NQN>
#         3. Part 2: reset IO statistics - for every filter combination, reset and
#            verify the matching get reports no statistics. Each CLI gets its own
#            reset round, with an fio run in between so the second CLI's reset has
#            statistics to actually clear.
#         4. Re-run fio
#         5. Part 3: get IO statistics after reset - verify each returns statistics again
#
# The gateway collects IO statistics per connection, so this test drives its own
# fio runs: once to populate the statistics checked in part 1, once between the
# two part 2 reset rounds, and once more to repopulate the statistics checked in
# part 3.

source /etc/ceph/nvmeof.env

SPDK_CONTROLLER="Ceph bdev Controller"
# `connection get_io_statistics` prints one block per connection: either an
# "IO statistics for ..." header followed by a table, or a "No IO statistics
# available ..." notice. A single command covering several connections can
# therefore return both at once.
STATS_HEADER_MSG="IO statistics for "
NO_STATS_MSG="No IO statistics available"
LINE_SIGN=$(printf '=%.0s' {1..130})
DASH_SIGN=$(printf -- '-%.0s' {1..130})
FIO_RUNTIME="${FIO_RUNTIME:-60}"
# CLIs to exercise, in order. Set to "container" or "native" to restrict the run
# to a single CLI.
NVMEOF_CLI_MODES="${NVMEOF_CLI_MODES:-container native}"

sudo yum -y install fio

on_exit() {
    local rc=$?
    if [ "$rc" -eq 0 ]; then
        return 0
    fi
    echo
    echo "❌ CONTROLLER GET IO STATISTICS TEST FAILED (exit code $rc)"
    echo
    sudo nvme list-subsys || true
    sudo nvme list || true
    sudo dmesg -T > $TESTDIR/archive/dmesg-controller_get_io_statistics_test.log 2>/dev/null || true
}
trap on_exit EXIT

cli_name() {
    if [ "$1" = native ]; then echo "ceph nvmeof"; else echo "nvmeof-cli"; fi
}

# Print the command line a CLI invocation runs, for logging.
cli_display_cmd() {
    local mode="$1"
    shift
    if [ "$mode" = native ]; then
        echo "ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS $*"
    else
        echo "sudo podman run -it $NVMEOF_CLI_IMAGE" \
            "--server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" \
            "--server-port $NVMEOF_SRPORT --output stdio $*"
    fi
}

# "--output stdio" is required for the container CLI: it defaults to
# "--output log", which writes the command output to stderr through the logger
# instead of to stdout.
cli_run() {
    local mode="$1"
    shift
    if [ "$mode" = native ]; then
        ceph nvmeof --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS "$@"
    else
        sudo podman run -it $NVMEOF_CLI_IMAGE \
            --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS \
            --server-port $NVMEOF_SRPORT --output stdio "$@"
    fi
}

# Build the IO statistics subcommand for a CLI into the IO_STATS_ARGS array.
#   $1: CLI mode - "container" or "native"
#   $2: action - "get" or "reset"
#   $3: subsystem NQN to filter on, empty for all subsystems
#   $4: host NQN to filter on, empty for all hosts
#   $5: "true" to add the verbose flag - ignored for "reset", which has no
#       verbose variant
build_io_stats_args() {
    local mode="$1" action="$2" nqn="$3" host="$4" verbose="$5"
    local nqn_flag host_flag verbose_flag

    IO_STATS_ARGS=()
    if [ "$mode" = native ]; then
        # "--format json" is a global `ceph nvmeof` flag, so it goes before the
        # subcommand. The reset only prints a status line, so it needs no format.
        if [ "$action" = get ]; then
            IO_STATS_ARGS+=(--format json)
        fi
        IO_STATS_ARGS+=(connection "${action}_extended_io_statistics")
        nqn_flag="--nqn"
        host_flag="--host-nqn"
        verbose_flag="--verbose_statistics_mode"
    else
        IO_STATS_ARGS+=(connection "${action}_io_statistics")
        nqn_flag="--subsystem"
        host_flag="--host-nqn"
        verbose_flag="--verbose"
    fi

    [ -n "$nqn" ] && IO_STATS_ARGS+=("$nqn_flag" "$nqn")
    [ -n "$host" ] && IO_STATS_ARGS+=("$host_flag" "$host")
    [ "$verbose" = true ] && [ "$action" = get ] && IO_STATS_ARGS+=("$verbose_flag")
    return 0
}

# Cut the JSON payload out of a `ceph` response: `ceph` can print unrelated text
# (cluster warnings, for instance) ahead of the payload. Each line is tried in
# turn as the start of the payload until one decodes; scanning rather than
# cutting at the first '{' or '[' matters because a warning like
# "cluster [WRN] ..." puts a '[' ahead of the real payload.
json_payload() {
    local text="$1" total line candidate
    total=$(printf '%s\n' "$text" | wc -l)
    for (( line = 1; line <= total; line++ )); do
        candidate=$(printf '%s\n' "$text" | tail -n +"$line")
        if printf '%s' "$candidate" | jq -e . >/dev/null 2>&1; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    return 1
}

# Redraw a table with ASCII characters. The container CLI frames its tables with
# Unicode box-drawing characters, which teuthology's log capture mangles into
# sequences like "â•’â•" - the UTF-8 bytes of "╒═" read back as cp1252. Only the
# copy printed to the log goes through this; the text the parsers see is the
# untouched CLI output.
ascii_table() {
    LC_ALL=C.UTF-8 sed \
        -e 's/[─━═]/-/g' \
        -e 's/[│┃║]/|/g' \
        -e 's/[┌┬┐├┼┤└┴┘╒╤╕╞╪╡╘╧╛╔╦╗╠╬╣╚╩╝]/+/g'
}

# Print the full response of a CLI command in a delimited block, so every
# get/reset invocation is readable in the test log.
#
# `ceph nvmeof --format json` returns the payload on a single line, which is
# unreadable once it carries latency buckets, so it is pretty-printed. The
# container CLI prints a human-readable table, which is shown as-is apart from
# the ASCII reframing above. stderr is appended under its own marker when
# non-empty: `ceph` writes cluster warnings there, and keeping them out of the
# payload block avoids mistaking them for part of the response.
print_cli_output() {
    local mode="$1" stdout="$2" stderr="$3" payload

    echo "$DASH_SIGN"
    if [ -z "$(printf '%s' "$stdout$stderr" | tr -d '[:space:]')" ]; then
        echo "(no output)"
    else
        if [ -n "$(printf '%s' "$stdout" | tr -d '[:space:]')" ]; then
            if [ "$mode" = native ] && payload=$(json_payload "$stdout"); then
                echo "$payload" | jq .
            else
                echo "$stdout" | ascii_table
            fi
        fi
        if [ -n "$(printf '%s' "$stderr" | tr -d '[:space:]')" ]; then
            echo "--- stderr ---"
            echo "$stderr"
        fi
    fi
    echo "$DASH_SIGN"
}

# Split a container CLI response into its per-connection results, setting
# STATS_COUNT and NO_STATS_COUNT.
#
# Note: "Total IOs count" is not a usable marker - the CLI prints it in the
# no-stats branch too.
split_text_io_stats() {
    local output="$1"
    STATS_COUNT=$(echo "$output" | grep -c "$STATS_HEADER_MSG" || true)
    NO_STATS_COUNT=$(echo "$output" | grep -c "$NO_STATS_MSG" || true)
}

# Split a native CLI response into its per-connection results, setting
# STATS_COUNT and NO_STATS_COUNT.
#
# Shape of the `ceph nvmeof --format json connection get_extended_io_statistics`
# payload:
#   {"status": 0, "error_message": "", "category": "total",
#    "statistics_entries": [{"subsystem_nqn": ..., "host_nqn": ...,
#                            "total_num_ios": "57216", "buckets": [...]}]}
# The NQNs come back empty on entries that aggregate over all subsystems/hosts.
#
# An entry counts as reporting statistics when it carries a non-empty latency
# bucket list - the same signal the container CLI uses to choose between printing
# a table and printing "No IO statistics available". "total_num_ios" is
# deliberately not used: the gateway keeps it across a reset, so it cannot tell
# "has statistics" apart from "was just reset".
split_json_io_stats() {
    local output="$1" description="$2" payload status error_message

    if ! payload=$(json_payload "$output"); then
        echo "$description: expected JSON output from 'ceph nvmeof' but could not decode any" \
             "— see the output above"
        return 1
    fi

    status=$(echo "$payload" | jq -r '.status // 0')
    if [ "$status" != "0" ]; then
        error_message=$(echo "$payload" | jq -r '.error_message // ""')
        echo "$description: command returned status $status: ${error_message:-see the output above}"
        return 1
    fi

    STATS_COUNT=$(echo "$payload" |
        jq '[.statistics_entries // [] | .[] | select(((.buckets // []) | length) > 0)] | length')
    NO_STATS_COUNT=$(echo "$payload" |
        jq '[.statistics_entries // [] | .[] | select(((.buckets // []) | length) == 0)] | length')
}

# Execute a connection IO statistics command and assert the per-connection
# expectations.
#   $1: CLI mode - "container" or "native"
#   $2: expectation - "stats" (at least one connection must report statistics),
#       "no_stats" (no connection may report statistics) or "none" (no assertion
#       on the output)
#   $3: description of the step/command, for logging and assert messages
#   $4..: the CLI subcommand and its arguments
run_io_stats_cli() {
    local mode="$1" expect="$2" description="$3"
    shift 3

    echo
    echo
    echo "$description"
    echo "$LINE_SIGN"
    cli_display_cmd "$mode" "$@"
    echo "$LINE_SIGN"

    local stdout stderr combined rc err_file
    err_file=$(mktemp -t nvmeof-io-stats-err-XXXX)
    set +e
    stdout=$(cli_run "$mode" "$@" 2>"$err_file")
    rc=$?
    set -e
    stderr=$(cat "$err_file")
    rm -f "$err_file"
    # Command substitution strips trailing newlines, so the two streams are
    # rejoined with one: otherwise the last stdout line and the first stderr line
    # would merge and throw off the per-line counting below.
    combined="$stdout"
    if [ -n "$stderr" ]; then
        combined="${combined:+$combined$'\n'}$stderr"
    fi

    print_cli_output "$mode" "$stdout" "$stderr"

    if [ "$expect" = none ]; then
        echo
        echo
        return 0
    fi

    # The failure messages below do not repeat the response: print_cli_output has
    # just shown it in full.
    if [ "$rc" -ne 0 ]; then
        echo "$description: command failed (exit code $rc) — see the output above"
        return 1
    fi

    STATS_COUNT=0
    NO_STATS_COUNT=0
    if [ "$mode" = native ]; then
        # `ceph` writes its own warnings to stderr, so only the payload on stdout
        # is parsed.
        split_json_io_stats "$stdout" "$description" || return 1
    else
        split_text_io_stats "$combined"
    fi

    case "$expect" in
        stats)
            if [ -z "$(echo "$combined" | tr -d '[:space:]')" ]; then
                echo "$description: expected IO statistics in output but output was empty"
                return 1
            fi
            # Connections that saw no IO legitimately report none alongside the
            # ones that did, so those on their own are not a failure.
            if [ "$STATS_COUNT" -lt 1 ]; then
                echo "$description: expected at least one connection to report IO statistics" \
                     "but all $NO_STATS_COUNT reported none — see the output above"
                return 1
            fi
            local idle_note=""
            if [ "$NO_STATS_COUNT" -gt 0 ]; then
                idle_note=" ($NO_STATS_COUNT idle connection(s) reported no statistics)"
            fi
            echo "✅ Verified: statistics present for $STATS_COUNT connection(s)$idle_note"
            ;;
        no_stats)
            if [ "$mode" != native ] && [ "$NO_STATS_COUNT" -lt 1 ]; then
                # The container CLI always prints a notice per connection, so
                # silence means something other than "reset worked". The native
                # CLI instead returns an empty entry list.
                echo "$description: expected every connection to report no IO statistics" \
                     "but found no connection at all — see the output above"
                return 1
            fi
            if [ "$STATS_COUNT" -gt 0 ]; then
                echo "$description: expected no IO statistics after reset but $STATS_COUNT" \
                     "connection(s) still report statistics — see the output above"
                return 1
            fi
            if [ "$NO_STATS_COUNT" -gt 0 ]; then
                echo "✅ Verified: no statistics for all $NO_STATS_COUNT connection(s)"
            else
                echo "✅ Verified: no connection reports statistics"
            fi
            ;;
    esac

    echo
    echo
}

# Run every get combination against one CLI and verify statistics are present:
#   - (no filter) [verbose]
#   - --host-nqn <NQN> [verbose]
#   - subsystem filter <NQN> [verbose]
#   - subsystem filter <NQN> --host-nqn <NQN> [verbose]
verify_all_get_io_statistics() {
    local part_num="$1" mode="$2"
    local i nqn host verbose name
    name=$(cli_name "$mode")
    for i in "${!FILTER_NQNS[@]}"; do
        nqn="${FILTER_NQNS[$i]}"
        host="${FILTER_HOSTS[$i]}"
        for verbose in false true; do
            build_io_stats_args "$mode" get "$nqn" "$host" "$verbose"
            run_io_stats_cli "$mode" stats \
                "Part $part_num: $name ${IO_STATS_ARGS[*]}" \
                "${IO_STATS_ARGS[@]}"
        done
    done
}

# Run every reset combination against one CLI and verify with the matching get
# command that no connection reports statistics any more:
#   - (no filter)
#   - --host-nqn <NQN>
#   - subsystem filter <NQN>
#   - subsystem filter <NQN> --host-nqn <NQN>
reset_and_verify_io_statistics() {
    local mode="$1"
    local i nqn host name
    name=$(cli_name "$mode")
    for i in "${!FILTER_NQNS[@]}"; do
        nqn="${FILTER_NQNS[$i]}"
        host="${FILTER_HOSTS[$i]}"

        build_io_stats_args "$mode" reset "$nqn" "$host" false
        run_io_stats_cli "$mode" none \
            "Part 2: $name ${IO_STATS_ARGS[*]}" \
            "${IO_STATS_ARGS[@]}"

        build_io_stats_args "$mode" get "$nqn" "$host" false
        run_io_stats_cli "$mode" no_stats \
            "Part 2 verify: $name ${IO_STATS_ARGS[*]} — expect no stats" \
            "${IO_STATS_ARGS[@]}"
    done
}

run_fio() {
    local fio_file drives device
    drives=$(sudo nvme list --output-format=json |
        jq -r '.Devices | sort_by(.NameSpace) | .[] |
               select(.ModelNumber == "'"$SPDK_CONTROLLER"'") | .DevicePath')
    if [ -z "$drives" ]; then
        echo "[nvmeof.io_stats] ERROR: no NVMeoF devices found — cannot run fio"
        sudo nvme list
        return 1
    fi

    fio_file=$(mktemp -t nvmeof-io-stats-fio-XXXX)
    cat > $fio_file <<EOF
[global]
ioengine=sync
bsrange=4k-64k
numjobs=1
size=1G
time_based=1
runtime=$FIO_RUNTIME
rw=randrw
group_reporting
direct=1

EOF
    for device in $drives; do
        echo "[job-$device]" >> "$fio_file"
        echo "filename=$device" >> "$fio_file"
        echo "" >> "$fio_file"
    done
    cat $fio_file
    sudo fio $fio_file
}

# Ensure we are connected to all subsystems, so that every subsystem has a
# connection the gateway can report IO statistics for. Re-running connect-all
# is idempotent.
sudo nvme connect-all --traddr="$NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" --transport=tcp -l 3600
sleep 5

# Step 1: Discover NQNs and host NQN
nqns=
for mode in $NVMEOF_CLI_MODES; do
    for attempt in 1 2 3; do
        nqns=$(cli_run "$mode" --format json subsystem list | jq -r '.subsystems[].nqn') && break
        echo "[nvmeof.io_stats] subsystem list via $(cli_name "$mode") attempt $attempt failed, retrying..."
        sleep 5
    done
    [ -n "$nqns" ] && break
done
if [ -z "$nqns" ]; then
    echo "[nvmeof.io_stats] ERROR: no subsystems found"
    exit 1
fi

host_nqn=$(cat /etc/nvme/hostnqn)

# The per-connection IO statistics commands used below require a gateway and a
# CLI supporting the extended IO statistics API (optional subsystem/host filters
# and verbose). Older gateways only support the mandatory-arguments form, and the
# gateway may have IO statistics collection disabled altogether.
CLI_MODES=()
for mode in $NVMEOF_CLI_MODES; do
    echo "[nvmeof.io_stats] Checking $(cli_name "$mode") support for connection IO statistics..."
    build_io_stats_args "$mode" get "" "" false
    set +e
    probe_output=$(cli_run "$mode" "${IO_STATS_ARGS[@]}" 2>&1)
    probe_rc=$?
    set -e
    echo "$probe_output"
    if [ "$probe_rc" -ne 0 ] &&
       echo "$probe_output" | grep -qiE "unimplemented|unrecognized arguments|invalid choice|arguments are required|not supported|is disabled|no valid command found|invalid command"; then
        echo "[nvmeof.io_stats] Connection IO statistics are not supported by $(cli_name "$mode") — skipping it"
        continue
    fi
    CLI_MODES+=("$mode")
done
if [ "${#CLI_MODES[@]}" -eq 0 ]; then
    echo "[nvmeof.io_stats] Connection IO statistics are not supported by this gateway/CLI — skipping test"
    exit 0
fi

# Target filters, in the order they are exercised: (subsystem NQN, host NQN).
# An empty entry means "no filter"; build_io_stats_args maps each pair onto the
# flag names of the CLI in use.
FILTER_NQNS=("" "")
FILTER_HOSTS=("" "$host_nqn")
nqns_count=0
nqns_list=
for nqn in $nqns; do
    FILTER_NQNS+=("$nqn" "$nqn")
    FILTER_HOSTS+=("" "$host_nqn")
    nqns_count=$((nqns_count + 1))
    if [ -n "$nqns_list" ]; then
        nqns_list="$nqns_list, "
    fi
    nqns_list="$nqns_list'$nqn'"
done

echo
echo "$LINE_SIGN"
echo "CONTROLLER GET IO STATISTICS TEST"
echo "$LINE_SIGN"
echo "Found $nqns_count subsystem(s): [$nqns_list]"
echo "Host NQN: $host_nqn"
echo "CLIs under test: ${CLI_MODES[*]}"

# Generate the IO the part 1 statistics are collected from
echo
echo "$LINE_SIGN"
echo "RUNNING FIO (generating IO statistics)"
echo "$LINE_SIGN"
run_fio

# Step 2: Part 1 - Get IO statistics before reset, with each CLI
for mode in "${CLI_MODES[@]}"; do
    echo
    echo "$LINE_SIGN"
    echo "PART 1 [$(cli_name "$mode")]: GET IO STATISTICS BEFORE RESET"
    echo "$LINE_SIGN"
    verify_all_get_io_statistics 1 "$mode"
done

# Step 3: Part 2 - Reset IO statistics and verify no statistics are reported.
# Each CLI resets statistics it has seen populated: the first reset round clears
# everything, so fio is re-run before handing over to the next CLI, otherwise its
# reset would have nothing to clear and the verification would be vacuous.
for i in "${!CLI_MODES[@]}"; do
    mode="${CLI_MODES[$i]}"
    if [ "$i" -gt 0 ]; then
        echo
        echo "$LINE_SIGN"
        echo "RE-RUNNING FIO (repopulating statistics for the next CLI's reset)"
        echo "$LINE_SIGN"
        run_fio
    fi
    echo
    echo "$LINE_SIGN"
    echo "PART 2 [$(cli_name "$mode")]: RESET IO STATISTICS"
    echo "$LINE_SIGN"
    reset_and_verify_io_statistics "$mode"
done

# Step 4: Re-run fio
echo
echo "$LINE_SIGN"
echo "RE-RUNNING FIO"
echo "$LINE_SIGN"
run_fio

# Step 5: Part 3 - Get IO statistics after reset + fio, with each CLI
for mode in "${CLI_MODES[@]}"; do
    echo
    echo "$LINE_SIGN"
    echo "PART 3 [$(cli_name "$mode")]: GET IO STATISTICS AFTER RESET"
    echo "$LINE_SIGN"
    verify_all_get_io_statistics 3 "$mode"
done

echo
echo "$LINE_SIGN"
echo "✅ CONTROLLER GET IO STATISTICS TEST PASSED"
echo "$LINE_SIGN"
echo
