#!/bin/bash -e

# Controller Get IO Statistics Test:
#     Test NVMe-oF connection get_io_statistics and reset_io_statistics commands
#     with various options (--verbose, --subsystem, --host-nqn).
#
#     Steps:
#         1. Get all subsystem NQNs and host NQN
#         2. Part 1: connection get_io_statistics before reset - verify each returns statistics:
#            - nvmeof-cli connection get_io_statistics
#            - nvmeof-cli connection get_io_statistics --verbose
#            - nvmeof-cli connection get_io_statistics --subsystem <NQN>
#            - nvmeof-cli connection get_io_statistics --subsystem <NQN> --verbose
#            - nvmeof-cli connection get_io_statistics --host-nqn <NQN>
#            - nvmeof-cli connection get_io_statistics --host-nqn <NQN> --verbose
#            - nvmeof-cli connection get_io_statistics --subsystem <NQN> --host-nqn <NQN>
#            - nvmeof-cli connection get_io_statistics --subsystem <NQN> --host-nqn <NQN> --verbose
#         3. Part 2: reset_io_statistics - verify each returns "No IO statistics available":
#            - nvmeof-cli connection reset_io_statistics
#            - nvmeof-cli connection reset_io_statistics --subsystem <NQN>
#            - nvmeof-cli connection reset_io_statistics --host-nqn <NQN>
#            - nvmeof-cli connection reset_io_statistics --subsystem <NQN> --host-nqn <NQN>
#         4. Re-run fio
#         5. Part 3: connection get_io_statistics after reset - verify each returns statistics again
#
# The gateway collects IO statistics per connection, so this test drives its own
# fio runs: once to populate the statistics checked in part 1, and once more after
# the reset of part 2 to repopulate the statistics checked in part 3.

source /etc/ceph/nvmeof.env

SPDK_CONTROLLER="Ceph bdev Controller"
NO_STATS_MSG="No IO statistics available"
LINE_SIGN=$(printf '=%.0s' {1..130})
FIO_RUNTIME="${FIO_RUNTIME:-60}"

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

# "--output stdio" is required: the CLI defaults to "--output log", which writes
# the command output to stderr through the logger instead of to stdout.
nvmeof_cli_cmd() {
    echo "sudo podman run -it $NVMEOF_CLI_IMAGE" \
        "--server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS" \
        "--server-port $NVMEOF_SRPORT --output stdio $*"
}

nvmeof_cli() {
    sudo podman run -it $NVMEOF_CLI_IMAGE \
        --server-address $NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS \
        --server-port $NVMEOF_SRPORT --output stdio "$@"
}

# Execute an nvmeof-cli connection IO statistics command and assert output expectations.
#   $1: expectation - "stats" (statistics must be returned), "no_stats" ("No IO statistics
#       available" must be returned) or "none" (no assertion on the output)
#   $2: description of the step/command, for logging and assert messages
#   $3..: the CLI subcommand and its arguments
run_io_stats_cli() {
    local expect="$1"
    local description="$2"
    shift 2

    echo
    echo
    echo "$description"
    echo "$LINE_SIGN"
    nvmeof_cli_cmd "$@"
    echo "$LINE_SIGN"

    local output rc
    set +e
    output=$(nvmeof_cli "$@" 2>&1)
    rc=$?
    set -e

    # `get_io_statistics` prints a large table per connection, which is only
    # asserted on and not shown. `reset_io_statistics` prints a single status
    # line, which is shown. Any assertion failure below includes the output.
    local print_output=true
    case " $* " in
        *" get_io_statistics "*) print_output=false ;;
    esac
    if [ "$print_output" = true ]; then
        echo "$output"
        echo
    fi

    case "$expect" in
        stats)
            if echo "$output" | grep -q "$NO_STATS_MSG"; then
                echo "$description: expected IO statistics in output but got '$NO_STATS_MSG'"
                return 1
            fi
            if [ -z "$(echo "$output" | tr -d '[:space:]')" ]; then
                echo "$description: expected IO statistics in output but output was empty"
                return 1
            fi
            if [ "$rc" -ne 0 ]; then
                echo "$description: expected IO statistics in output but command failed (exit code $rc): $output"
                return 1
            fi
            echo "✅ Verified: statistics present in output"
            ;;
        no_stats)
            if ! echo "$output" | grep -q "$NO_STATS_MSG"; then
                echo "$description: expected '$NO_STATS_MSG' in output but got: $output"
                return 1
            fi
            echo "✅ Verified: '$NO_STATS_MSG' present in output"
            ;;
    esac

    echo
    echo
}

# Run all combinations of `connection get_io_statistics` commands and verify statistics
# are present:
#   - (no args)
#   - --verbose
#   - --subsystem <NQN> [--verbose]
#   - --host-nqn <NQN> [--verbose]
#   - --subsystem <NQN> --host-nqn <NQN> [--verbose]
verify_all_get_io_statistics() {
    local part_num=$1
    local i filter_args label
    for i in "${!FILTER_ARGS[@]}"; do
        filter_args="${FILTER_ARGS[$i]}"
        label="${FILTER_LABELS[$i]}"
        run_io_stats_cli stats \
            "Part $part_num: nvmeof-cli connection get_io_statistics $label" \
            connection get_io_statistics $filter_args
        run_io_stats_cli stats \
            "Part $part_num: nvmeof-cli connection get_io_statistics $label --verbose" \
            connection get_io_statistics $filter_args --verbose
    done
}

# Run each `connection reset_io_statistics` combination and verify with
# `connection get_io_statistics` that "No IO statistics available" is returned:
#   - (no args)
#   - --subsystem <NQN>
#   - --host-nqn <NQN>
#   - --subsystem <NQN> --host-nqn <NQN>
reset_and_verify_io_statistics() {
    local i filter_args label
    for i in "${!FILTER_ARGS[@]}"; do
        filter_args="${FILTER_ARGS[$i]}"
        label="${FILTER_LABELS[$i]}"
        run_io_stats_cli none \
            "Part 2: nvmeof-cli connection reset_io_statistics $label" \
            connection reset_io_statistics $filter_args
        run_io_stats_cli no_stats \
            "Part 2 verify: nvmeof-cli connection get_io_statistics $label — expect no stats" \
            connection get_io_statistics $filter_args
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
for attempt in 1 2 3; do
    nqns=$(nvmeof_cli --format json subsystem list | jq -r '.subsystems[].nqn') && break
    echo "[nvmeof.io_stats] subsystem list attempt $attempt failed, retrying..."
    sleep 5
done
if [ -z "$nqns" ]; then
    echo "[nvmeof.io_stats] ERROR: no subsystems found"
    exit 1
fi

host_nqn=$(cat /etc/nvme/hostnqn)

# The per-connection IO statistics commands used below require a gateway and a CLI
# supporting the extended IO statistics API (optional --subsystem/--host-nqn and
# --verbose). Older gateways only support the mandatory-arguments form, and the
# gateway may have IO statistics collection disabled altogether.
echo "[nvmeof.io_stats] Checking gateway support for connection IO statistics..."
set +e
probe_output=$(nvmeof_cli connection get_io_statistics 2>&1)
probe_rc=$?
set -e
echo "$probe_output"
if [ "$probe_rc" -ne 0 ] &&
   echo "$probe_output" | grep -qiE "unimplemented|unrecognized arguments|invalid choice|arguments are required|not supported|is disabled"; then
    echo "[nvmeof.io_stats] Connection IO statistics are not supported by this gateway/CLI — skipping test"
    exit 0
fi

# Target filters, in the order they are exercised: (filter args, label)
FILTER_ARGS=("")
FILTER_LABELS=("(no args)")
FILTER_ARGS+=("--host-nqn $host_nqn")
FILTER_LABELS+=("--host-nqn $host_nqn")
nqns_count=0
nqns_list=
for nqn in $nqns; do
    FILTER_ARGS+=("--subsystem $nqn")
    FILTER_LABELS+=("--subsystem $nqn")
    FILTER_ARGS+=("--subsystem $nqn --host-nqn $host_nqn")
    FILTER_LABELS+=("--subsystem $nqn --host-nqn $host_nqn")
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

# Generate the IO the part 1 statistics are collected from
echo
echo "$LINE_SIGN"
echo "RUNNING FIO (generating IO statistics)"
echo "$LINE_SIGN"
run_fio

# Step 2: Part 1 - Connection get IO statistics before reset
echo
echo "$LINE_SIGN"
echo "PART 1: CONNECTION GET IO STATISTICS BEFORE RESET"
echo "$LINE_SIGN"
verify_all_get_io_statistics 1

# Step 3: Part 2 - Reset IO statistics and verify "No IO statistics available"
echo
echo "$LINE_SIGN"
echo "PART 2: RESET IO STATISTICS"
echo "$LINE_SIGN"
reset_and_verify_io_statistics

# Step 4: Re-run fio
echo
echo "$LINE_SIGN"
echo "RE-RUNNING FIO"
echo "$LINE_SIGN"
run_fio

# Step 5: Part 3 - Connection get IO statistics after reset + fio
echo
echo "$LINE_SIGN"
echo "PART 3: CONNECTION GET IO STATISTICS AFTER RESET"
echo "$LINE_SIGN"
verify_all_get_io_statistics 3

echo
echo "$LINE_SIGN"
echo "✅ CONTROLLER GET IO STATISTICS TEST PASSED"
echo "$LINE_SIGN"
echo
