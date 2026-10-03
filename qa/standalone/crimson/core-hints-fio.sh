#!/usr/bin/env bash
#
# Compare client I/O performance against Crimson OSDs, with and without
# routing client ops directly to the reactor core owning the PG:
#   - the OSDs listen on a port per reactor core (crimson_osd_core_listeners),
#     and hint the owning core's address in their op replies;
#   - the client follows these hints only with objecter_use_osd_core_hints.
# The OSD side is identical in both modes; only the client flag differs.
#
# For each workload (random read, random write, mixed), fio (rados engine) is
# run once with the flag off and once with it on, and the results compared:
#   - IOPS, and average/p99 completion latency (fio);
#   - the fraction of client ops that needed a cross-core hop on the OSDs
#     (osd_pg_shard_op_remote vs op_local, from 'dump_metrics');
#   - the fraction of ops the client sent on per-core connections
#     (objecter op_send_core vs op_send, from the fio clients' admin sockets);
#   - the load of the OSDs' reactor cores: busy time over the measured part
#     of the run (reactor_cpu_busy_ms), averaged over all of them, and of the
#     busiest one. Per-core values are kept in <RESULTS_DIR>/<run>.cores.tsv,
#     and the client ops each core served (local: without a cross-core hop,
#     remote: forwarded to it) in <RESULTS_DIR>/<run>.ops.tsv;
#   - per client op (over all OSD reactors): reactor tasks run and pollers
#     executed, and TCP segments sent (system wide: on loopback, every
#     unbatched send is a segment), as indications of batching; and the CPU
#     the fio clients used (cores). Per-reactor deltas (busy %, polls, tasks,
#     network bytes sent/received) are in <RESULTS_DIR>/<run>.reactor.tsv.
#   All but the fio figures are measured over the same window: from about
#   the end of the ramp to about the end of the run.
#
# Run with:
#   cd build && ../qa/run-standalone.sh crimson/core-hints-fio.sh
#
# Tunables (environment):
#   NUM_OSDS       OSDs                                         (default: 3)
#   POOL_SIZE      the replicated pool's size                   (default: 3)
#   OSD_SMP        reactor cores per OSD                        (default: 4)
#   OSD_CPU_BASE   unset: the OSDs' reactors are not pinned (crimson_cpu_num,
#                  at most 32). Set: OSD i's reactors are pinned to the
#                  OSD_SMP CPUs from OSD_CPU_BASE + i * OSD_SMP on
#                  (crimson_cpu_set)                            (default: unset)
#   CLIENT_CPUS    the CPUs (taskset list) to run fio on        (default: any)
#   OSD_EXTRA_ARGS more OSD command line options, e.g. "--ms_tcp_nodelay=false"
#   CLIENT_CONF_EXTRA  more [client] settings, ';'-separated, e.g.
#                  "ms tcp nodelay = false; ms async op threads = 6"
#   OSD_MEMORY     memory per OSD (crimson_memory)              (default: 4G)
#   STORE          cyanstore | seastore | bluestore (alienstore) (default: seastore)
#   SEASTORE_DEVS  seastore: comma-separated block devices, one per OSD (in
#                  OSD id order), as with vstart.sh --seastore-devs. Their
#                  first MiB is zeroed! Unset: a SEASTORE_SIZE block file per
#                  OSD, in the test dir                         (default: unset)
#   SEASTORE_SIZE  seastore block file size, without SEASTORE_DEVS (default: 10G)
#   SEASTORE_BACKEND  rbm (random block: RANDOM_BLOCK_SSD devices) or
#                  segmented (SSD devices)                      (default: rbm)
#   POOL_PGS       the pool's PGs (PG_NUM is accepted too)      (default: 32)
#   WORKLOADS      fio rw modes, from: randread randwrite randrw
#                                     (default: all three; randrw is 70% reads)
#   FIO            the fio binary (with the rados engine)       (default: fio)
#   FIO_RUNTIME, FIO_RAMP   seconds per run, and warm-up        (default: 60, 10)
#   FIO_BS, FIO_IODEPTH, FIO_NUMJOBS                            (default: 4k, 16, 4)
#   FIO_NRFILES, FIO_FILESIZE  objects per fio job, and their size (default: 32, 4m)
#                  (a job's fio 'size' is their product)
#   FIO_PREFILL_BS, FIO_PREFILL_IODEPTH  block size and iodepth of writing
#                  the objects initially                        (default: 1m, 4)
#                  Note: crimson_memory is split evenly among an OSD's
#                  reactors, and a reactor that runs out of it fails its
#                  connections (std::bad_alloc). Keep OSD_MEMORY / OSD_SMP
#                  well above the data in flight per reactor.
#   REPEAT         repetitions of every (workload, mode) pair   (default: 1)
#   RESULTS_DIR    where fio outputs, the summary and the OSD logs are kept
#                  (default: ./fio-hints-results.<date> under the build dir)
#   STRICT         1: fail unless the flag removes most of the OSD-side
#                  cross-core hops (default: 0, only warn)

# read before ceph-helpers.sh sets its own PG_NUM (4)
POOL_PGS=${POOL_PGS:-${PG_NUM:-32}}

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

NUM_OSDS=${NUM_OSDS:-3}
POOL_SIZE=${POOL_SIZE:-3}
OSD_SMP=${OSD_SMP:-4}
OSD_CPU_BASE=${OSD_CPU_BASE:-}
CLIENT_CPUS=${CLIENT_CPUS:-}
OSD_EXTRA_ARGS=${OSD_EXTRA_ARGS:-}
CLIENT_CONF_EXTRA=${CLIENT_CONF_EXTRA:-}
OSD_MEMORY=${OSD_MEMORY:-4G}
STORE=${STORE:-seastore}
SEASTORE_DEVS=${SEASTORE_DEVS:-}
SEASTORE_SIZE=${SEASTORE_SIZE:-10G}
SEASTORE_BACKEND=${SEASTORE_BACKEND:-rbm}
WORKLOADS=${WORKLOADS:-"randread randwrite randrw"}
FIO=${FIO:-fio}
FIO_RUNTIME=${FIO_RUNTIME:-60}
FIO_RAMP=${FIO_RAMP:-10}
FIO_BS=${FIO_BS:-4k}
FIO_IODEPTH=${FIO_IODEPTH:-16}
FIO_NUMJOBS=${FIO_NUMJOBS:-4}
FIO_NRFILES=${FIO_NRFILES:-32}
FIO_FILESIZE=${FIO_FILESIZE:-4m}
FIO_PREFILL_BS=${FIO_PREFILL_BS:-1m}
FIO_PREFILL_IODEPTH=${FIO_PREFILL_IODEPTH:-4}
REPEAT=${REPEAT:-1}
# Note: teardown() takes any file in the build dir whose name starts or ends
# with "core" for a core dump (with a relative kernel.core_pattern), and
# fails the test. Hence the name.
RESULTS_DIR=${RESULTS_DIR:-$PWD/fio-hints-results.$(date +%Y%m%d-%H%M%S)}
STRICT=${STRICT:-0}

POOL=corehints

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7231" # git grep '\<7231\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    command -v "$FIO" >/dev/null || { echo "ERROR: $FIO not found"; return 1; }
    _check_params || return 1
    "$FIO" --enghelp=rados >/dev/null 2>&1 ||
        { echo "ERROR: $FIO has no rados engine"; return 1; }
    # fio uses the librados it is linked with: make it this build's
    export LD_LIBRARY_PATH=$PWD/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
    mkdir -p "$RESULTS_DIR" || return 1

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        echo "-------------- Prepare Test $func -------------------"
        setup $dir || return 1
        echo "-------------- Run Test $func -----------------------"
        $func $dir || { _save_logs $dir; teardown $dir 1; return 1; }
        _save_logs $dir
        echo "-------------- Teardown Test $func ------------------"
        teardown $dir || return 1
        echo "-------------- Complete Test $func ------------------"
    done
}

# keep the OSD and fio client logs (teardown() removes the test dir):
# moved to <RESULTS_DIR>/logs
function _save_logs() {
    local dir=$1
    mkdir -p $RESULTS_DIR/logs || return 1
    mv $dir/osd.*.log $dir/fio-*.log $RESULTS_DIR/logs/ 2>/dev/null
    echo "OSD and fio client logs saved in $RESULTS_DIR/logs"
}

#
# Cluster
#

# The vstart.sh equivalent of the cluster, for reference:
# MGR=1 MON=1 OSD=3 MDS=0 RGW=1 ../src/vstart.sh -n --crimson --no-restart --without-dashboard --seastore --seastore-devs /dev/nvme5n1,/dev/nvme6n1,/dev/nvme7n1 --crimson-smp 24 -o "crimson_seastar_blocked_reactor_notify_ms = 10000" --msgr2 -X -o "osd_op_queue=wpq"

function _check_params() {
    if [ $POOL_SIZE -gt $NUM_OSDS ]; then
        echo "ERROR: POOL_SIZE $POOL_SIZE > NUM_OSDS $NUM_OSDS"
        return 1
    fi
    if [ $FIO_RUNTIME -lt 10 ]; then
        echo "ERROR: FIO_RUNTIME must be at least 10 (seconds)"
        return 1
    fi
    if [ -n "$OSD_CPU_BASE" ]; then
        local last=$((OSD_CPU_BASE + NUM_OSDS * OSD_SMP - 1))
        if [ $last -ge $(nproc) ]; then
            echo "ERROR: the OSDs would need CPUs $OSD_CPU_BASE-$last" \
                 "of $(nproc)"
            return 1
        fi
    elif [ $OSD_SMP -gt 32 ]; then
        echo "ERROR: unpinned OSDs (crimson_cpu_num) are limited to 32" \
             "reactors: set OSD_CPU_BASE"
        return 1
    fi
}

# OSD id's reactor CPUs: pinned (see OSD_CPU_BASE), or just their number
function _osd_cpu_args() {
    local id=$1
    if [ -n "$OSD_CPU_BASE" ]; then
        local first=$((OSD_CPU_BASE + id * OSD_SMP))
        echo "--crimson_cpu_set=$first-$((first + OSD_SMP - 1))"
    else
        echo "--crimson_cpu_num=$OSD_SMP"
    fi
}

# the SEASTORE_DEVS, one per line
function _seastore_devs() {
    tr ',' '\n' <<<"$SEASTORE_DEVS" | grep -v '^$'
}

function _check_seastore_devs() {
    [ "$STORE" = seastore ] && [ -n "$SEASTORE_DEVS" ] || return 0
    local -a devs=($(_seastore_devs))
    if [ ${#devs[@]} -lt $NUM_OSDS ]; then
        echo "ERROR: ${#devs[@]} SEASTORE_DEVS for $NUM_OSDS OSDs"
        return 1
    fi
    local dev
    for dev in "${devs[@]}"; do
        if [ ! -b "$dev" ] || [ ! -w "$dev" ]; then
            echo "ERROR: $dev is not a writable block device"
            return 1
        fi
    done
}

# OSD id's seastore device: as vstart.sh does, zero its first MiB, and link
# it as the 'block' of the OSD's data dir, for the OSD's mkfs to use
function _prepare_osd_dev() {
    local dir=$1 id=$2
    [ "$STORE" = seastore ] && [ -n "$SEASTORE_DEVS" ] || return 0
    local -a devs=($(_seastore_devs))
    local dev=${devs[$id]}
    echo "osd.$id: seastore on $dev"
    mkdir -p $dir/$id || return 1
    dd if=/dev/zero of=$dev bs=1M count=1 oflag=direct || return 1
    ln -sf $dev $dir/$id/block || return 1
}

function _osd_store_args() {
    case "$STORE" in
        cyanstore)
            echo "--osd_objectstore=cyanstore" ;;
        seastore)
            local args="--osd_objectstore=seastore"
            case "$SEASTORE_BACKEND" in
                rbm)
                    args+=" --seastore_hot_device_type=RANDOM_BLOCK_SSD"
                    args+=" --seastore_hot_backend_type=RANDOM_BLOCK" ;;
                segmented)
                    args+=" --seastore_hot_device_type=SSD"
                    args+=" --seastore_hot_backend_type=SEGMENTED" ;;
                *)
                    echo "ERROR: unknown SEASTORE_BACKEND '$SEASTORE_BACKEND'" >&2
                    return 1 ;;
            esac
            # without SEASTORE_DEVS: a block file. With: the device linked
            # as 'block' by _prepare_osd_dev()
            [ -n "$SEASTORE_DEVS" ] ||
                args+=" --seastore_device_size=$SEASTORE_SIZE"
            echo "$args" ;;
        bluestore)
            echo "--osd_objectstore=bluestore" ;;
        *)
            echo "ERROR: unknown STORE '$STORE'" >&2
            return 1 ;;
    esac
}

# NUM_OSDS Crimson OSDs with per-core listeners, and a replicated crimson pool
# of POOL_SIZE
function _setup_cluster() {
    local dir=$1

    # allow the pool's PGs (mon_max_pg_per_osd defaults to 500)
    local pgs_per_osd=$(( (POOL_PGS * POOL_SIZE + NUM_OSDS - 1) / NUM_OSDS ))
    run_mon $dir a --osd_pool_default_size=$POOL_SIZE \
        --mon_max_pg_per_osd=$(( pgs_per_osd > 500 ? pgs_per_osd + 100 : 500 )) \
        --mon_allow_pool_size_one=true \
        --osd_pool_default_crimson=true \
        --osd_pool_default_pg_autoscale_mode=off || return 1
    run_mgr $dir x || return 1

    local store_args
    store_args=$(_osd_store_args) || return 1
    _check_seastore_devs || return 1
    local id
    for id in $(seq 0 $((NUM_OSDS - 1))); do
        _prepare_osd_dev $dir $id || return 1
        run_crimson_osd $dir $id \
            $(_osd_cpu_args $id) \
            --crimson_memory=$OSD_MEMORY \
            --crimson_osd_core_listeners=true \
            $store_args $OSD_EXTRA_ARGS || return 1
    done

    create_pool $POOL $POOL_PGS $POOL_PGS || return 1
    ceph osd pool set $POOL size $POOL_SIZE --yes-i-really-mean-it || return 1
    wait_for_clean || return 1

    # each OSD must have bound its per-core listeners
    for id in $(seq 0 $((NUM_OSDS - 1))); do
        grep -q "public_msgr core listeners: \[v2:" $dir/osd.$id.log || {
            echo "ERROR: osd.$id has no per-core listeners"
            return 1
        }
    done
    _check_store $dir || return 1
}

# record the OSDs' object store setup, and verify the seastore backend
function _check_store() {
    local dir=$1 id line
    local want=
    if [ "$STORE" = seastore ]; then
        case "$SEASTORE_BACKEND" in
            rbm) want="main device type: RANDOM_BLOCK_SSD, main backend type: RANDOM_BLOCK" ;;
            segmented) want="main device type: SSD, main backend type: SEGMENTED" ;;
        esac
    fi
    : > $RESULTS_DIR/osds.txt
    for id in $(seq 0 $((NUM_OSDS - 1))); do
        line=$(grep -m1 "main device type:" $dir/osd.$id.log)
        echo "osd.$id: ${line:-no 'main device type' log line}" \
            >> $RESULTS_DIR/osds.txt
        if [ -n "$want" ] && [[ "$line" != *"$want"* ]]; then
            echo "ERROR: osd.$id: expected '$want', got: '$line'"
            return 1
        fi
    done
    cat $RESULTS_DIR/osds.txt
}

#
# fio
#

# the client configuration of one mode (hints off/on)
function _client_conf() {
    local dir=$1 mode=$2
    local conf=$dir/client-$mode.conf
    local abs_dir
    abs_dir=$(cd $dir && pwd) || return 1
    cat > $conf <<EOF
[global]
mon host = $CEPH_MON
auth cluster required = none
auth service required = none
auth client required = none
[client]
objecter use osd core hints = $([ $mode = on ] && echo true || echo false)
admin socket = $abs_dir/fio-$mode.\$pid.\$cctid.asok
log file = $abs_dir/fio-$mode.\$pid.log
$(tr ';' '\n' <<<"$CLIENT_CONF_EXTRA" | sed -e 's/^ *//')
EOF
    echo $conf
}

# fio_job <conf> <rw> [extra fio options...]: the job file
function _fio_job() {
    local conf=$1 rw=$2
    shift 2
    # the rados engine ignores 'filesize': it splits a job's 'size' evenly
    # among its 'nrfiles' objects (without 'size', it does no I/O at all)
    local object_bytes
    object_bytes=$(numfmt --from=iec "${FIO_FILESIZE^^}") || return 1
    cat <<EOF
[global]
ioengine=rados
clientname=admin
conf=$conf
pool=$POOL
bs=$FIO_BS
iodepth=$FIO_IODEPTH
numjobs=$FIO_NUMJOBS
nrfiles=$FIO_NRFILES
size=$((FIO_NRFILES * object_bytes))
file_service_type=random
group_reporting=1
$(printf '%s\n' "$@")
[corehints]
rw=$rw
EOF
}

# run fio (on CLIENT_CPUS, if set) on a job file, into RESULTS_DIR/<tag>.json.
# Stopped early if an OSD runs out of memory (see _oom_watchdog()).
function _fio() {
    local dir=$1 tag=$2 job=$3
    local -a pin=()
    [ -n "$CLIENT_CPUS" ] && pin=(taskset -c "$CLIENT_CPUS")
    "${pin[@]}" "$FIO" --output-format=json --output=$RESULTS_DIR/$tag.json \
        $job &
    local fio_pid=$!
    _oom_watchdog $dir $fio_pid &
    local watchdog_pid=$!
    local ret=0
    wait $fio_pid || ret=$?
    kill $watchdog_pid 2>/dev/null
    wait $watchdog_pid 2>/dev/null
    return $ret
}

# kill fio (pid) once an OSD ran out of memory: the OSDs' connections then
# keep failing, ops get stuck, and fio would never complete
function _oom_watchdog() {
    local dir=$1 pid=$2
    while kill -0 $pid 2>/dev/null; do
        sleep 10
        if ! _check_osds_memory $dir > /dev/null; then
            echo "ERROR: an OSD ran out of memory: stopping fio" >&2
            kill $pid 2>/dev/null
            return
        fi
    done
}

# write all the objects once, so that the read workloads find them
function _fio_prefill() {
    local dir=$1
    local conf
    conf=$(_client_conf $dir off) || return 1
    _fio_job $conf write bs=$FIO_PREFILL_BS iodepth=$FIO_PREFILL_IODEPTH \
        > $dir/prefill.fio || return 1
    _fio $dir prefill $dir/prefill.fio || { _check_osds_memory $dir; return 1; }
    _check_osds_memory $dir || return 1
    _check_fio_io prefill || return 1
}

# fail if an OSD ran out of (its reactors' share of) memory: its
# connections then fail and reconnect in a loop, and the results are
# meaningless
function _check_osds_memory() {
    local dir=$1 id
    for id in $(seq 0 $((NUM_OSDS - 1))); do
        if grep -q -m1 bad_alloc $dir/osd.$id.log; then
            echo "ERROR: osd.$id ran out of memory (std::bad_alloc):" \
                 "raise OSD_MEMORY ($OSD_MEMORY for $OSD_SMP reactors)," \
                 "or lower the I/O in flight"
            grep -m3 bad_alloc $dir/osd.$id.log
            return 1
        fi
    done
}

# fail if a fio run completed no I/O (fio itself does not)
function _check_fio_io() {
    local tag=$1
    local ios
    ios=$(jq '.jobs[0] | .read.total_ios + .write.total_ios' \
          $RESULTS_DIR/$tag.json) || return 1
    if [ "$ios" -eq 0 ]; then
        echo "ERROR: fio $tag completed no I/O; see $RESULTS_DIR/$tag.json"
        return 1
    fi
}

# the client_request op_local / op_remote counters of every OSD reactor, as
# "<osd> <shard> <local> <remote>" lines
function _osd_shard_ops() {
    local id
    for id in $(seq 0 $((NUM_OSDS - 1))); do
        ceph tell osd.$id dump_metrics osd_pg_shard --format=json |
            jq -r --arg osd $id '
              [.metrics[] | to_entries[] |
               select(.value.op_type == "client_request") |
               {shard: .value.shard, key: .key, v: .value.value}] |
              group_by(.shard)[] |
              "\($osd) \(.[0].shard) " +
              "\(map(select(.key == "osd_pg_shard_op_local") | .v) | add // 0) " +
              "\(map(select(.key == "osd_pg_shard_op_remote") | .v) | add // 0)"' ||
            return 1
    done
}

# per reactor client ops between two _osd_shard_ops snapshots, as
# "<osd> <shard> <local> <remote>" lines
function _shard_ops_delta() {
    local before=$1 after=$2
    awk 'NR == FNR { l[$1 " " $2] = $3; r[$1 " " $2] = $4; next }
         { print $1, $2, $3 - l[$1 " " $2], $4 - r[$1 " " $2] }' \
        $before $after | sort -n -k1 -k2
}

# 'perf dump <logger>' over an admin socket (the admin socket protocol:
# a JSON command, NUL terminated; the reply is prefixed by its length)
function _asok_perf_dump() {
    local asok=$1 logger=$2
    python3 - "$asok" "$logger" 2>&1 <<'EOF'
import json, socket, struct, sys
s = socket.socket(socket.AF_UNIX)
s.settimeout(10)
s.connect(sys.argv[1])
s.sendall(json.dumps({"prefix": "perf dump", "logger": sys.argv[2]}).encode()
          + b"\0")
def recv(n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise EOFError("admin socket closed")
        buf += chunk
    return buf
print(recv(struct.unpack(">I", recv(4))[0]).decode())
EOF
}

# sum of the objecter op_send / op_send_core counters of a mode's running
# fio clients, printed as "<op_send> <op_send_core> <clients read>".
# The details go to <out>.
function _client_counters() {
    local dir=$1 mode=$2 out=$3
    local asok dump send=0 core=0 s c found=0
    : > $out
    for asok in $dir/fio-$mode.*.asok; do
        [ -S "$asok" ] || continue
        # (python's errors to stdout: stderr also carries the shell's trace)
        if ! dump=$(_asok_perf_dump $asok objecter 2>/dev/null); then
            echo "$asok: perf dump failed: $dump" >> $out
            continue
        fi
        s=$(jq '.objecter.op_send // 0' <<<"$dump")
        c=$(jq '.objecter.op_send_core // 0' <<<"$dump")
        echo "$asok: op_send $s op_send_core $c" >> $out
        send=$((send + s))
        core=$((core + c))
        found=$((found + 1))
    done
    if [ $found = 0 ]; then
        echo "WARNING: no fio client ($mode) counters read; see $out" >&2
        { echo "no readable admin socket in $dir:"; ls -l $dir; } >> $out
    fi
    echo "$send $core $found"
}

# per reactor counters of all OSDs, as lines of
# "<osd> <shard> <busy ms> <polls> <tasks> <net bytes sent> <net bytes received>"
function _reactor_stats() {
    local id
    for id in $(seq 0 $((NUM_OSDS - 1))); do
        { ceph tell osd.$id dump_metrics reactor_ --format=json &&
          ceph tell osd.$id dump_metrics network_ --format=json; } |
            jq -s -r --arg osd $id '
              def total(name): map(select(.k == name) | .v) | add // 0;
              [.[].metrics[] | to_entries[] |
               {k: .key, s: (.value.shard | tonumber), v: .value.value}] |
              group_by(.s)[] |
              "\($osd) \(.[0].s) \(total("reactor_cpu_busy_ms")) " +
              "\(total("reactor_polls")) \(total("reactor_tasks_processed")) " +
              "\(total("network_bytes_sent")) \(total("network_bytes_received"))"' ||
            return 1
    done
}

# per reactor deltas between two _reactor_stats snapshots taken <ms> apart,
# as "<osd> <shard> <busy %> <polls> <tasks> <bytes sent> <bytes received>"
function _reactor_delta() {
    local before=$1 after=$2 ms=$3
    awk -v ms=$ms 'NR == FNR { for (i = 3; i <= 7; i++) b[$1 " " $2, i] = $i; next }
                   ($1 " " $2, 3) in b {
                       printf "%s %s %.1f", $1, $2, 100 * ($3 - b[$1 " " $2, 3]) / ms
                       for (i = 4; i <= 7; i++) printf " %d", $i - b[$1 " " $2, i]
                       printf "\n"
                   }' $before $after | sort -n -k1 -k2
}

# the system's TCP segments, as "<in> <out>"
function _tcp_segs() {
    awk '/^Tcp:/ { if (!hdr) { for (i = 2; i <= NF; i++) col[$i] = i; hdr = 1 }
                   else { print $col["InSegs"], $col["OutSegs"] } }' /proc/net/snmp
}

# the CPU time (clock ticks) used so far by the running fio processes
function _client_cpu_ticks() {
    local pid total=0
    for pid in $(pgrep -x fio); do
        # utime and stime: fields 14 and 15 (the command, field 2, is "(fio)")
        total=$((total + $(awk '{ print $14 + $15 }' /proc/$pid/stat 2>/dev/null || echo 0)))
    done
    echo $total
}

# snapshot of everything measured over the window, into <prefix>.*
function _snapshot() {
    local prefix=$1
    date +%s%3N > $prefix.time
    _osd_shard_ops > $prefix.ops || return 1
    _reactor_stats > $prefix.reactor || return 1
    _tcp_segs > $prefix.tcp
    _client_cpu_ticks > $prefix.client_cpu
}

# one fio run: workload rw, client mode (off/on), repetition rep.
# Appends a line to the summary table.
function _fio_run() {
    local dir=$1 rw=$2 mode=$3 rep=$4
    local tag=$rw-$mode-$rep
    local conf
    conf=$(_client_conf $dir $mode) || return 1
    # the objects exist (see _fio_prefill()): do not touch them again, as
    # that is not part of the workload
    local extra=(time_based=1 runtime=$FIO_RUNTIME ramp_time=$FIO_RAMP
                 touch_objects=0)
    [ $rw = randrw ] && extra+=(rwmixread=70)
    _fio_job $conf $rw "${extra[@]}" > $dir/$tag.fio || return 1

    rm -f $dir/fio-$mode.*.asok

    _fio $dir $tag $dir/$tag.fio &
    local fio_pid=$!
    # the measured part of the run starts after the ramp: sample from
    # (about) then, and until (about) its end - when the fio clients are
    # still alive to be sampled too
    sleep $((FIO_RAMP + 2))
    _snapshot $dir/$tag.s0 || return 1
    sleep $((FIO_RUNTIME - 5))
    _snapshot $dir/$tag.s1 || return 1
    local client
    client=$(_client_counters $dir $mode $RESULTS_DIR/$tag.client.txt)
    wait $fio_pid || {
        echo "ERROR: fio $tag failed"
        _check_osds_memory $dir
        return 1
    }
    _check_osds_memory $dir || return 1
    _check_fio_io $tag || return 1

    local ms=$(( $(cat $dir/$tag.s1.time) - $(cat $dir/$tag.s0.time) ))
    _shard_ops_delta $dir/$tag.s0.ops $dir/$tag.s1.ops > $RESULTS_DIR/$tag.ops.tsv
    _reactor_delta $dir/$tag.s0.reactor $dir/$tag.s1.reactor $ms \
        > $RESULTS_DIR/$tag.reactor.tsv
    awk '{ print $1, $2, $3 }' $RESULTS_DIR/$tag.reactor.tsv \
        > $RESULTS_DIR/$tag.cores.tsv
    local cpu_avg cpu_max
    cpu_avg=$(awk '{ t += $3; n++ } END { printf "%.1f", n ? t / n : 0 }' \
              $RESULTS_DIR/$tag.cores.tsv)
    cpu_max=$(awk 'BEGIN { m = 0 } $3 > m { m = $3 } END { printf "%.1f", m }' \
              $RESULTS_DIR/$tag.cores.tsv)

    local -a c=($client)
    local d_local d_remote
    d_local=$(awk '{ t += $3 } END { print t + 0 }' $RESULTS_DIR/$tag.ops.tsv)
    d_remote=$(awk '{ t += $4 } END { print t + 0 }' $RESULTS_DIR/$tag.ops.tsv)
    local hop_pct client_pct=n/a
    hop_pct=$(awk -v l=$d_local -v r=$d_remote \
                  'BEGIN { t = l + r; printf "%.1f", t ? 100 * r / t : 0 }')
    # per client op, over the window
    local ops=$((d_local + d_remote))
    local tasks_op polls_op segs_op clnt_cores
    tasks_op=$(awk -v ops=$ops '{ t += $5 } END { printf "%.1f", ops ? t / ops : 0 }' \
               $RESULTS_DIR/$tag.reactor.tsv)
    polls_op=$(awk -v ops=$ops '{ t += $4 } END { printf "%.1f", ops ? t / ops : 0 }' \
               $RESULTS_DIR/$tag.reactor.tsv)
    local -a seg0=($(cat $dir/$tag.s0.tcp)) seg1=($(cat $dir/$tag.s1.tcp))
    segs_op=$(awk -v d=$((seg1[1] - seg0[1])) -v ops=$ops \
                  'BEGIN { printf "%.2f", ops ? d / ops : 0 }')
    clnt_cores=$(awk -v d=$(( $(cat $dir/$tag.s1.client_cpu) - $(cat $dir/$tag.s0.client_cpu) )) \
                     -v hz=$(getconf CLK_TCK) -v ms=$ms \
                     'BEGIN { printf "%.1f", ms ? d / hz / (ms / 1000) : 0 }')
    if [ ${c[2]} -gt 0 ] && [ ${c[0]} -gt 0 ]; then
        client_pct=$(awk -v s=${c[0]} -v k=${c[1]} \
                         'BEGIN { printf "%.1f", 100 * k / s }')
    fi

    jq -r --arg rw $rw --arg mode $mode --arg rep $rep \
          --arg hop $hop_pct --arg client $client_pct \
          --arg cpu_avg $cpu_avg --arg cpu_max $cpu_max \
          --arg tasks_op $tasks_op --arg polls_op $polls_op \
          --arg segs_op $segs_op --arg clnt_cores $clnt_cores '
        .jobs[0] as $j |
        [$rw, $mode, $rep,
         ($j.read.iops | floor), ($j.write.iops | floor),
         ($j.read.clat_ns.mean / 1000 | floor),
         (($j.read.clat_ns.percentile["99.000000"] // 0) / 1000 | floor),
         ($j.write.clat_ns.mean / 1000 | floor),
         (($j.write.clat_ns.percentile["99.000000"] // 0) / 1000 | floor),
         $hop, $client, $cpu_avg, $cpu_max,
         $tasks_op, $polls_op, $segs_op, $clnt_cores] | @tsv' \
        $RESULTS_DIR/$tag.json >> $RESULTS_DIR/summary.tsv || return 1
    echo "$d_remote $((d_local + d_remote))" > $dir/$tag.hops
}

function _print_summary() {
    {
        printf 'workload\tmode\trep\trd_iops\twr_iops\trd_lat_us\trd_p99_us\twr_lat_us\twr_p99_us\tosd_hop_%%\tcore_send_%%\tcpu_avg_%%\tcpu_max_%%\ttasks/op\tpolls/op\tsegs/op\tclnt_cores\n'
        cat $RESULTS_DIR/summary.tsv
    } | column -t -s $'\t' | tee $RESULTS_DIR/summary.txt
    echo "fio outputs and summary in $RESULTS_DIR"
}

# with the flag on, most client ops should be served without a cross-core
# hop on the OSD. Prints the offending runs, and fails if STRICT.
function _check_effect() {
    local dir=$1 rw rep ok=0
    local -a off on
    for rw in $WORKLOADS; do
        for rep in $(seq 1 $REPEAT); do
            off=($(cat $dir/$rw-off-$rep.hops))
            on=($(cat $dir/$rw-on-$rep.hops))
            # on: remote/total < half of off: remote/total
            if [ $((2 * on[0] * off[1])) -ge $((off[0] * on[1])) ]; then
                echo "WARNING: $rw #$rep: the flag did not halve the OSD" \
                     "cross-core hops (off: ${off[0]}/${off[1]}," \
                     "on: ${on[0]}/${on[1]})"
                ok=1
            fi
        done
    done
    [ $STRICT = 1 ] && return $ok
    return 0
}

function TEST_core_hints_fio() {
    local dir=$1

    _setup_cluster $dir || return 1
    echo "prefilling $POOL..."
    _fio_prefill $dir || return 1

    : > $RESULTS_DIR/summary.tsv
    local rep rw mode
    for rep in $(seq 1 $REPEAT); do
        for rw in $WORKLOADS; do
            # alternate the order of the modes, so that neither benefits
            # systematically from running second
            local modes="off on"
            [ $((rep % 2)) = 0 ] && modes="on off"
            for mode in $modes; do
                echo "fio: $rw, hints $mode, #$rep"
                _fio_run $dir $rw $mode $rep || return 1
            done
        done
    done

    _print_summary
    _check_effect $dir || return 1
}

main core-hints-fio "$@"

# Local Variables:
# compile-command: "cd ../../../build ; ../qa/run-standalone.sh crimson/core-hints-fio.sh"
# End:
