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
#     (objecter op_send_core vs op_send, from the fio clients' admin sockets).
#
# Run with:
#   cd build && ../qa/run-standalone.sh crimson/core-hints-fio.sh
#
# Tunables (environment):
#   NUM_OSDS       OSDs, and the replicated pool's size         (default: 3)
#   OSD_SMP        reactor cores per OSD (crimson_cpu_num)      (default: 4)
#   OSD_MEMORY     memory per OSD (crimson_memory)              (default: 4G)
#   STORE          cyanstore | seastore | bluestore (alienstore) (default: seastore)
#   SEASTORE_DEVS  seastore: comma-separated block devices, one per OSD (in
#                  OSD id order), as with vstart.sh --seastore-devs. Their
#                  first MiB is zeroed! Unset: a SEASTORE_SIZE block file per
#                  OSD, in the test dir                         (default: unset)
#   SEASTORE_SIZE  seastore block file size, without SEASTORE_DEVS (default: 10G)
#   PG_NUM         the pool's PGs                               (default: 32)
#   WORKLOADS      fio rw modes, from: randread randwrite randrw
#                                     (default: all three; randrw is 70% reads)
#   FIO            the fio binary (with the rados engine)       (default: fio)
#   FIO_RUNTIME, FIO_RAMP   seconds per run, and warm-up        (default: 60, 10)
#   FIO_BS, FIO_IODEPTH, FIO_NUMJOBS                            (default: 4k, 16, 4)
#   FIO_NRFILES, FIO_FILESIZE  objects per fio job, and their size (default: 32, 4m)
#   REPEAT         repetitions of every (workload, mode) pair   (default: 1)
#   RESULTS_DIR    where fio outputs and the summary are kept
#                  (default: ./fio-hints-results.<date> under the build dir)
#   STRICT         1: fail unless the flag removes most of the OSD-side
#                  cross-core hops (default: 0, only warn)

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

NUM_OSDS=${NUM_OSDS:-3}
OSD_SMP=${OSD_SMP:-4}
OSD_MEMORY=${OSD_MEMORY:-4G}
STORE=${STORE:-seastore}
SEASTORE_DEVS=${SEASTORE_DEVS:-}
SEASTORE_SIZE=${SEASTORE_SIZE:-10G}
PG_NUM=${PG_NUM:-32}
WORKLOADS=${WORKLOADS:-"randread randwrite randrw"}
FIO=${FIO:-fio}
FIO_RUNTIME=${FIO_RUNTIME:-60}
FIO_RAMP=${FIO_RAMP:-10}
FIO_BS=${FIO_BS:-4k}
FIO_IODEPTH=${FIO_IODEPTH:-16}
FIO_NUMJOBS=${FIO_NUMJOBS:-4}
FIO_NRFILES=${FIO_NRFILES:-32}
FIO_FILESIZE=${FIO_FILESIZE:-4m}
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
        $func $dir || { teardown $dir 1; return 1; }
        echo "-------------- Teardown Test $func ------------------"
        teardown $dir || return 1
        echo "-------------- Complete Test $func ------------------"
    done
}

#
# Cluster
#

# The vstart.sh equivalent of the cluster, for reference:
# MGR=1 MON=1 OSD=3 MDS=0 RGW=1 ../src/vstart.sh -n --crimson --no-restart --without-dashboard --seastore --seastore-devs /dev/nvme5n1,/dev/nvme6n1,/dev/nvme7n1 --crimson-smp 24 -o "crimson_seastar_blocked_reactor_notify_ms = 10000" --msgr2 -X -o "osd_op_queue=wpq"

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
            if [ -n "$SEASTORE_DEVS" ]; then
                # the device linked as 'block' by _prepare_osd_dev()
                echo "--osd_objectstore=seastore"
            else
                echo "--osd_objectstore=seastore --seastore_device_size=$SEASTORE_SIZE"
            fi ;;
        bluestore)
            echo "--osd_objectstore=bluestore" ;;
        *)
            echo "ERROR: unknown STORE '$STORE'" >&2
            return 1 ;;
    esac
}

# NUM_OSDS Crimson OSDs with per-core listeners, and a replicated crimson pool
function _setup_cluster() {
    local dir=$1

    run_mon $dir a --osd_pool_default_size=$NUM_OSDS \
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
            --crimson_cpu_num=$OSD_SMP \
            --crimson_memory=$OSD_MEMORY \
            --crimson_osd_core_listeners=true \
            $store_args || return 1
    done

    create_pool $POOL $PG_NUM $PG_NUM || return 1
    ceph osd pool set $POOL size $NUM_OSDS --yes-i-really-mean-it || return 1
    wait_for_clean || return 1

    # each OSD must have bound its per-core listeners
    for id in $(seq 0 $((NUM_OSDS - 1))); do
        grep -q "public_msgr core listeners: \[v2:" $dir/osd.$id.log || {
            echo "ERROR: osd.$id has no per-core listeners"
            return 1
        }
    done
}

#
# fio
#

# the client configuration of one mode (hints off/on)
function _client_conf() {
    local dir=$1 mode=$2
    local conf=$dir/client-$mode.conf
    cat > $conf <<EOF
[global]
mon host = $CEPH_MON
auth cluster required = none
auth service required = none
auth client required = none
[client]
objecter use osd core hints = $([ $mode = on ] && echo true || echo false)
admin socket = $dir/fio-$mode.\$pid.\$cctid.asok
log file = $dir/fio-$mode.\$pid.log
EOF
    echo $conf
}

# fio_job <conf> <rw> [extra fio options...]: the job file
function _fio_job() {
    local conf=$1 rw=$2
    shift 2
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
filesize=$FIO_FILESIZE
file_service_type=random
group_reporting=1
$(printf '%s\n' "$@")
[corehints]
rw=$rw
EOF
}

# write all the objects once, so that the read workloads find them
function _fio_prefill() {
    local dir=$1
    local conf
    conf=$(_client_conf $dir off) || return 1
    _fio_job $conf write > $dir/prefill.fio
    "$FIO" --output-format=json --output=$RESULTS_DIR/prefill.json \
        $dir/prefill.fio || return 1
}

# sum of the client_request op_local / op_remote counters over all OSDs,
# printed as "<local> <remote>"
function _osd_hop_counters() {
    local id local_sum=0 remote_sum=0 out l r
    for id in $(seq 0 $((NUM_OSDS - 1))); do
        out=$(ceph tell osd.$id dump_metrics osd_pg_shard --format=json) ||
            return 1
        l=$(jq '[.metrics[] | to_entries[] |
                 select(.key == "osd_pg_shard_op_local" and
                        .value.op_type == "client_request") |
                 .value.value] | add // 0' <<<"$out")
        r=$(jq '[.metrics[] | to_entries[] |
                 select(.key == "osd_pg_shard_op_remote" and
                        .value.op_type == "client_request") |
                 .value.value] | add // 0' <<<"$out")
        local_sum=$((local_sum + l))
        remote_sum=$((remote_sum + r))
    done
    echo "$local_sum $remote_sum"
}

# sum of the objecter op_send / op_send_core counters of a mode's running
# fio clients, printed as "<op_send> <op_send_core>"
function _client_counters() {
    local dir=$1 mode=$2
    local asok out send=0 core=0 s c
    for asok in $dir/fio-$mode.*.asok; do
        [ -S "$asok" ] || continue
        out=$(ceph --admin-daemon $asok perf dump objecter 2>/dev/null) ||
            continue
        s=$(jq '.objecter.op_send // 0' <<<"$out")
        c=$(jq '.objecter.op_send_core // 0' <<<"$out")
        send=$((send + s))
        core=$((core + c))
    done
    echo "$send $core"
}

# one fio run: workload rw, client mode (off/on), repetition rep.
# Appends a line to the summary table.
function _fio_run() {
    local dir=$1 rw=$2 mode=$3 rep=$4
    local tag=$rw-$mode-$rep
    local conf
    conf=$(_client_conf $dir $mode) || return 1
    local extra=(time_based=1 runtime=$FIO_RUNTIME ramp_time=$FIO_RAMP)
    [ $rw = randrw ] && extra+=(rwmixread=70)
    _fio_job $conf $rw "${extra[@]}" > $dir/$tag.fio

    rm -f $dir/fio-$mode.*.asok
    local before after
    before=$(_osd_hop_counters) || return 1

    "$FIO" --output-format=json --output=$RESULTS_DIR/$tag.json \
        $dir/$tag.fio &
    local fio_pid=$!
    # the fio clients exit with fio: sample their counters near the end
    sleep $((FIO_RAMP + FIO_RUNTIME - 3))
    local client
    client=$(_client_counters $dir $mode)
    wait $fio_pid || { echo "ERROR: fio $tag failed"; return 1; }

    after=$(_osd_hop_counters) || return 1

    local -a b=($before) a=($after) c=($client)
    local d_local=$((a[0] - b[0])) d_remote=$((a[1] - b[1]))
    local hop_pct client_pct
    hop_pct=$(awk -v l=$d_local -v r=$d_remote \
                  'BEGIN { t = l + r; printf "%.1f", t ? 100 * r / t : 0 }')
    client_pct=$(awk -v s=${c[0]} -v k=${c[1]} \
                     'BEGIN { printf "%.1f", s ? 100 * k / s : 0 }')

    jq -r --arg rw $rw --arg mode $mode --arg rep $rep \
          --arg hop $hop_pct --arg client $client_pct '
        .jobs[0] as $j |
        [$rw, $mode, $rep,
         ($j.read.iops | floor), ($j.write.iops | floor),
         ($j.read.clat_ns.mean / 1000 | floor),
         (($j.read.clat_ns.percentile["99.000000"] // 0) / 1000 | floor),
         ($j.write.clat_ns.mean / 1000 | floor),
         (($j.write.clat_ns.percentile["99.000000"] // 0) / 1000 | floor),
         $hop, $client] | @tsv' \
        $RESULTS_DIR/$tag.json >> $RESULTS_DIR/summary.tsv || return 1
    echo "$d_remote $((d_local + d_remote))" > $dir/$tag.hops
}

function _print_summary() {
    {
        printf 'workload\tmode\trep\trd_iops\twr_iops\trd_lat_us\trd_p99_us\twr_lat_us\twr_p99_us\tosd_hop_%%\tcore_send_%%\n'
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
