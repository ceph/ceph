#!/usr/bin/env bash
#
# Run a vstart cluster (1 mon, 1 mgr) backed by fake OSDs
# (ceph_test_mgr_fake_osd) to load the mgr with a large osdmap/pgmap.
#
# Run from the build dir:
#   ../src/test/mgr/fake-osd-cluster.sh start [-o OSDS] [-p PGS] [-s SIZE] [--no-vstart]
#   ../src/test/mgr/fake-osd-cluster.sh add N
#   ../src/test/mgr/fake-osd-cluster.sh status
#   ../src/test/mgr/fake-osd-cluster.sh stop [--all]

set -euo pipefail

NUM_OSDS=10
NUM_PGS=10000
POOL_SIZE=3
POOL=fakepool
VSTART=1
WAIT_SECS=600
DIR=out/fake-osd

ceph() { ./bin/ceph -c ceph.conf "$@" 2>/dev/null; }
log() { echo "[$(date +%T)] $*"; }
die() { echo "error: $*" >&2; exit 1; }

usage() {
    sed -n '3,10p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
}

[ -x bin/ceph_test_mgr_fake_osd ] ||
    die "run from the build dir after: ninja ceph_test_mgr_fake_osd vstart-base"

wait_for() {
    local what=$1 check=$2 deadline=$((SECONDS + WAIT_SECS))
    until eval "$check"; do
        [ $SECONDS -lt $deadline ] || die "timed out waiting for $what"
        sleep 3
    done
}

num_up() { ceph osd stat | grep -oE '[0-9]+ up' | grep -oE '[0-9]+' || echo 0; }
num_clean() {
    ceph pg stat | grep -oE '[0-9]+ active\+clean[;,]' | grep -oE '^[0-9]+' || echo 0
}

start_cluster() {
    log "starting vstart: 1 mon, 1 mgr, no real osds"
    ../src/stop.sh >/dev/null 2>&1 || true
    pkill -u "$USER" -x ceph_test_mgr_f || true
    rm -rf $DIR
    MDS=0 MGR=1 OSD=0 MON=1 ../src/vstart.sh -n -x --without-dashboard -d \
        > out.vstart.log 2>&1 || die "vstart failed, see out.vstart.log"
    # vstart with OSD=0 sets osd_pool_default_size=0; a size-0 pool corrupts
    # the mon heap as soon as crush has a device
    ceph config set global osd_pool_default_size 1
    ceph osd pool set .mgr size 1 --yes-i-really-mean-it >/dev/null
}

configure() {
    local per_osd=$(( (NUM_PGS * POOL_SIZE + NUM_OSDS - 1) / NUM_OSDS ))
    local max=$(( per_osd * 2 + 100 ))
    [ $max -ge 500 ] || max=500
    log "mon_max_pg_per_osd=$max, mon_osd_max_initial_pgs=$NUM_PGS, autoscaler off"
    ceph config set global mon_max_pg_per_osd $max
    # vstart's ceph.conf sets it too and wins over the config db
    ceph tell 'mon.*' config set mon_max_pg_per_osd $max >/dev/null
    ceph tell 'mgr.*' config set mon_max_pg_per_osd $max >/dev/null
    # create the pool with all its pgs instead of growing it from 1024
    ceph config set mon mon_osd_max_initial_pgs $NUM_PGS
    ceph config set global osd_pool_default_pg_autoscale_mode off
}

add_osds() {
    local n=$1 i uuid id
    mkdir -p $DIR
    for ((i = 0; i < n; i++)); do
        uuid=$(uuidgen)
        id=$(ceph osd new $uuid)
        ceph auth get-or-create osd.$id mon 'allow profile osd' \
            mgr 'allow profile osd' osd 'allow *' -o $DIR/osd.$id.keyring
        ceph osd crush add osd.$id 1.0 host=fakehost-$id root=default >/dev/null
        echo $uuid > $DIR/osd.$id.uuid
        start_osd $id
    done
}

start_osd() {
    local id=$1
    nohup ./bin/ceph_test_mgr_fake_osd -c ceph.conf -i $id \
        --osd-uuid $(cat $DIR/osd.$id.uuid) \
        --keyring $DIR/osd.$id.keyring --log-file $DIR/osd.$id.log \
        > $DIR/osd.$id.stdout 2>&1 &
    echo $! > $DIR/osd.$id.pid
    log "started fake osd.$id (pid $!)"
}

stop_osds() {
    local f
    for f in $DIR/osd.*.pid; do
        [ -e "$f" ] || continue
        kill $(cat $f) 2>/dev/null || true
        rm -f $f
    done
}

status() {
    ceph -s
    echo
    printf "%-8s %-8s %-8s\n" OSD PID RSS_MB
    local f id pid rss
    for f in $DIR/osd.*.pid; do
        [ -e "$f" ] || continue
        id=$(basename $f .pid); pid=$(cat $f)
        rss=$(ps -o rss= -p $pid 2>/dev/null || echo 0)
        printf "%-8s %-8s %-8s\n" $id $pid $((rss / 1024))
    done
}

cmd=${1:-}; [ -n "$cmd" ] || usage; shift
case $cmd in
start)
    while [ $# -gt 0 ]; do
        case $1 in
        -o) NUM_OSDS=$2; shift ;;
        -p) NUM_PGS=$2; shift ;;
        -s) POOL_SIZE=$2; shift ;;
        --no-vstart) VSTART=0 ;;
        *) usage ;;
        esac
        shift
    done
    [ $POOL_SIZE -le $NUM_OSDS ] || die "pool size $POOL_SIZE > $NUM_OSDS osds"
    [ $VSTART -eq 0 ] || start_cluster
    configure
    base_up=$(num_up)
    add_osds $NUM_OSDS
    want=$((base_up + NUM_OSDS))
    log "waiting for $want osds up"
    wait_for "osds up" '[ "$(num_up)" -ge $want ]'
    log "creating pool $POOL: $NUM_PGS pgs, size $POOL_SIZE"
    total=$(( $(ceph pg stat | grep -oE '^[0-9]+') + NUM_PGS ))
    ./bin/ceph -c ceph.conf osd pool create $POOL $NUM_PGS $NUM_PGS \
        replicated --size $POOL_SIZE --autoscale-mode off 2>&1 |
        grep -vE 'DEVELOPER|WARNING' || true
    ceph osd pool ls | grep -qx $POOL || die "pool create failed"
    log "waiting for $total pgs active+clean"
    wait_for "pgs active+clean" '[ "$(num_clean)" -ge "$total" ]'
    log "done"
    status
    ;;
add)
    [ $# -eq 1 ] || usage
    add_osds $1
    ;;
status)
    status
    ;;
stop)
    stop_osds
    log "fake osds stopped"
    if [ "${1:-}" = "--all" ]; then
        ../src/stop.sh >/dev/null 2>&1 || true
        log "cluster stopped"
    fi
    ;;
*)
    usage
    ;;
esac
