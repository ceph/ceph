#!/bin/bash
# One chaos run, start to finish, on the vstart cluster in $CEPH_BUILD
# (see env.sh), from the source root:
#
#   CEPH_BUILD=$PWD/build src/script/vstart_chaos/run_chaos.sh [options]
#
# CEPH_BUILD defaults to the main checkout's build/ even when the scripts are
# in a worktree; set it explicitly there. The run prints the build dir and
# its binaries' version before destroying anything.
#
#   1. stop the running cluster (by its pid files) and wipe it
#   2. create a fresh cluster with one test pool (setup_cluster.sh)
#   3. optionally rolling-restart mons/OSDs onto a binary snapshot
#      (upgrade_cluster.sh)
#   4. run chaos.py with every read policy, and with a multi-zone pool every
#      zone failover variant
#   5. summarise: result, cycles, findings, crashes, asserts, final health
#
# usage: run_chaos.sh [options]
#   -s DIR    binary snapshot to run (default: the binaries in $CEPH_BUILD)
#   -b DIR    build dir to snapshot first into a new dir under $CHAOS_SNAPS
#             (e.g. a worktree's build/); overrides -s
#   -S SEED   chaos seed (default random)
#   -t SECS   wall-clock limit for the chaos phase (default 36000)
#   -c N      number of cycles, 0 = until failure/limit (default 0)
#   -n NAME   run name (default chaos-<date>-s<seed>)
#   -p TYPE   pool type: erasure (default) or replicated
#   -z N      zones: 1 (default) or 2.  With 2 the pool spans two datacenters
#             in stretch mode: a --num_zones 2 pool where the monitors support
#             it, otherwise global stretch mode (replicated pools only, so
#             without --num_zones in $CEPH_BUILD/bin/ceph-mon -z 2 needs
#             -p replicated)
#   -g        with -z 2, use global stretch mode even where --num_zones exists
#   -k K -m M EC profile (default 2+1)
#   -r N      replicated pool size per zone (default 3, or 2 with zones;
#             global stretch mode needs 2)
#   -L        single-zone EC pool without allow_ec_optimizations
#   -x        stop the cluster after the run (default: leave it for inspection)
#   -y        do not ask before destroying the existing cluster
# Extra arguments after -- are passed to chaos.py.
set -e
. "$(dirname "$0")/env.sh"
B=$CEPH_BUILD
SNAP=
BUILD_DIR= SEED= LIMIT=36000 CYCLES=0 NAME= STOP=0 YES=0
export POOL=chaos POOL_TYPE=erasure ZONES=1 K=2 M=1 REPLICAS= EC_OPT=1 GLOBAL_STRETCH=0
while getopts "s:b:S:t:c:n:p:z:gk:m:r:Lxyh" o; do
    case $o in
        s) SNAP=$OPTARG ;; b) BUILD_DIR=$OPTARG ;; S) SEED=$OPTARG ;;
        t) LIMIT=$OPTARG ;; c) CYCLES=$OPTARG ;; n) NAME=$OPTARG ;;
        p) POOL_TYPE=$OPTARG ;; z) ZONES=$OPTARG ;; g) GLOBAL_STRETCH=1 ;; r) REPLICAS=$OPTARG ;;
        k) K=$OPTARG ;; m) M=$OPTARG ;; L) EC_OPT=0 ;; x) STOP=1 ;; y) YES=1 ;;
        h|*) sed -n '2,/^set -e/p' $0 | sed '$d; s/^# \{0,1\}//'; exit 2 ;;
    esac
done
shift $((OPTIND - 1))
SEED=${SEED:-$((RANDOM * 32768 + RANDOM))}
NAME=${NAME:-chaos-$(date +%m%d-%H%M)-s$SEED}
R=$CHAOS_RUNS/$NAME
[ -e "$R" ] && { echo "$R already exists"; exit 1; }
say() { echo "[$(date +%T)] $*"; }

running_daemons() {
    for f in $B/out/*.pid; do
        [ -e "$f" ] && kill -0 $(cat $f) 2>/dev/null && echo "$(basename $f .pid)"
    done
}

stop_cluster() {
    local pids=() p
    for f in $B/out/*.pid; do
        [ -e "$f" ] || continue
        p=$(cat $f); kill -0 $p 2>/dev/null && pids+=($p)
    done
    [ ${#pids[@]} = 0 ] && return 0
    say "stopping ${#pids[@]} daemons"
    kill ${pids[@]} 2>/dev/null || true
    for i in $(seq 1 60); do
        p=0; for x in ${pids[@]}; do kill -0 $x 2>/dev/null && p=1; done
        [ $p = 0 ] && return 0; sleep 1
    done
    kill -9 ${pids[@]} 2>/dev/null || true
}

# --- 0. preflight
case $POOL_TYPE in erasure|replicated) ;; *) echo "-p must be erasure or replicated"; exit 2 ;; esac
case $ZONES in 1|2) ;; *) echo "-z must be 1 or 2"; exit 2 ;; esac
[ -x $B/bin/ceph-mon ] || { echo "no $B/bin/ceph-mon: set CEPH_BUILD to a build dir"; exit 1; }
# setup_cluster.sh runs on $B's monitors whatever -s or -b say
GLOBAL=
if [ $ZONES = 2 ]; then
    if [ $GLOBAL_STRETCH = 1 ]; then
        GLOBAL="-g"
    elif ! grep -qaF 'name=num_zones,type=CephInt' $B/bin/ceph-mon; then
        GLOBAL="-z 2 with $B/bin/ceph-mon (no 'osd pool create --num_zones')"
    fi
    if [ -n "$GLOBAL" ] && { [ $POOL_TYPE = erasure ] || [ "${REPLICAS:-2}" != 2 ]; }; then
        echo "$GLOBAL takes a replicated pool (-p replicated) with 2 copies per zone"; exit 2
    fi
fi
if [ $POOL_TYPE = erasure ]; then
    CONFIG="erasure k=$K m=$M"
    [ $ZONES = 1 ] && [ $EC_OPT = 0 ] && CONFIG="$CONFIG legacy"
else
    CONFIG="replicated size/zone=${REPLICAS:-$([ $ZONES = 1 ] && echo 3 || echo 2)}"
fi
CONFIG="$CONFIG zones=$ZONES"
[ -n "$GLOBAL" ] && CONFIG="$CONFIG global-stretch"
if pgrep -f '^python3 [c]haos.py' >/dev/null; then
    echo "a chaos run is already in progress"; exit 1
fi
ver=$($B/bin/ceph-mon --version 2>/dev/null | head -1)
top=$(git -C $B rev-parse --show-toplevel 2>/dev/null || true)
WHERE="CEPH_BUILD=$B: ${ver:-ceph-mon --version failed}; source $top at $(git -C $B log -1 --format='%h %s' 2>/dev/null || true)"
[ "$top" = "$(git -C $CHAOS_DIR rev-parse --show-toplevel)" ] || WHERE="$WHERE (not the tree of $CHAOS_DIR)"
up=$(running_daemons | tr '\n' ' ')
if { [ -n "$up" ] || [ -d $B/dev ]; } && [ $YES = 0 ]; then
    echo "$WHERE"
    read -r -p "Destroy the vstart cluster in $B (${up:-not running})? [y/N] " a
    [ "$a" = y ] || exit 1
fi
if [ -n "$BUILD_DIR" ]; then
    SNAP=$CHAOS_SNAPS/$(basename $(dirname $(realpath $BUILD_DIR)))-$(date +%m%d-%H%M)
    say "snapshotting $BUILD_DIR -> $SNAP"
    mkdir -p $CHAOS_SNAPS
    $CHAOS_DIR/make_snapshot.sh $BUILD_DIR $SNAP
fi
[ -z "$SNAP" ] || [ -x $SNAP/bin/ceph-osd ] || { echo "no snapshot at $SNAP"; exit 1; }
# vstart devices are sparse: 8 x (4G block + 512M db + 128M wal) plus logs can
# grow to ~40G over a long run; chaos.py stops cleanly below 10G free
free=$(( $(df --output=avail -k $B | tail -1) / 1048576 ))
old=$(du -sk $B/dev 2>/dev/null | cut -f1)
reclaim=$(( ${old:-0} / 1048576 ))
if [ $((free + reclaim)) -lt 20 ]; then
    echo "$B has ${free}G free (+${reclaim}G from the old cluster); need at least 20G"; exit 1
elif [ $((free + reclaim)) -lt 40 ]; then
    echo "warning: $B has $((free + reclaim))G for the cluster; a long run may stop on the disk guard"
fi
mkdir -p $R
exec > >(tee -a $R/run.log) 2>&1
say "$WHERE"
say "run $NAME: binaries ${SNAP:-$B/bin} ($(cat $SNAP/VERSION 2>/dev/null || git -C $B log -1 --format='%h %s')), seed $SEED, $CONFIG, limit ${LIMIT}s"

# --- 1+2. fresh cluster
stop_cluster
say "creating cluster"
T0=$(date +%s)
OSD=8 $CHAOS_DIR/setup_cluster.sh > $R/setup.log 2>&1 || {
    say "setup failed, see $R/setup.log"; grep '^setup_cluster.sh: ' $R/setup.log; exit 1; }

# --- 3. onto the snapshot
if [ -n "$SNAP" ]; then
    say "upgrading daemons to $SNAP"
    $CHAOS_DIR/upgrade_cluster.sh $SNAP > $R/upgrade.log 2>&1 || { say "upgrade failed, see $R/upgrade.log"; exit 1; }
    export CHAOS_BIN_DIR=$SNAP/bin LD_LIBRARY_PATH=$SNAP/lib:$LD_LIBRARY_PATH \
           CEPH_ARGS="--erasure_code_dir=$SNAP/lib --plugin_dir=$SNAP/lib" \
           RADOSC_LIB=$SNAP/lib/librados.so.2
fi
ceph -s | sed 's/^/    /'

# --- 4. chaos
say "chaos started (tail -f $R/chaos.out)"
set +e
( cd $CHAOS_DIR && timeout $LIMIT python3 chaos.py --pool $POOL --cycles $CYCLES --seed $SEED \
    --quiesce-every 30 --revive-timeout 2400 --clean-timeout 2400 \
    --read-policies none,localize,balance \
    --zf-variants standard,osds_first,flap,surviving_loss,mon_only \
    --rundir $R/chaos "$@" > $R/chaos.out 2>&1 )
rc=$?
set -e
# a failure during the quiesce at the time limit still ends with rc 124
[ $rc = 124 ] && grep -q '>> FATAL' $R/chaos.out && rc=1
if [ $rc != 0 ] && [ $rc != 124 ]; then
    say "saving PG evidence in $R/fail-evidence"
    mkdir -p $R/fail-evidence
    ceph health detail > $R/fail-evidence/health_detail.txt 2>&1 || true
    ceph osd dump -f json > $R/fail-evidence/osd_dump.json 2>&1 || true
    ceph pg dump pgs -f json > $R/fail-evidence/pg_dump.json 2>&1 || true
    for pg in $(ceph pg ls-by-pool $POOL -f json 2>/dev/null | python3 -c '
import json, sys
for p in json.load(sys.stdin)["pg_stats"]:
    if not p["state"].startswith("active+clean"):
        print(p["pgid"])' || true); do
        ceph pg $pg query > $R/fail-evidence/$pg.query.json 2>&1 || true
        ceph pg $pg list_unfound > $R/fail-evidence/$pg.unfound.json 2>&1 || true
    done
fi
dups=$(ceph pg ls-by-pool $POOL -f json 2>/dev/null | python3 -c '
import json, sys
for p in json.load(sys.stdin)["pg_stats"]:
    a = [o for o in p["acting"] if o != 2147483647]
    if len(a) != len(set(a)) and "clean" in p["state"]:
        print(p["pgid"])' || true)
if [ -n "$dups" ]; then
    say "duplicate OSDs in acting sets of $(echo $dups); saving evidence in $R/dup-evidence"
    mkdir -p $R/dup-evidence
    for pg in $dups; do ceph pg $pg query > $R/dup-evidence/$pg.query.json 2>&1 || true; done
    ceph osd dump -f json > $R/dup-evidence/osd_dump.json 2>&1 || true
    ceph pg dump pgs -f json > $R/dup-evidence/pg_dump.json 2>&1 || true
fi
if { [ $rc != 0 ] && [ $rc != 124 ]; } || [ -n "$dups" ]; then
    say "archiving daemon logs to $R/daemon-logs.tar.gz"
    tar -C $B/out -czf $R/daemon-logs.tar.gz $(cd $B/out && ls osd.*.log mon.*.log mgr.*.log 2>/dev/null) || true
fi

# --- 5. conclusion
{
echo "=============== $NAME summary ==============="
echo "binaries : ${SNAP:-$B/bin}  $(cat $SNAP/VERSION 2>/dev/null || git -C $B log -1 --format='%h %s')"
echo "config   : $CONFIG"
echo "pool     : $(grep -m1 -oE '\] pool [^ ]+ id=.*' $R/chaos.out | cut -c8-)"
echo "seed     : $SEED"
echo "duration : $(( ($(date +%s) - T0) / 60 )) min"
last=$(grep -oE 'cycle [0-9]+' $R/chaos.out | tail -1)
case $rc in
    0)   res="PASS: completed $CYCLES cycles" ;;
    124) res="PASS: time limit reached, no failure" ;;
    *)   res="STOPPED rc=$rc: $(grep -m1 'FATAL' $R/chaos.out | sed 's/.*>> //')" ;;
esac
echo "result   : $res"
echo "cycles   : ${last#cycle }"
echo "actions  :"
awk '{print $3}' $R/chaos/timeline.log 2>/dev/null | sed 's/\[.*//' | sort | uniq -c | sort -rn | sed 's/^/    /'
echo "findings :"
[ -s $R/chaos/findings.log ] && sed 's/^/    /' $R/chaos/findings.log || echo "    none"
echo "crashes  :"
ceph crash ls 2>/dev/null | sed 's/^/    /' | grep -v '^    ID' || true
echo "asserts/aborts in daemon logs:"
grep -aHE 'FAILED ceph_assert|\*\*\* Caught signal' $B/out/{osd,mon}.*.log 2>/dev/null \
    | sort -u | head -20 | sed 's/^/    /' || true
echo "duplicate OSDs in acting sets:"
ceph pg ls-by-pool $POOL -f json 2>/dev/null | python3 -c '
import json, sys
for p in json.load(sys.stdin)["pg_stats"]:
    a = [o for o in p["acting"] if o != 2147483647]
    if len(a) != len(set(a)):
        print("   ", p["pgid"], "acting", p["acting"], p["state"])' || true
echo "health   : $(ceph health 2>/dev/null)"
echo "diag     : $(ls -d $R/chaos/diag-* 2>/dev/null | tr '\n' ' ')"
} | tee $R/SUMMARY

# --- 6. teardown
if [ $STOP = 1 ]; then
    stop_cluster
    say "cluster stopped"
else
    say "cluster left running for inspection (stop with: $0 ... -x, or src/stop.sh)"
fi
exit $rc
