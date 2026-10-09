#!/bin/bash
# Recreate the vstart cluster in $CEPH_BUILD (see env.sh) with one test pool.
#   POOL_TYPE  erasure (default) or replicated
#   ZONES      1 (default) or 2; with 2 the OSDs are split over datacenters
#              dc1 and dc2 and the mons get locations (c is the arbiter).
#              Where the monitors support 'osd pool create --num_zones', the
#              pool and the rbd metadata pool (2 copies per zone) are created
#              with --num_zones 2, which enables stretch mode.
#              Otherwise every pool gets a rule placing 2 copies in each
#              datacenter and 'ceph mon enable_stretch_mode' turns on global
#              stretch mode, which takes replicated pools only.
#   GLOBAL_STRETCH  1 to use global stretch mode even where --num_zones exists
#   K, M       EC profile (default 2+1)
#   REPLICAS   replicated pool size per zone (default 3, or 2 with zones;
#              global stretch mode needs 2)
#   EC_OPT     0 to leave allow_ec_optimizations off on a single-zone EC pool
#   OSD        number of OSDs (default 8)
#   POOL       pool name (default chaos)
#   LOG_LEVELS    daemon log levels: low, default or high (run_chaos.sh -l)
#   DEBUG_LEVELS  more levels, comma separated [osd:|mon:|mgr:]SUBSYS=LEVEL
#                 (run_chaos.sh -D)
set -ex
. "$(dirname "$0")/env.sh"
POOL_TYPE=${POOL_TYPE:-erasure} ZONES=${ZONES:-1} POOL=${POOL:-chaos} OSD=${OSD:-8}
case $POOL_TYPE in erasure|replicated) ;; *) echo "bad POOL_TYPE $POOL_TYPE"; exit 1 ;; esac
case $ZONES in 1|2) ;; *) echo "ZONES must be 1 or 2"; exit 1 ;; esac
die() { set +x; echo "setup_cluster.sh: $*" >&2; exit 1; }
cd $CEPH_BUILD
env -u CEPH_KEYRING -u CEPH_CONF CEPH_PORT=30000 CEPH_ARGS='--bluestore_block_size=4294967296 --bluestore_block_wal_size=134217728 --bluestore_block_db_size=536870912' MON=3 OSD=$OSD MDS=0 MGR=1 RGW=0 ../src/vstart.sh -n -d --without-dashboard \
    -o 'osd_pool_default_pg_autoscale_mode=off'
# Daemon log levels go in the config database, so that restarted daemons
# keep them. ceph.conf would override it, so its monitor debug lines go;
# the running monitors read those at startup and get overrides instead.
# Other subsystems keep the levels of vstart -d.
sed -i -E '/^\s*debug (osd|mon|mgr|paxos|auth|monc|mgrc|ms) = /d; s/^(\s*debug client) = [0-9/]+\s*$/\1 = 1\/20/' ceph.conf
levels="osd:osd=1/20 osd:ms=0/5 osd:bluestore=1/10 osd:bluefs=1/10 osd:bdev=1/10 osd:rocksdb=1/5
        mon:mon=1/20 mon:ms=0/5 mon:paxos=1/10 mon:osd=1/20 mon:auth=1/20 mon:mgrc=1/20"
case ${LOG_LEVELS:-default} in
    default) ;;
    low) levels="$levels osd:monc=1/10 osd:objecter=1/10 osd:mgrc=1/10 osd:reserver=1/10
                 osd:objclass=1/10 mgr:mgr=1/20 mgr:ms=0/5 mgr:monc=1/10 mgr:mon=1/10" ;;
    high) levels="$levels osd:osd=20 osd:ms=1 mon:mon=20 mon:ms=1 mon:paxos=10" ;;
    *) die "LOG_LEVELS must be low, default or high" ;;
esac
for kv in $levels ${DEBUG_LEVELS//,/ }; do
    who="osd mon mgr"
    case $kv in *:*) who=${kv%%:*} kv=${kv#*:} ;; esac
    for w in $who; do
        ceph config set $w debug_${kv%%=*} ${kv#*=} || die "cannot set debug_${kv%%=*} to ${kv#*=} for $w"
        if [ $w = mon ]; then
            ceph tell mon.\* config set debug_${kv%%=*} ${kv#*=} >/dev/null
        fi
    done
done
ceph config set osd osd_crush_update_on_start false
ceph config set osd bluestore_debug_inject_read_err true

stretch= per_zone=
if [ $ZONES = 2 ]; then
    # the replicas per zone argument of 'osd pool create' became replica
    read -r stretch per_zone < <(python3 - <<'EOF'
import json, rados
with rados.Rados(conffile="") as cluster:
    ret, out, err = cluster.mon_command(json.dumps({"prefix": "get_command_descriptions"}), b"")
assert ret == 0, err
args = set()
for cmd in json.loads(out).values():
    if cmd["sig"][:3] == ["osd", "pool", "create"]:
        args |= {a.get("name") for a in cmd["sig"] if isinstance(a, dict)}
if "num_zones" in args:
    print("num_zones", "replica" if "replica" in args else "num_replica_per_zone")
else:
    print("global")
EOF
)
    [ "${GLOBAL_STRETCH:-0}" = 1 ] && stretch=global
    if [ $stretch = global ]; then
        [ $POOL_TYPE = replicated ] ||
            die "global stretch mode takes replicated pools only; a two-zone erasure coded pool needs monitors with 'osd pool create --num_zones'"
        [ "${REPLICAS:-2}" = 2 ] || die "global stretch mode keeps 2 copies in each zone; REPLICAS must be 2"
    fi
    half=$(( OSD / 2 ))
    for dc in dc1 dc2; do ceph osd crush add-bucket $dc datacenter; ceph osd crush move $dc root=default; done
    ceph osd crush add-bucket h1 host; ceph osd crush move h1 datacenter=dc1
    ceph osd crush add-bucket h2 host; ceph osd crush move h2 datacenter=dc2
    for o in $(seq 0 $((half-1))); do ceph osd crush set osd.$o 1.0 host=h1; done
    for o in $(seq $half $((OSD-1))); do ceph osd crush set osd.$o 1.0 host=h2; done
    ceph mon set_location a datacenter=dc1
    ceph mon set_location b datacenter=dc2
    ceph mon set_location c datacenter=arbiter
fi

rule=
if [ "$stretch" = global ]; then
    rule=stretch_rule
    t=$(mktemp -d)
    rule_id=$(ceph osd crush rule dump -f json | python3 -c 'import json, sys; print(max(r["rule_id"] for r in json.load(sys.stdin)) + 1)')
    ceph osd getcrushmap -o $t/crush
    crushtool -d $t/crush -o $t/crush.txt
    cat >> $t/crush.txt <<EOF
rule $rule {
    id $rule_id
    type replicated
    step take default
    step choose firstn 0 type datacenter
    step choose firstn 2 type osd
    step emit
}
EOF
    crushtool -c $t/crush.txt -o $t/crush.new
    ceph osd setcrushmap -i $t/crush.new
    rm -rf $t
    # every pool, including one the mgr creates later, needs the stretch rule
    ceph config set mon osd_pool_default_crush_rule $rule_id
    for p in $(ceph osd pool ls); do ceph osd pool set $p crush_rule $rule; done
fi

if [ $POOL_TYPE = erasure ]; then
    if [ "$stretch" = num_zones ]; then
        ceph osd pool create $POOL erasure --k ${K:-2} --m ${M:-1} --num_zones 2 --osd_failure_domain osd --pg_num 16
    else
        ceph osd erasure-code-profile set $POOL-profile k=${K:-2} m=${M:-1} crush-failure-domain=osd
        ceph osd pool create $POOL 16 16 erasure $POOL-profile
        if [ "${EC_OPT:-1}" = 1 ]; then
            ceph osd pool set $POOL allow_ec_optimizations true
        fi
    fi
    ceph osd pool set $POOL allow_ec_overwrites true
elif [ "$stretch" = num_zones ]; then
    ceph osd pool create $POOL replicated --num_zones 2 --$per_zone ${REPLICAS:-2} --osd_failure_domain osd --pg_num 16
elif [ "$stretch" = global ]; then
    ceph osd pool create $POOL 16 16 replicated $rule
else
    ceph osd pool create $POOL replicated --pg_num 16
    ceph osd pool set $POOL size ${REPLICAS:-3} --yes-i-really-mean-it
    r=${REPLICAS:-3}
    ceph osd pool set $POOL min_size $(( r > 1 ? r - 1 : 1 ))
fi
ceph osd pool application enable $POOL rbd
if [ "$stretch" = num_zones ]; then
    ceph osd pool create rbd replicated --num_zones 2 --$per_zone 2 --osd_failure_domain osd --pg_num 8
else
    ceph osd pool create rbd 8 8 replicated $rule
fi
ceph osd pool application enable rbd rbd
if [ "$stretch" = global ]; then
    # the monitor election it starts can re-run it and report "already
    # engaged" although it worked (https://tracker.ceph.com/issues/81136)
    out=$(ceph mon enable_stretch_mode c $rule datacenter 2>&1) ||
        [[ $out == *"stretch mode is already engaged"* ]] || die "enable_stretch_mode: $out"
    for i in $(seq 1 30); do
        ceph osd dump | grep -q '^stretch_mode_enabled true' && break
        [ $i = 30 ] && die "stretch mode is not enabled after 30s"
        sleep 1
    done
fi
ceph osd pool ls detail
