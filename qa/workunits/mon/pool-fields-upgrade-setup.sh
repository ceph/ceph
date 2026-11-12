#!/usr/bin/env bash

# The first half of an upgrade test of the pools, their fields (num_zones,
# replica, size, min_size) and their CRUSH rules. On the old release, this
# lays out 8 OSDs, one to a host, 4 in each of the two zones dc1 and dc2,
# and creates the pools and rules of scenario POOL_FIELD_SCENARIO:
#   2az: pool_fields_upgrade, of 4 copies, in global stretch mode, which is
#        enabled with its rule and the tiebreaker monitor c.
#   ec: erasure coded pools of every kind, none of them over zones.
#   rules: pools on rules of every kind, two pools on some, and weight-sets.
#   3az: the OSDs go to datacenters dc1, dc2 and dc3 in turn, and a
#        replicated pool on a rule over all three is made an individual
#        stretch pool with osd pool stretch set.
# Except in 3az, each pool gets 10 objects. The cluster is then upgraded and
# osd require-osd-release vampire, the upgrade commit, is run, and
# pool-fields-upgrade-verify.sh checks what the commit did to the pools and
# reads the objects back. The tentacle-x/stretch and umbrella-x upgrade
# suites run a job for each scenario, and their stretch-zone-loss jobs run
# 2az with a zone lost during the upgrade.

set -euo pipefail

scenario=${POOL_FIELD_SCENARIO:?POOL_FIELD_SCENARIO must be set}
# 2az only: the zones' bucket type, the kind of rule global stretch mode is
# enabled with (take each zone, choose the zones, or choose them in the class
# of the OSDs), and whether each zone is under a region of its own
zone_type=${POOL_FIELD_ZONE_TYPE:-datacenter}
rule_kind=${POOL_FIELD_RULE:-take}
deep=${POOL_FIELD_DEEP:-}
pool=pool_fields_upgrade
rule=pool_fields_upgrade_rule
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

case "$scenario" in
    2az|3az|ec|rules) ;;
    *)
        echo "unknown pool field scenario: $scenario"
        exit 1
        ;;
esac
[[ "$scenario" == 2az ]] || zone_type=datacenter

osd_count=$(ceph osd ls | wc -l)
if [[ "$osd_count" -lt 8 ]]; then
    echo "test requires at least 8 OSDs, found $osd_count"
    exit 1
fi

ceph config set osd osd_crush_update_on_start false

# one device class for every OSD, for the class rules below
class=$(ceph osd crush get-device-class osd.0)
for osd in $(ceph osd ls); do
    if [[ "$(ceph osd crush get-device-class "osd.$osd")" != "$class" ]]; then
        ceph osd crush rm-device-class "osd.$osd"
        ceph osd crush set-device-class "$class" "osd.$osd"
    fi
done

# only 3az has a dc3 bucket: mon enable_stretch_mode and num_zones 2 need
# exactly two buckets of the zone type, so the tiebreaker's dc3 is only a
# monitor location
zones="dc1 dc2"
[[ "$scenario" == 3az ]] && zones="$zones dc3"
for zone in $zones; do
    ceph osd crush add-bucket "$zone" "$zone_type"
    if [[ -n "$deep" ]]; then
        ceph osd crush add-bucket "region-$zone" region
        ceph osd crush move "region-$zone" root=default
        ceph osd crush move "$zone" "region=region-$zone"
    else
        ceph osd crush move "$zone" root=default
    fi
done

for osd in {0..7}; do
    host=$(printf 'host%02d' "$((osd + 1))")
    ceph osd crush add-bucket "$host" host
    if [[ "$scenario" == 3az ]]; then
        zone=$(printf 'dc%d' "$((osd % 3 + 1))")
    elif [[ "$osd" -lt 4 ]]; then
        zone=dc1
    else
        zone=dc2
    fi
    ceph osd crush move "$host" "$zone_type=$zone"
    ceph osd crush move "osd.$osd" "host=$host"
done

ceph osd getcrushmap > "$tmpdir/crushmap"
crushtool --decompile "$tmpdir/crushmap" > "$tmpdir/crushmap.txt"
sed 's/^# end crush map$//' "$tmpdir/crushmap.txt" > "$tmpdir/crushmap_modified.txt"
rule_id=$(ceph osd crush dump -f json | jq '[.rules[].rule_id] | max + 1')

if [[ "$scenario" == 2az && "$rule_kind" == take ]]; then
    cat >> "$tmpdir/crushmap_modified.txt" <<EOF
rule $rule {
        id $rule_id
        type replicated
        step take dc1
        step chooseleaf firstn 2 type host
        step emit
        step take dc2
        step chooseleaf firstn 2 type host
        step emit
}
EOF
elif [[ "$scenario" == 2az ]]; then
    take=default
    [[ "$rule_kind" == class ]] && take="default class $class"
    cat >> "$tmpdir/crushmap_modified.txt" <<EOF
rule $rule {
        id $rule_id
        type replicated
        step take $take
        step choose firstn 2 type $zone_type
        step chooseleaf firstn 2 type host
        step emit
}
EOF
elif [[ "$scenario" == 3az ]]; then
    cat >> "$tmpdir/crushmap_modified.txt" <<EOF
rule $rule {
        id $rule_id
        type replicated
        step take default
        step choose firstn 3 type datacenter
        step chooseleaf firstn 2 type host
        step emit
}
EOF
elif [[ "$scenario" == rules ]]; then
    # Rook's stretch rule, a rule whose placement cannot be read for a pool
    # in one zone, and a rule that no pool uses
    cat >> "$tmpdir/crushmap_modified.txt" <<EOF
rule rook_stretch {
        id $rule_id
        type replicated
        step take default
        step choose firstn 0 type datacenter
        step chooseleaf firstn 2 type host
        step emit
}
rule per_zone_one {
        id $((rule_id + 1))
        type replicated
        step take default
        step choose firstn 0 type datacenter
        step chooseleaf firstn 1 type host
        step emit
}
rule spare {
        id $((rule_id + 2))
        type replicated
        step take default
        step chooseleaf firstn 0 type host
        step emit
}
EOF
else
    # an erasure rule that puts 2 of k+m=4 shards in each zone
    cat >> "$tmpdir/crushmap_modified.txt" <<EOF
rule ec_hand_rule {
        id $rule_id
        type erasure
        step set_chooseleaf_tries 5
        step set_choose_tries 100
        step take default
        step choose indep 2 type datacenter
        step chooseleaf indep 2 type host
        step emit
}
EOF
fi
echo '# end crush map' >> "$tmpdir/crushmap_modified.txt"

crushtool --compile "$tmpdir/crushmap_modified.txt" -o "$tmpdir/crushmap.bin"
ceph osd setcrushmap -i "$tmpdir/crushmap.bin"

# objects whose content follows from their names, for the verify workunit
write_objects() {
    local p=$1 i
    for i in {1..10}; do
        seq "$((i * 1000))" | sed "s/^/$p /" | rados -p "$p" put "upgrade-obj$i" -
    done
}

if [[ "$scenario" != 3az ]]; then
    ceph mon set election_strategy connectivity
    ceph mon add disallowed_leader c
    ceph mon set_location a "$zone_type=dc1" host=host01
    ceph mon set_location b "$zone_type=dc2" host=host05
    ceph mon set_location c "$zone_type=dc3" host=arbiter
fi

if [[ "$scenario" == 2az ]]; then
    ceph osd pool create "$pool" 8 8 "$rule"
    ceph osd pool set "$pool" size 4
    ceph osd pool set "$pool" min_size 2
    ceph osd pool application enable "$pool" rados
    ceph mon enable_stretch_mode c "$rule" "$zone_type"
    write_objects "$pool"
elif [[ "$scenario" == 3az ]]; then
    ceph osd pool create "$pool" 8 8 replicated_rule
    ceph osd pool stretch set "$pool" 2 3 datacenter "$rule" 6 3
    ceph osd pool application enable "$pool" rados
elif [[ "$scenario" == rules ]]; then
    # pools on rules of every kind: class_hosts (made by create-replicated,
    # in the class of the OSDs; rcls1 and rcls2), erasure-code (the rule that
    # erasure coded pools of the default profile share; e1, and e2, a
    # legacy erasure coded pool), osds (made by create-erasure; eosd),
    # rook_stretch (rrook1 and rrook2) and per_zone_one (runread); a compat
    # weight-set and the weight-sets of rcls1 and rrook1
    ceph osd crush rule create-replicated class_hosts default host "$class"
    ceph osd erasure-code-profile set fast22osd k=2 m=2 crush-failure-domain=osd
    ceph osd crush rule create-erasure osds fast22osd
    ceph osd pool create rcls1 8 8 replicated class_hosts
    ceph osd pool create rcls2 8 8 replicated class_hosts
    ceph osd pool create e1 8 8 erasure
    ceph osd pool create e2 8 8 erasure
    ceph osd pool set e1 allow_ec_optimizations true
    ceph osd pool create eosd 8 8 erasure fast22osd osds
    ceph osd pool set eosd allow_ec_optimizations true
    for p in rrook1 rrook2; do
        ceph osd pool create "$p" 8 8 replicated rook_stretch
        ceph osd pool set "$p" size 4
    done
    ceph osd pool create runread 8 8 replicated per_zone_one
    ceph osd pool set runread size 2
    ceph osd crush weight-set create-compat
    ceph osd crush weight-set reweight-compat osd.0 0.9
    # not positional on a class rule: https://tracker.ceph.com/issues/82900
    ceph osd crush weight-set create rcls1 flat
    ceph osd crush weight-set create rrook1 positional
    for p in rcls1 rcls2 e1 e2 eosd rrook1 rrook2 runread; do
        ceph osd pool application enable "$p" rados
        write_objects "$p"
    done
else
    # erasure coded pools of every kind: ec_fast, with allow_ec_optimizations,
    # on a profile of its own, ec_shared1 and ec_shared2, of the default
    # profile, which share the rule erasure-code, ec_legacy, a legacy
    # erasure coded pool, and ec_hand, with allow_ec_optimizations, on a
    # hand-written rule over both zones
    ceph osd erasure-code-profile set fast22 k=2 m=2 crush-failure-domain=host
    ceph osd erasure-code-profile set legacy21 k=2 m=1 crush-failure-domain=host
    ceph osd pool create ec_fast 8 8 erasure fast22
    ceph osd pool create ec_shared1 8 8 erasure
    ceph osd pool create ec_shared2 8 8 erasure
    ceph osd pool create ec_legacy 8 8 erasure legacy21
    ceph osd pool create ec_hand 8 8 erasure fast22 ec_hand_rule
    for p in ec_fast ec_shared1 ec_shared2 ec_hand; do
        ceph osd pool set "$p" allow_ec_optimizations true
    done
    for p in ec_fast ec_shared1 ec_shared2 ec_legacy ec_hand; do
        ceph osd pool application enable "$p" rados
        write_objects "$p"
    done
    test "$(ceph osd pool get ec_shared1 crush_rule -f json | jq -r .crush_rule)" = erasure-code
    test "$(ceph osd pool get ec_shared2 crush_rule -f json | jq -r .crush_rule)" = erasure-code
fi
