#!/usr/bin/env bash

set -euo pipefail

scenario=${POOL_FIELD_SCENARIO:?POOL_FIELD_SCENARIO must be set}
pool=pool_fields_upgrade
rule=pool_fields_upgrade_rule
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

case "$scenario" in
    2az|3az) ;;
    *)
        echo "unknown pool field scenario: $scenario"
        exit 1
        ;;
esac

osd_count=$(ceph osd ls | wc -l)
if [[ "$osd_count" -lt 8 ]]; then
    echo "test requires at least 8 OSDs, found $osd_count"
    exit 1
fi

ceph config set osd osd_crush_update_on_start false

for zone in dc1 dc2 dc3; do
    ceph osd crush add-bucket "$zone" datacenter
    ceph osd crush move "$zone" root=default
done

for osd in {0..7}; do
    host=$(printf 'host%02d' "$((osd + 1))")
    ceph osd crush add-bucket "$host" host
    if [[ "$scenario" == 2az ]]; then
        if [[ "$osd" -lt 4 ]]; then
            zone=dc1
        else
            zone=dc2
        fi
    else
        zone=$(printf 'dc%d' "$((osd % 3 + 1))")
    fi
    ceph osd crush move "$host" "datacenter=$zone"
    ceph osd crush move "osd.$osd" "host=$host"
done

ceph osd getcrushmap > "$tmpdir/crushmap"
crushtool --decompile "$tmpdir/crushmap" > "$tmpdir/crushmap.txt"
sed 's/^# end crush map$//' "$tmpdir/crushmap.txt" > "$tmpdir/crushmap_modified.txt"
rule_id=$(ceph osd crush dump -f json | jq '[.rules[].rule_id] | max + 1')

if [[ "$scenario" == 2az ]]; then
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
# end crush map
EOF
else
    cat >> "$tmpdir/crushmap_modified.txt" <<EOF
rule $rule {
        id $rule_id
        type replicated
        step take default
        step choose firstn 3 type datacenter
        step chooseleaf firstn 2 type host
        step emit
}
# end crush map
EOF
fi

crushtool --compile "$tmpdir/crushmap_modified.txt" -o "$tmpdir/crushmap.bin"
ceph osd setcrushmap -i "$tmpdir/crushmap.bin"

if [[ "$scenario" == 2az ]]; then
    ceph mon set election_strategy connectivity
    ceph mon add disallowed_leader c
    ceph mon set_location a datacenter=dc1 host=host01
    ceph mon set_location b datacenter=dc2 host=host05
    ceph mon set_location c datacenter=dc3 host=arbiter

    ceph osd pool create "$pool" 8 8 "$rule"
    ceph osd pool set "$pool" size 4
    ceph osd pool set "$pool" min_size 2
    ceph osd pool application enable "$pool" rados
    ceph mon enable_stretch_mode c "$rule" datacenter
elif [[ "$scenario" == 3az ]]; then
    ceph osd pool create "$pool" 8 8 replicated_rule
    ceph osd pool stretch set "$pool" 2 3 datacenter "$rule" 6 3
    ceph osd pool application enable "$pool" rados
fi