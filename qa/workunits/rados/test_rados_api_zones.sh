#!/usr/bin/env bash
set -ex
# The librados API tests that need a CRUSH map with two zones, which skip
# themselves elsewhere: put half of the OSDs in each of two datacenters, the
# default zone type, one OSD to a host, and the monitors in the two zones and
# a tiebreaker datacenter, as a pool over zones needs; then run them.

mons=($(ceph mon dump | sed -n 's/^[0-9]*: .* mon\.\([^ ]*\)$/\1/p'))
[ ${#mons[@]} -ge 3 ]
for i in "${!mons[@]}"; do
    dc=zone-a
    [ $((i % 2)) -eq 1 ] && dc=zone-b
    [ $i -eq $(( ${#mons[@]} - 1 )) ] && dc=zone-tiebreaker
    ceph mon set_location ${mons[$i]} datacenter=$dc
done

osds=($(ceph osd ls))
half=$(( ${#osds[@]} / 2 ))
for dc in zone-a zone-b; do
    ceph osd crush add-bucket $dc datacenter
    ceph osd crush move $dc root=default
done
for i in "${!osds[@]}"; do
    dc=zone-a
    [ $i -ge $half ] && dc=zone-b
    ceph osd crush add-bucket zone-host-$i host
    ceph osd crush move zone-host-$i datacenter=$dc
    ceph osd crush move osd.${osds[$i]} host=zone-host-$i
done
ceph osd tree

ceph_test_rados_api_split_op_pp
ceph_test_rados_api_ec_pool_create_pp

echo OK
