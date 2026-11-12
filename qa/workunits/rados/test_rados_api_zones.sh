#!/usr/bin/env bash
set -ex
# ceph_test_rados_api_split_op_pp and ceph_test_rados_api_ec_pool_create_pp
# have tests that create pools over two zones, which they skip unless the
# CRUSH map has two buckets of the zone type, datacenter by default. This
# script gives the cluster that map, then runs both: half of the OSDs go to
# datacenter zone-a and half to zone-b, each OSD on a host of its own. The
# monitors alternate between zone-a and zone-b, and the last one goes to
# zone-tiebreaker, which has no OSDs, as the tiebreaker that a pool over
# zones needs. The rados/singleton rados-api-zones job runs it.

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
