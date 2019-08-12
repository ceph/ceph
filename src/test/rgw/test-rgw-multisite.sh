#!/usr/bin/env bash

[ $# -lt 1 ] && echo "usage: $0 <num-clusters>" && exit 1

num_clusters=$1

[ $num_clusters -lt 1 ] && echo "clusters num must be at least 1" && exit 1

. "`dirname $0`/test-rgw-common.sh"
. "`dirname $0`/test-rgw-meta-sync.sh"

set -e

realm_name=earth
zg=zg1

system_access_key="1234567890"
system_secret="pencil"

# bring up first cluster
x $(start_ceph_cluster c1) -n

# create realm, zonegroup, zone, start rgw
init_first_zone c1 $realm_name $zg ${zg}-1 8001 $system_access_key $system_secret
x $(rgw c1 8001)

output=`$(rgw_admin c1) realm get`

echo realm_status=$output

# bring up next clusters

i=2
while [ $i -le $num_clusters ]; do
  x $(start_ceph_cluster c$i) -n

  # create new zone, start rgw
  init_zone_in_existing_zg c$i $realm_name $zg ${zg}-${i} 8001 $((8000+$i)) $zone_port $system_access_key $system_secret
  x $(rgw c$i $((8000+$i)))

  i=$((i+1))
done

i=2
while [ $i -le $num_clusters ]; do
  wait_for_meta_sync c1 c$i $realm_name

  i=$((i+1))
done

