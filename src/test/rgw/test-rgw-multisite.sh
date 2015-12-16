#!/bin/bash

[ $# -lt 1 ] && echo "usage: $0 <num-clusters>" && exit 1

num_clusters=$1

[ $num_clusters -lt 1 ] && echo "clusters num must be at least 1" && exit 1

. "`dirname $0`/test-rgw-common.sh"
. "`dirname $0`/test-rgw-meta-sync.sh"

set -e

realm_name=earth
zg=us

i=1
while [ $i -le $num_clusters ]; do
  eval zone$i=${zg}-$i
  eval zone${i}_port=$((8000+$i))
  i=$((i+1))
done

system_access_key="1234567890"
system_secret="pencil"

# bring up first cluster
x $(start_ceph_cluster 1) -n

# create realm, zonegroup, zone, start rgw
init_first_zone 1 $realm_name $zg $zone1 $zone1_port $system_access_key $system_secret

output=`$(rgw_admin 1) realm get`

echo realm_status=$output

# bring up next clusters

i=2
while [ $i -le $num_clusters ]; do
  x $(start_ceph_cluster $i) -n


  # create new zone, start rgw
  zone_port=eval echo '$'zone${i}_port
  init_zone_in_existing_zg $i $realm_name $zg $zone1 $zone1_port $zone_port $system_access_key $system_secret

  i=$((i+1))
done

i=2
while [ $i -le $num_clusters ]; do
  wait_for_meta_sync 1 $i $realm_name
done

