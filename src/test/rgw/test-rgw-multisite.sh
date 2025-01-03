#!/usr/bin/env bash

[ $# -lt 1 ] && echo "usage: $0 <num-zones> <num-zonegroups>[rgw parameters...]" && exit 1

num_zones=$1
num_zonegroups=$2
shift

[ $num_zones -lt 1 ] && echo "clusters num must be at least 1" && exit 1

. "`dirname $0`/test-rgw-common.sh"
. "`dirname $0`/test-rgw-meta-sync.sh"

set -e

realm_name=earth
zg=zg1

system_access_key="1234567890"
system_secret="pencil"

# bring up first cluster
x $(start_ceph_cluster c1) -n $(get_mstart_parameters 1)

if [ -n "$RGW_PER_ZONE" ]; then
  rgws="$RGW_PER_ZONE"
else
  rgws=1
fi

url=http://localhost

i=1
while [ $i -le $rgws ]; do
  port=$((8100+i))
  endpoints="$endpoints""$url:$port,"
  i=$((i+1))
done

# create realm, zonegroup, zone, start rgws
init_first_zone c1 $realm_name $zg ${zg}-1 $endpoints $system_access_key $system_secret
i=1
while [ $i -le $rgws ]; do
  port=$((8100+i))
  x $(rgw c1 "$port" "$@")
  i="$((i+1))"
done

output=`$(rgw_admin c1) realm get`

echo realm_status=$output

# bring up next clusters

endpoints=""
i=2
while [ $i -le $num_zones ]; do
  x $(start_ceph_cluster c$i) -n $(get_mstart_parameters $i)
  j=1
  endpoints=""
  while [ $j -le $rgws ]; do
    port=$((8000+i*100+j))
    endpoints="$endpoints""$url:$port,"
    j=$((j+1))
  done

  # create new zone, start rgw
  init_zone_in_existing_zg c$i $realm_name $zg ${zg}-${i} 8101 $endpoints $zone_port $system_access_key $system_secret
  j=1
  while [ $j -le $rgws ]; do
    port=$((8000+i*100+j))
    x $(rgw c$i "$port" "$@")
    j="$((j+1))"
  done
  i=$((i+1))
done

endpoints=""
k=2
while [ $k -le $num_zonegroups ]; do
  x $(start_ceph_cluster c$i) -n $(get_mstart_parameters $i)
  j=1
  endpoints=""
  while [ $j -le $rgws ]; do
    port=$((8000+i*100+j))
    endpoints="$endpoints""$url:$port,"
    j=$((j+1))
  done

  # create new zone, start rgw
  init_first_zone_in_peer_zg c$i $realm_name zg${k} zg${k}-${i} 8101 $endpoints  $system_access_key $system_secret
  j=1
  while [ $j -le $rgws ]; do
    port=$((8000+i*100+j))
    x $(rgw c$i "$port" "$@")
    j="$((j+1))"
  done
# bring up next clusters in zonegroup k
  i=$((i+1))

  endpoints=""
  l=2
  while [ $l -le $num_zones ]; do
    x $(start_ceph_cluster c$i) -n $(get_mstart_parameters $i)
    j=1
    endpoints=""
    while [ $j -le $rgws ]; do
      port=$((8000+i*100+j))
      endpoints="$endpoints""$url:$port,"
      j=$((j+1))
    done

    # create new zone, start rgw
    init_zone_in_existing_zg c$i $realm_name zg${k} zg${k}-${i} 8101 $endpoints $zone_port $system_access_key $system_secret
    j=1
    while [ $j -le $rgws ]; do
      port=$((8000+i*100+j))
      x $(rgw c$i "$port" "$@")
      j="$((j+1))"
    done
    l=$((l+1))
    i=$((i+1))
  done

  k=$((k+1))
done

