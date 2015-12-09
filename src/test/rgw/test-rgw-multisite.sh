#!/bin/bash

. "`dirname $0`/test-rgw-common.sh"

set -e


realm_name=earth
zg=us
zone1=${zg}-1
zone2=${zg}-2

zone1_port=8000
zone2_port=8001

system_access_key="1234567890"
system_secret="pencil"

# bring up first cluster
x $(start_ceph_cluster 1) -n

# create realm, zonegroup, zone, start rgw
init_first_zone 1 $realm_name $zg $zone1 $zone1_port $system_access_key $system_secret

output=`$(rgw_admin 1) realm get`

echo realm_status=$output

# bring up second cluster
x $(start_ceph_cluster 2) -n


# create new zone, start rgw
init_zone_in_existing_zg 2 $realm_name $zg $zone1 $zone1_port $system_access_key $system_secret

