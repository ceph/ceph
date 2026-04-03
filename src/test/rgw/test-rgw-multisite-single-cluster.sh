#!/usr/bin/env bash

# Set up a single-cluster multisite: one RADOS cluster, one realm,
# one zonegroup, two zones, two RGW instances on different ports.
#
# This matches the teuthology multisite topology where both zones
# share the same cluster.
#
# Usage (from build dir):
#   ../src/test/rgw/test-rgw-multisite-single-cluster.sh [rgw params...]
#
# Example with aggressive sync config:
#   ../src/test/rgw/test-rgw-multisite-single-cluster.sh \
#       --rgw-data-sync-poll-interval=5 \
#       --rgw-meta-sync-poll-interval=5 \
#       --rgw-sync-log-trim-interval=0 \
#       --rgw-data-notify-interval-msec=0 \
#       --debug-rgw=1
#
# Cluster: c1
# Zone z1 (primary):   port 8001
# Zone z2 (secondary): port 8002
# System user: zone.user / 1234567890 / pencil
# Regular user: regular.user / 0987654321 / crayon

set -e

script_dir=$(cd "$(dirname "$0")" && pwd)
root_path=$(cd "$script_dir/../.." && pwd)

mstart="$root_path/mstart.sh"
mrun="$root_path/mrun"
mrgw="$root_path/mrgw.sh"

realm="earth"
zg="zg1"
zone1="z1"
zone2="z2"
port1=8001
port2=8002

system_access_key="1234567890"
system_secret="pencil"
regular_access_key="0987654321"
regular_secret="crayon"

echo "=== Setting up single-cluster multisite ==="
echo "Cluster: c1"
echo "Zone $zone1 (primary):   port $port1"
echo "Zone $zone2 (secondary): port $port2"
echo ""

# start one cluster
MON=1 OSD=3 RGW=0 MDS=0 MGR=1 "$mstart" c1 -n -d

# create realm, zonegroup, primary zone
"$mrun" c1 radosgw-admin realm create --rgw-realm="$realm" --default
"$mrun" c1 radosgw-admin zonegroup create --rgw-zonegroup="$zg" \
    --rgw-realm="$realm" --master --default
"$mrun" c1 radosgw-admin zone create --rgw-zonegroup="$zg" \
    --rgw-zone="$zone1" --rgw-realm="$realm" \
    --access-key="$system_access_key" --secret="$system_secret" \
    --endpoints="http://localhost:$port1" --master --default

# create secondary zone in the same zonegroup, same cluster
"$mrun" c1 radosgw-admin zone create --rgw-zonegroup="$zg" \
    --rgw-zone="$zone2" --rgw-realm="$realm" \
    --access-key="$system_access_key" --secret="$system_secret" \
    --endpoints="http://localhost:$port2"

# system user
"$mrun" c1 radosgw-admin user create --uid=zone.user \
    --display-name=ZoneUser \
    --access-key="$system_access_key" --secret="$system_secret" \
    --system

# commit the period
"$mrun" c1 radosgw-admin period update --commit

# regular user for S3 operations
"$mrun" c1 radosgw-admin user create --uid=regular.user \
    --display-name=RegularUser \
    --access-key="$regular_access_key" --secret="$regular_secret"

# start two RGW instances on the same cluster, different zones
"$mrgw" c1 "$port1" 0 --rgw-zone="$zone1" --rgw-zonegroup="$zg" \
    --rgw-realm="$realm" "$@"
"$mrgw" c1 "$port2" 0 --rgw-zone="$zone2" --rgw-zonegroup="$zg" \
    --rgw-realm="$realm" "$@"

echo ""
echo "=== Single-cluster multisite is up ==="
echo "Primary zone ($zone1):   http://localhost:$port1"
echo "Secondary zone ($zone2): http://localhost:$port2"
echo ""
echo "System credentials:  $system_access_key / $system_secret"
echo "Regular credentials: $regular_access_key / $regular_secret"
echo ""
echo "To stop: ../src/mstop.sh c1"
