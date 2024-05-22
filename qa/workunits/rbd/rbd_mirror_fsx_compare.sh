#!/usr/bin/env bash
#
# rbd_mirror_fsx_compare.sh - test rbd-mirror daemon under FSX workload
#
# The script is used to compare FSX-generated images between two clusters.
#

set -ex

. $(dirname $0)/rbd_mirror_helpers.sh

trap 'cleanup $?' INT TERM EXIT

setup_tempdir

testlog "TEST: wait for all images"
image_count=$(rbd --cluster ${CLUSTER1} --pool ${POOL} ls | wc -l)
retrying_seconds=0
sleep_seconds=10
while [ ${retrying_seconds} -le 7200 ]; do
    [ $(rbd --cluster ${CLUSTER2} --pool ${POOL} ls | wc -l) -ge ${image_count} ] && break
    sleep ${sleep_seconds}
    retrying_seconds=$(($retrying_seconds+${sleep_seconds}))
done

testlog "TEST: snapshot all pool images"
snap_id=`uuidgen`
for image in $(rbd --cluster ${CLUSTER1} --pool ${POOL} ls); do
    create_snapshot ${CLUSTER1} ${POOL} ${image} ${snap_id}
done

testlog "TEST: wait for snapshots"
for image in $(rbd --cluster ${CLUSTER1} --pool ${POOL} ls); do
    wait_for_snap_present ${CLUSTER2} ${POOL} ${image} ${snap_id}
done

testlog "TEST: compare image snapshots"
for image in $(rbd --cluster ${CLUSTER1} --pool ${POOL} ls); do
    compare_image_snapshots ${POOL} ${image}
done
