#!/bin/sh -ex
#
# rbd_mirror_bootstrap.sh - test peer bootstrap create/import
#

RBD_MIRROR_MANUAL_PEERS=1
RBD_MIRROR_INSTANCES=${RBD_MIRROR_INSTANCES:-1}
. $(dirname $0)/rbd_mirror_helpers.sh

setup

testlog "TEST: bootstrap cluster2 from cluster1"
# create token on cluster1 and import to cluster2
TOKEN=${TEMPDIR}/peer-token
TOKEN_2=${TEMPDIR}/peer-token-2
CEPH_ARGS='' rbd --cluster ${CLUSTER1} mirror pool peer bootstrap create ${POOL} > ${TOKEN}
CEPH_ARGS='' rbd --cluster ${CLUSTER1} mirror pool peer bootstrap create ${PARENT_POOL} > ${TOKEN_2}
cmp ${TOKEN} ${TOKEN_2}

CEPH_ARGS='' rbd --cluster ${CLUSTER2} --pool ${POOL} mirror pool peer bootstrap import ${TOKEN} --direction rx-only
CEPH_ARGS='' rbd --cluster ${CLUSTER2} --pool ${PARENT_POOL} mirror pool peer bootstrap import ${TOKEN} --direction rx-tx

start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}

testlog "TEST: verify rx-only direction"
# rx-only peer is added immediately by "rbd mirror pool peer bootstrap import"
rbd --cluster ${CLUSTER2} --pool ${POOL} mirror pool info --format json | jq -e '.peers[0].direction == "rx-only"'
# tx-only peer is added asynchronously by mirror_peer_ping class method
while ! rbd --cluster ${CLUSTER1} --pool ${POOL} mirror pool info --format json | jq -e '.peers | length > 0'; do
    sleep 1
done
rbd --cluster ${CLUSTER1} --pool ${POOL} mirror pool info --format json | jq -e '.peers[0].direction == "tx-only"'

create_image_and_enable_mirror ${CLUSTER1} ${POOL} image1

wait_for_image_replay_started ${CLUSTER2} ${POOL} image1
write_image ${CLUSTER1} ${POOL} image1 100
wait_for_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} image1

testlog "TEST: verify rx-tx direction"
# both rx-tx peers are added immediately by "rbd mirror pool peer bootstrap import"
rbd --cluster ${CLUSTER1} --pool ${PARENT_POOL} mirror pool info --format json | jq -e '.peers[0].direction == "rx-tx"'
rbd --cluster ${CLUSTER2} --pool ${PARENT_POOL} mirror pool info --format json | jq -e '.peers[0].direction == "rx-tx"'

create_image ${CLUSTER1} ${PARENT_POOL} image1
create_image ${CLUSTER2} ${PARENT_POOL} image2

enable_mirror ${CLUSTER1} ${PARENT_POOL} image1
enable_mirror ${CLUSTER2} ${PARENT_POOL} image2

wait_for_image_replay_started ${CLUSTER2} ${PARENT_POOL} image1
write_image ${CLUSTER1} ${PARENT_POOL} image1 100
wait_for_replay_complete ${CLUSTER2} ${CLUSTER1} ${PARENT_POOL} image1

wait_for_image_replay_started ${CLUSTER1} ${PARENT_POOL} image2
write_image ${CLUSTER2} ${PARENT_POOL} image2 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${PARENT_POOL} image2
