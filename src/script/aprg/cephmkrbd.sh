#!/usr/bin/bash

if pgrep crimson; then
	bin/ceph daemon -c /ceph/build/ceph.conf osd.0 dump_metrics > /tmp/new_cluster_dump.json
else
	bin/ceph daemon -c /ceph/build/ceph.conf osd.0 perf dump > /tmp/new_cluster_dump.json
fi
# probably add the config opts for the basic/manual here as well

# basic setup
bin/ceph osd pool create rbd
bin/ceph osd pool application enable rbd rbd
[ -z "$NUM_RBD_IMAGES" ] && NUM_RBD_IMAGES=1
for (( i=0; i<$NUM_RBD_IMAGES; i++ )); do
  bin/rbd create --size 10G rbd/fio_test_${i}
  rbd du fio_test_${i}
done
bin/ceph status
