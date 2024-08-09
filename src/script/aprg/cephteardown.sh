#!/usr/bin/bash
# basic setup
[ -z "$NUM_RBD_IMAGES" ] && NUM_RBD_IMAGES=1
for (( i=0; i<$NUM_RBD_IMAGES; i++ )); do
  rbd rm fio_test_${i}
done
bin/ceph osd pool rm rbd rbd --yes-i-really-really-mean-it
bin/ceph status
