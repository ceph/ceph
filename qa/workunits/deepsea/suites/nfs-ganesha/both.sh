#!/bin/bash
set -x
WORKUNITS_DIR=$CEPH_BASE/qa/workunits
sudo $WORKUNITS_DIR/deepsea/health-ok.sh --client-nodes=1 --mds --min-nodes=2 --nfs-ganesha --rgw
