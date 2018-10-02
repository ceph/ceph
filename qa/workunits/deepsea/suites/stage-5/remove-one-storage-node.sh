#!/bin/bash
set -x
WORKUNITS_DIR=$CEPH_BASE/qa/workunits
sudo $WORKUNITS_DIR/deepsea/health-ok.sh --min-nodes=3 --client-nodes=0 || exit 1
sudo $WORKUNITS_DIR/deepsea/stage-5.sh
