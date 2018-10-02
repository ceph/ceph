#!/bin/bash
set -x
WORKUNITS_DIR=$CEPH_BASE/qa/workunits
sudo $WORKUNITS_DIR/deepsea/health-ok.sh --mds --openstack --rgw --tuned=on || exit 1
sudo salt-run state.orch ceph.functests.1node
