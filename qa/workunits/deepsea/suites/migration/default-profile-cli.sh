#!/bin/bash
set -x
WORKUNITS_DIR=$CEPH_BASE/qa/workunits
sudo $WORKUNITS_DIR/deepsea/health-ok.sh --cli
sudo timeout 180m deepsea --log-file=/var/log/salt/deepsea.log --log-level=debug salt-run state.orch ceph.functests.3nodes.migrate --simple-output
