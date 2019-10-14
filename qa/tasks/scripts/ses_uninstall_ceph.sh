set -ex 


salt-run state.orch ceph.purge || true
sleep 3
salt-run disengage.safety
sleep 3
salt-run state.orch ceph.purge 

