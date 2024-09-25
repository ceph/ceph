  $ ceph balancer off
  $ ceph balancer mode none
  $ ceph osd pool create balancer_opt 128
  pool 'balancer_opt' created
  $ ceph osd pool application enable balancer_opt rados
  enabled application 'rados' on pool 'balancer_opt'
  $ rados bench -p balancer_opt 50 write --no-cleanup > /dev/null
  $ ceph balancer on
  $ ceph balancer mode crush-compat
  $ ceph balancer ls
  []
  $ ceph config set osd.* target_max_misplaced_ratio .07
  $ ceph balancer eval
  current cluster score [0-9]*\.?[0-9]+.* (re)
  read_balance_scores \(lower is better\) {'rbd': [0-9]*\.?[0-9]+.*, 'balancer_opt': [0-9]*\.?[0-9]+.*} (re)
# Turn off active balancer to use manual commands
  $ ceph balancer off
  $ ceph balancer optimize test_plan balancer_opt
  $ ceph balancer ls
  [
      "test_plan"
  ]
  $ ceph balancer execute test_plan
  $ ceph balancer eval
  current cluster score [0-9]*\.?[0-9]+.* (re)
  read_balance_scores \(lower is better\) {'rbd': [0-9]*\.?[0-9]+.*, 'balancer_opt': [0-9]*\.?[0-9]+.*} (re)
# Plan is gone after execution ?
  $ ceph balancer execute test_plan
  Error ENOENT: plan test_plan not found
  [2]
  $ ceph osd pool rm balancer_opt balancer_opt --yes-i-really-really-mean-it
  pool 'balancer_opt' removed
