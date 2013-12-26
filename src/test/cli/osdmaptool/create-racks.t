  $ osdmaptool --create-from-conf om -c $TESTDIR/ceph.conf.withracks
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool --test-map-pg 0.0 om
  osdmaptool: osdmap file 'om'
   parsed '0.0' -> 0.0
  0.0 raw [] up [] acting []
  $ osdmaptool --print om
  osdmaptool: osdmap file 'om'
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  modified \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  flags 
  
  pool 0 'data' replicated size 3 min_size 2 crush_ruleset 0 object_hash rjenkins pg_num 15296 pgp_num 15296 last_change 0 owner 0 flags hashpspool crash_replay_interval 45
  pool 1 'metadata' replicated size 3 min_size 2 crush_ruleset 0 object_hash rjenkins pg_num 15296 pgp_num 15296 last_change 0 owner 0 flags hashpspool
  pool 2 'rbd' replicated size 3 min_size 2 crush_ruleset 0 object_hash rjenkins pg_num 15296 pgp_num 15296 last_change 0 owner 0 flags hashpspool
  
  max_osd 239
  

  $ osdmaptool --clobber --create-from-conf --osd_pool_default_crush_replicated_ruleset 55 om -c $TESTDIR/ceph.conf.withracks  
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool --print om | grep 'pool 0'
  osdmaptool: osdmap file 'om'
  pool 0 'data' replicated size 3 min_size 2 crush_ruleset 55 object_hash rjenkins pg_num 15296 pgp_num 15296 last_change 0 owner 0 flags hashpspool crash_replay_interval 45
  $ osdmaptool --clobber --create-from-conf --osd_pool_default_crush_rule 55 om -c $TESTDIR/ceph.conf.withracks 2>&1 >/dev/null | sed -e 's/^.* 0 osd_pool_//'
  osdmaptool: osdmap file 'om'
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 0
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 0
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 0
  $ osdmaptool --print om | grep 'pool 0'
  osdmaptool: osdmap file 'om'
  pool 0 'data' replicated size 3 min_size 2 crush_ruleset 55 object_hash rjenkins pg_num 15296 pgp_num 15296 last_change 0 owner 0 flags hashpspool crash_replay_interval 45
  $ osdmaptool --clobber --create-from-conf --osd_pool_default_crush_replicated_ruleset 66 --osd_pool_default_crush_rule 55 om -c $TESTDIR/ceph.conf.withracks 2>&1 >/dev/null | sed -e 's/^.* 0 osd_pool_//'
  osdmaptool: osdmap file 'om'
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 66
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 66
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 66
  $ osdmaptool --print om | grep 'pool 0'
  osdmaptool: osdmap file 'om'
  pool 0 'data' replicated size 3 min_size 2 crush_ruleset 55 object_hash rjenkins pg_num 15296 pgp_num 15296 last_change 0 owner 0 flags hashpspool crash_replay_interval 45
  $ rm -f om
