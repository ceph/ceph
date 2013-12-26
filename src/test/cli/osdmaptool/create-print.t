  $ osdmaptool --createsimple 3 myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap

  $ osdmaptool --print myosdmap
  osdmaptool: osdmap file 'myosdmap'
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  modified \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  flags 
  
  pool 0 'data' replicated size 3 min_size 2 crush_ruleset 0 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0 flags hashpspool crash_replay_interval 45
  pool 1 'metadata' replicated size 3 min_size 2 crush_ruleset 0 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0 flags hashpspool
  pool 2 'rbd' replicated size 3 min_size 2 crush_ruleset 0 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0 flags hashpspool
  
  max_osd 3
  
  $ osdmaptool --clobber --createsimple 3 --osd_pool_default_crush_replicated_ruleset 66 myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap
  $ osdmaptool --print myosdmap | grep 'pool 0'
  osdmaptool: osdmap file 'myosdmap'
  pool 0 'data' replicated size 3 min_size 2 crush_ruleset 66 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0 flags hashpspool crash_replay_interval 45
  $ osdmaptool --clobber --createsimple 3 --osd_pool_default_crush_rule 55 myosdmap 2>&1 >/dev/null | sed -e 's/^.* 0 osd_pool_//'
  osdmaptool: osdmap file 'myosdmap'
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 0
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 0
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 0
  $ osdmaptool --print myosdmap | grep 'pool 0'
  osdmaptool: osdmap file 'myosdmap'
  pool 0 'data' replicated size 3 min_size 2 crush_ruleset 55 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0 flags hashpspool crash_replay_interval 45
  $ osdmaptool --clobber --createsimple 3 --osd_pool_default_crush_replicated_ruleset 66 --osd_pool_default_crush_rule 55 myosdmap 2>&1 >/dev/null | sed -e 's/^.* 0 osd_pool_//'
  osdmaptool: osdmap file 'myosdmap'
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 66
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 66
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 66
  $ osdmaptool --print myosdmap | grep 'pool 0'
  osdmaptool: osdmap file 'myosdmap'
  pool 0 'data' replicated size 3 min_size 2 crush_ruleset 55 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0 flags hashpspool crash_replay_interval 45
  $ rm -f myosdmap
