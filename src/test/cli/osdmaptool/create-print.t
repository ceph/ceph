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
  
  pool 0 'data' rep size 2 min_size 1 crush_ruleset 0 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0 crash_replay_interval 45
  pool 1 'metadata' rep size 2 min_size 1 crush_ruleset 1 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0
  pool 2 'rbd' rep size 2 min_size 1 crush_ruleset 2 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 owner 0
  
  max_osd 3
  
