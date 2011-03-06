# TODO it seems osdmaptool refuses to create files at all without --clobber
  $ osdmaptool --createsimple 3 myosdmap
  osdmaptool: osdmap file 'myosdmap'
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{8,} can't open myosdmap: error 2: No such file or directory (re)
  osdmaptool: couldn't open myosdmap: error 2: No such file or directory
  [255]

  $ osdmaptool --createsimple 3 --clobber myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap

# TODO it seems osdmaptool will happily overwrite without --clobber
  $ osdmaptool --createsimple 3 myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap

  $ ORIG_FSID="$(osdmaptool --print myosdmap|grep ^fsid)"

# hasn't changed yet
#TODO typo
#TODO this one has so many pg_pools only because the above --clobber appended instead of overwriting?
  $ osdmaptool --print myosdmap
  osdmaptool: osdmap file 'myosdmap'
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  modifed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  flags
  
  pg_pool 0 'data' pg_pool(rep pg_size 2 crush_ruleset 0 object_hash rjenkins pg_num 192 pgp_num 192 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 1 'metadata' pg_pool(rep pg_size 2 crush_ruleset 1 object_hash rjenkins pg_num 192 pgp_num 192 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 2 'casdata' pg_pool(rep pg_size 2 crush_ruleset 2 object_hash rjenkins pg_num 192 pgp_num 192 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 3 'rbd' pg_pool(rep pg_size 2 crush_ruleset 3 object_hash rjenkins pg_num 192 pgp_num 192 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 4 'data' pg_pool(rep pg_size 2 crush_ruleset 0 object_hash rjenkins pg_num 192 pgp_num 192 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 5 'metadata' pg_pool(rep pg_size 2 crush_ruleset 1 object_hash rjenkins pg_num 192 pgp_num 192 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 6 'casdata' pg_pool(rep pg_size 2 crush_ruleset 2 object_hash rjenkins pg_num 192 pgp_num 192 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 7 'rbd' pg_pool(rep pg_size 2 crush_ruleset 3 object_hash rjenkins pg_num 192 pgp_num 192 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  
  max_osd 3
  

  $ NEW_FSID="$(osdmaptool --print myosdmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]

  $ osdmaptool --createsimple 1 --clobber myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap

  $ osdmaptool --print myosdmap
  osdmaptool: osdmap file 'myosdmap'
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  modifed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  flags
  
  pg_pool 0 'data' pg_pool(rep pg_size 2 crush_ruleset 0 object_hash rjenkins pg_num 512 pgp_num 64 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 1 'metadata' pg_pool(rep pg_size 2 crush_ruleset 1 object_hash rjenkins pg_num 512 pgp_num 64 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 2 'casdata' pg_pool(rep pg_size 2 crush_ruleset 2 object_hash rjenkins pg_num 512 pgp_num 64 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  pg_pool 3 'rbd' pg_pool(rep pg_size 2 crush_ruleset 3 object_hash rjenkins pg_num 512 pgp_num 64 lpg_num 2 lpgp_num 2 last_change 0 owner 0)
  
  max_osd 1
  

  $ NEW_FSID="$(osdmaptool --print myosdmap|grep ^fsid)"
#TODO --clobber should probably set new fsid, remove the [1]
  $ [ "$ORIG_FSID" != "$NEW_FSID" ]
  [1]
