  $ osdmaptool --createsimple 3 myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap

  $ osdmaptool --export-crush oc myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: exported crush map to oc
  $ crushtool --decompile oc
  # begin crush map
  tunable choose_local_tries 0
  tunable choose_local_fallback_tries 0
  tunable choose_total_tries 50
  tunable chooseleaf_descend_once 1
  tunable chooseleaf_vary_r 1
  tunable straw_calc_version 1
  
  # devices
  device 0 osd.0
  device 1 osd.1
  device 2 osd.2
  
  # types
  type 0 osd
  type 1 host
  type 2 chassis
  type 3 rack
  type 4 row
  type 5 pdu
  type 6 pod
  type 7 room
  type 8 datacenter
  type 9 region
  type 10 root
  
  # buckets
  host localhost {
  \tid -2\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.0 weight 1.000 (esc)
  \titem osd.1 weight 1.000 (esc)
  \titem osd.2 weight 1.000 (esc)
  }
  rack localrack {
  \tid -3\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem localhost weight 3.000 (esc)
  }
  root default {
  \tid -1\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem localrack weight 3.000 (esc)
  }
  
  # rules
  rule replicated_ruleset {
  \truleset 0 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take default (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  
  # end crush map
  $ osdmaptool --print myosdmap
  osdmaptool: osdmap file 'myosdmap'
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  modified \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  flags 
  
  pool 0 'rbd' replicated size 3 min_size 2 crush_ruleset 0 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 flags hashpspool stripe_width 0
  
  max_osd 3
  
  $ osdmaptool --clobber --createsimple 3 --osd_pool_default_crush_replicated_ruleset 66 myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap
  $ osdmaptool --print myosdmap | grep 'pool 0'
  osdmaptool: osdmap file 'myosdmap'
  pool 0 'rbd' replicated size 3 min_size 2 crush_ruleset 66 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 flags hashpspool stripe_width 0
  $ osdmaptool --clobber --createsimple 3 --osd_pool_default_crush_rule 55 myosdmap 2>&1 >/dev/null | sed -e 's/^.* 0 osd_pool_//'
  osdmaptool: osdmap file 'myosdmap'
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 0
  $ osdmaptool --print myosdmap | grep 'pool 0'
  osdmaptool: osdmap file 'myosdmap'
  pool 0 'rbd' replicated size 3 min_size 2 crush_ruleset 55 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 flags hashpspool stripe_width 0
  $ osdmaptool --clobber --createsimple 3 --osd_pool_default_crush_replicated_ruleset 66 --osd_pool_default_crush_rule 55 myosdmap 2>&1 >/dev/null | sed -e 's/^.* 0 osd_pool_//'
  osdmaptool: osdmap file 'myosdmap'
  default_crush_rule is deprecated use osd_pool_default_crush_replicated_ruleset instead
  default_crush_rule = 55 overrides osd_pool_default_crush_replicated_ruleset = 66
  $ osdmaptool --print myosdmap | grep 'pool 0'
  osdmaptool: osdmap file 'myosdmap'
  pool 0 'rbd' replicated size 3 min_size 2 crush_ruleset 55 object_hash rjenkins pg_num 192 pgp_num 192 last_change 0 flags hashpspool stripe_width 0
  $ rm -f myosdmap
