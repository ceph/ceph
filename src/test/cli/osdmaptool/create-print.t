  $ osdmaptool --createsimple 3 myosdmap --with-default-pool
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
  tunable chooseleaf_stable 1
  tunable straw_calc_version 1
  tunable allowed_bucket_algs 54
  
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
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.0 weight 1.000 (esc)
  \titem osd.1 weight 1.000 (esc)
  \titem osd.2 weight 1.000 (esc)
  }
  rack localrack {
  \tid -3\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem localhost weight 3.000 (esc)
  }
  root default {
  \tid -1\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem localrack weight 3.000 (esc)
  }
  
  # rules
  rule replicated_rule {
  \tid 0 (esc)
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
  crush_version 1
  full_ratio 0
  backfillfull_ratio 0
  nearfull_ratio 0
  min_compat_client jewel
  
  pool 1 'rbd' replicated size 3 min_size 2 crush_rule 0 object_hash rjenkins pg_num 192 pgp_num 192 autoscale_mode warn last_change 0 flags hashpspool stripe_width 0 application rbd
  
  max_osd 3
  
  $ osdmaptool --clobber --createsimple 3 --with-default-pool myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap
  $ osdmaptool --print myosdmap | grep 'pool 1'
  osdmaptool: osdmap file 'myosdmap'
  pool 1 'rbd' replicated size 3 min_size 2 crush_rule 0 object_hash rjenkins pg_num 192 pgp_num 192 autoscale_mode warn last_change 0 flags hashpspool stripe_width 0 application rbd
  $ rm -f myosdmap
