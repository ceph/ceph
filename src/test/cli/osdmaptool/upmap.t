  $ osdmaptool --create-from-conf om -c $TESTDIR/ceph.conf.withracks --with-default-pool
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool --osd_calc_pg_upmaps_aggressively=false om --mark-up-in --upmap-max 11 --upmap c
  osdmaptool: osdmap file 'om'
  marking all OSDs up and in
  writing upmap command output to: c
  checking for upmap cleanups
  upmap, max-count 11, max deviation 5
  pools rbd 
  prepared 11/11 changes
  $ cat c
  ceph osd pg-upmap-items 1.7 142 147
  ceph osd pg-upmap-items 1.8 219 223
  ceph osd pg-upmap-items 1.17 201 202 171 173
  ceph osd pg-upmap-items 1.1a 201 202
  ceph osd pg-upmap-items 1.1c 201 202
  ceph osd pg-upmap-items 1.20 201 202
  ceph osd pg-upmap-items 1.24 232 233
  ceph osd pg-upmap-items 1.51 201 202
  ceph osd pg-upmap-items 1.62 219 223
  ceph osd pg-upmap-items 1.6f 219 223
  $ rm -f om c
