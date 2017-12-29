  $ osdmaptool --create-from-conf om -c $TESTDIR/ceph.conf.withracks --with-default-pool
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool om --mark-up-in --mark-out 147 --upmap-max 11 --upmap c
  osdmaptool: osdmap file 'om'
  marking all OSDs up and in
  marking OSD@147 as out
  writing upmap command output to: c
  checking for upmap cleanups
  upmap, max-count 11, max deviation 0.01
  $ cat c
  ceph osd pg-upmap-items 1.7 142 145
  ceph osd pg-upmap-items 1.8 219 223 99 103
  ceph osd pg-upmap-items 1.17 171 173 201 202
  ceph osd pg-upmap-items 1.1a 201 202 115 114
  ceph osd pg-upmap-items 1.1c 171 173 201 202 127 130
  ceph osd pg-upmap-items 1.20 88 87 201 202
  ceph osd pg-upmap-items 1.21 207 206 142 145
  ceph osd pg-upmap-items 1.51 201 202 65 64 186 189
  ceph osd pg-upmap-items 1.62 219 223
  ceph osd pg-upmap-items 1.6f 219 223 108 111
  ceph osd pg-upmap-items 1.82 219 223 157 158 6 3
  $ rm -f om c
