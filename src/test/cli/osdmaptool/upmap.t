  $ osdmaptool --create-from-conf om -c $TESTDIR/ceph.conf.withracks
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool om --mark-up-in --upmap-max 11 --upmap c
  osdmaptool: osdmap file 'om'
  marking all OSDs up and in
  writing upmap command output to: c
  checking for upmap cleanups
  upmap, max-count 11, max deviation 0.01
  $ cat c
  ceph osd pg-upmap-items 0.3 54 50 216 212 160 161
  ceph osd pg-upmap-items 0.20 130 128 117 119 54 50
  ceph osd pg-upmap-items 0.89 8 13 54 50
  ceph osd pg-upmap-items 0.8d 219 223 210 209
  ceph osd pg-upmap-items 0.90 163 166 210 209 192 191
  ceph osd pg-upmap-items 0.9e 210 209 27 28
  ceph osd pg-upmap-items 0.12b 54 50 227 225
  ceph osd pg-upmap-items 0.13f 54 50
  ceph osd pg-upmap-items 0.151 36 37 54 50
  ceph osd pg-upmap-items 0.1c0 78 83 43 48 54 50
  ceph osd pg-upmap-items 0.1e3 54 50 197 201
  $ rm -f om c
