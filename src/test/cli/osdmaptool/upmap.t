  $ osdmaptool --create-from-conf om -c $TESTDIR/ceph.conf.withracks
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool om --mark-up-in --upmap-max 11 --upmap c
  osdmaptool: osdmap file 'om'
  marking all OSDs up and in
  writing upmap command output to: c
  checking for upmap cleanups
  upmap, max-count 11, max deviation 0.01
  osdmaptool: writing epoch 3 to om
  $ cat c
  ceph osd pg-upmap-items 0.3 54 52 156 155
  ceph osd pg-upmap-items 0.1b 158 155 231 227 143 142
  ceph osd pg-upmap-items 0.22 168 163 54 52 136 135
  ceph osd pg-upmap-items 0.2e 87 86 54 52
  ceph osd pg-upmap-items 0.6f 69 65 54 52 157 155
  ceph osd pg-upmap-items 0.12b 54 52 226 227
  ceph osd pg-upmap-items 0.13f 54 52 96 95 43 46
  ceph osd pg-upmap-items 0.151 36 42 54 52
  ceph osd pg-upmap-items 0.185 60 61 54 52
  ceph osd pg-upmap-items 0.1e3 54 52
  ceph osd pg-upmap-items 0.272 54 52
  $ rm -f om c
