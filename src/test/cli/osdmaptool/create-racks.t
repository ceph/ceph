  $ osdmaptool --create-from-conf om -c ceph.conf.manyracks > /dev/null
  $ osdmaptool --test-map-pg 0.0 om
  osdmaptool: osdmap file 'om'
   parsed '0.0' -> 0.0
  0.0 raw [] up [] acting []
