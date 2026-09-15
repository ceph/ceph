  $ osdmaptool --createsimple 3 om --with-default-pool
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
#
# creation is reported, and is not persisted without --save
#
  $ osdmaptool om --create-osds 2
  osdmaptool: osdmap file 'om'
  created osd.3 through osd.4, max_osd is now 5
  $ osdmaptool --print om | grep max_osd
  osdmaptool: osdmap file 'om'
  max_osd 3
  $ osdmaptool om --create-osds 2 --save
  osdmaptool: osdmap file 'om'
  created osd.3 through osd.4, max_osd is now 5
  osdmaptool: writing epoch 2 to om
  $ osdmaptool --print om | grep max_osd
  osdmaptool: osdmap file 'om'
  max_osd 5
#
# a new osd exists, is up and is in, which is what placement requires of it
#
  $ osdmaptool --print om | grep ^osd.4
  osdmaptool: osdmap file 'om'
  osd.4 up   in  weight 1 * exists,up (glob)
#
# a count below one is refused
#
  $ osdmaptool om --create-osds 0
  osdmaptool: --create-osds requires a count greater than 0
  [1]
#
# --import-crush refuses a crush map wider than max_osd. --create-osds is what
# makes importing one possible, and it is applied first within a single invocation.
#
  $ osdmaptool --createsimple 6 omwide --with-default-pool
  osdmaptool: osdmap file 'omwide'
  osdmaptool: writing epoch 1 to omwide
  $ osdmaptool omwide --export-crush oc
  osdmaptool: osdmap file 'omwide'
  osdmaptool: exported crush map to oc
  $ osdmaptool --createsimple 3 omnarrow --with-default-pool
  osdmaptool: osdmap file 'omnarrow'
  osdmaptool: writing epoch 1 to omnarrow
  $ osdmaptool omnarrow --import-crush oc
  osdmaptool: osdmap file 'omnarrow'
  osdmaptool: crushmap max_devices 6 > osdmap max_osd 3
  [1]
  $ osdmaptool omnarrow --create-osds 3 --import-crush oc
  osdmaptool: osdmap file 'omnarrow'
  created osd.3 through osd.5, max_osd is now 6
  osdmaptool: imported * byte crush map from oc (glob)
  osdmaptool: writing epoch 3 to omnarrow
#
# pgs are placed on an osd the map did not contain before, which is the point
#
  $ osdmaptool omnarrow --mark-up-in --test-map-pgs | grep '^osd\.5'
  osdmaptool: osdmap file 'omnarrow'
  osd\.5\t[1-9][0-9]*\t.* (re)
#
# cleanup
#
  $ rm -f om omwide omnarrow oc
