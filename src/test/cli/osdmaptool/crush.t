  $ osdmaptool --createsimple 3 myosdmap
  *osdmaptool: osdmap file 'myosdmap' (glob)
  *osdmaptool: writing epoch 1 to myosdmap (glob)
  $ osdmaptool --export-crush oc myosdmap
  *osdmaptool: osdmap file 'myosdmap' (glob)
  *osdmaptool: exported crush map to oc (glob)
  $ osdmaptool --import-crush oc myosdmap
  *osdmaptool: osdmap file 'myosdmap' (glob)
  *osdmaptool: imported 486 byte crush map from oc (glob)
  *osdmaptool: writing epoch 3 to myosdmap (glob)
