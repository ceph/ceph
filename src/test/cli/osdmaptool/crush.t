  $ osdmaptool --createsimple 3 myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap
  $ osdmaptool --export-crush oc myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: exported crush map to oc
  $ osdmaptool --import-crush oc myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: imported 491 byte crush map from oc
  osdmaptool: writing epoch 3 to myosdmap
