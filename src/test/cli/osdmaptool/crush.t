  $ osdmaptool --createsimple 3 myosdmap --with-default-pool
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap
  $ osdmaptool --export-crush oc myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: exported crush map to oc
  $ osdmaptool --import-crush oc myosdmap
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: imported 497 byte crush map from oc
  osdmaptool: writing epoch 3 to myosdmap
  $ osdmaptool --adjust-crush-weight 0:5 myosdmap
  osdmaptool: osdmap file 'myosdmap'
  Adjusted osd.0 CRUSH weight to 5
  $ osdmaptool --adjust-crush-weight 0:5 myosdmap --save
  osdmaptool: osdmap file 'myosdmap'
  Adjusted osd.0 CRUSH weight to 5
  osdmaptool: writing epoch 5 to myosdmap