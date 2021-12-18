  $ osdmaptool --createsimple 3 --with-default-pool -o myosdmap
  osdmaptool: writing epoch 1 to myosdmap
  $ osdmaptool --export-crush oc myosdmap
  osdmaptool: input osdmap file 'myosdmap'
  osdmaptool: exported crush map to oc
  $ osdmaptool --import-crush oc myosdmap -o myosdmap --clobber
  osdmaptool: input osdmap file 'myosdmap'
  osdmaptool: imported 497 byte crush map from oc
  osdmaptool: writing epoch 3 to myosdmap
  $ osdmaptool --adjust-crush-weight 0:5 myosdmap
  osdmaptool: input osdmap file 'myosdmap'
  Adjusted osd.0 CRUSH weight to 5
  osdmaptool: successfully built or modified map.  Use '-o <file>' to write it out.
  $ osdmaptool --adjust-crush-weight 0:5 myosdmap -o myosdmap --clobber
  osdmaptool: input osdmap file 'myosdmap'
  Adjusted osd.0 CRUSH weight to 5
  osdmaptool: writing epoch 5 to myosdmap