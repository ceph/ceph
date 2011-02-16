  $ monmaptool --create mymonmap
  monmaptool: monmap file mymonmap
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{8,} can't open mymonmap: error 2: No such file or directory (re)
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  monmaptool: writing epoch 1 to mymonmap (0 monitors)

  $ ORIG_FSID="$(monmaptool --print mymonmap|grep ^fsid)"

  $ monmaptool --add foo 2.3.4.5:6789 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 2 to mymonmap (1 monitors)
  $ monmaptool --add bar 3.4.5.6:7890 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 3 to mymonmap (2 monitors)
  $ monmaptool --add baz 4.5.6.7:8901 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 4 to mymonmap (3 monitors)

  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 4
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  0: 3.4.5.6:7890/0 mon.bar
  1: 4.5.6.7:8901/0 mon.baz
  2: 2.3.4.5:6789/0 mon.foo

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]
