  $ monmaptool --create mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  monmaptool: writing epoch 0 to mymonmap (0 monitors)

  $ ORIG_FSID="$(monmaptool --print mymonmap|grep ^fsid)"

  $ monmaptool --add foo 2.3.4.5:3300 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 0 to mymonmap (1 monitors)
  $ monmaptool --add bar 3.4.5.6:7890 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 0 to mymonmap (2 monitors)
  $ monmaptool --add baz 4.5.6.7:8901 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 0 to mymonmap (3 monitors)

  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 0
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  0: 2.3.4.5:3300/0 mon.foo
  1: 3.4.5.6:7890/0 mon.bar
  2: 4.5.6.7:8901/0 mon.baz

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]
