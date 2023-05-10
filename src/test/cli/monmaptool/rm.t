  $ monmaptool --create --add foo 2.3.4.5:6789 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  setting min_mon_release = quincy
  monmaptool: writing epoch 0 to mymonmap (1 monitors)

  $ ORIG_FSID="$(monmaptool --print mymonmap|grep ^fsid)"

  $ monmaptool --rm foo mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: removing foo
  monmaptool: writing epoch 0 to mymonmap (0 monitors)

  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 0
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+.\d\d\d\d (re)
  created \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+.\d\d\d\d (re)
  min_mon_release 17 (quincy)
  election_strategy: 1

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]
