  $ monmaptool --create mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  setting min_mon_release = quincy
  monmaptool: writing epoch 0 to mymonmap (0 monitors)

  $ ORIG_FSID="$(monmaptool --print mymonmap|grep ^fsid)"

  $ monmaptool --add foo 2.3.4.5:6789 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 0 to mymonmap (1 monitors)
  $ monmaptool --add bar 3.4.5.6:7890 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 0 to mymonmap (2 monitors)
  $ monmaptool --add baz 4.5.6.7:8901 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 0 to mymonmap (3 monitors)
  $ monmaptool --addv fiz '[v2:172.21.15.68:6791,v1:172.21.15.68:6792]' mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 0 to mymonmap (4 monitors)
  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 0
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+.\d\d\d\d (re)
  created \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+.\d\d\d\d (re)
  min_mon_release 17 (quincy)
  election_strategy: 1
  0: v1:2.3.4.5:6789/0 mon.foo
  1: [v2:172.21.15.68:6791/0,v1:172.21.15.68:6792/0] mon.fiz
  2: v2:3.4.5.6:7890/0 mon.bar
  3: v2:4.5.6.7:8901/0 mon.baz

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]
