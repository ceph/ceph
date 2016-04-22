  $ monmaptool --create mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  monmaptool: writing epoch 0 to mymonmap (0 monitors)

  $ ORIG_FSID="$(monmaptool --print mymonmap|grep ^fsid)"

  $ monmaptool --add foo 2.3.4.5:3300 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 0 to mymonmap (1 monitors)
  $ monmaptool --add foo 3.4.5.6:7890 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: map already contains mon.foo
   usage: [--print] [--create [--clobber][--fsid uuid]] [--generate] [--set-initial-members] [--add name 1.2.3.4:567] [--rm name] <mapfilename>
  [1]

  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 0
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  0: 2.3.4.5:3300/0 mon.foo

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]
