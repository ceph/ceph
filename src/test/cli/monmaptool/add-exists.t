  $ monmaptool --create mymonmap
  monmaptool: monmap file mymonmap
  failed to open log file '/var/log/ceph/': error 21: Is a directory
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{12} can't open mymonmap: error 2: No such file or directory (re)
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  monmaptool: writing epoch 1 to mymonmap (0 monitors)

  $ ORIG_FSID="$(monmaptool --print mymonmap|grep ^fsid)"

  $ monmaptool --add foo 2.3.4.5:6789 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: writing epoch 2 to mymonmap (1 monitors)
  $ monmaptool --add foo 3.4.5.6:7890 mymonmap
  monmaptool: monmap file mymonmap
  monmaptool: map already contains mon.foo
   usage: [--print] [--create [--clobber]] [--add name 1.2.3.4:567] [--rm name] <mapfilename>
  [1]

  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 2
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  0: 2.3.4.5:6789/0 mon.foo

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]
