  $ monmaptool --create --add foo 2.3.4.5:6789 mymonmap
  *monmaptool: monmap file mymonmap (glob)
  .*monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  *monmaptool: writing epoch 0 to mymonmap (1 monitors) (glob)

  $ ORIG_FSID="$(monmaptool --print mymonmap|grep ^fsid)"

  $ monmaptool --rm doesnotexist mymonmap
  *monmaptool: monmap file mymonmap (glob)
  *monmaptool: removing doesnotexist (glob)
  *monmaptool: map does not contain doesnotexist (glob)
   usage: [--print] [--create [--clobber][--fsid uuid]] [--generate] [--set-initial-members] [--add name 1.2.3.4:567] [--rm name] <mapfilename>
  [1]

  $ monmaptool --print mymonmap
  *monmaptool: monmap file mymonmap (glob)
  epoch 0
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  0: 2.3.4.5:6789/0 mon.foo

  $ NEW_FSID="$(monmaptool --print mymonmap|grep ^fsid)"
  $ [ "$ORIG_FSID" = "$NEW_FSID" ]
