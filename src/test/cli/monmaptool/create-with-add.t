  $ monmaptool --create --add foo 2.3.4.5:6789 mymonmap
  *monmaptool: monmap file mymonmap (glob)
  .*monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  *monmaptool: writing epoch 0 to mymonmap (1 monitors) (glob)

  $ monmaptool --print mymonmap
  *monmaptool: monmap file mymonmap (glob)
  epoch 0
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  0: 2.3.4.5:6789/0 mon.foo
