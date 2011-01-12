  $ monmaptool --create mymonmap
  monmaptool: monmap file mymonmap
  failed to open log file '/var/log/ceph/': error 21: Is a directory
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{12} can't open mymonmap: error 2: No such file or directory (re)
  monmaptool: generated fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  monmaptool: writing epoch 1 to mymonmap (0 monitors)

  $ monmaptool --print mymonmap
  monmaptool: monmap file mymonmap
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  last_changed \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
