  $ touch empty

  $ cauthtool --list empty
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{12} auth: parse error at line 2: (re)
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{12} auth:[ ] (re)
  error reading file empty
  [1]

  $ cauthtool -l empty
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{12} auth: parse error at line 2: (re)
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{12} auth:[ ] (re)
  error reading file empty
  [1]
