# TODO handle --help properly, not as an error
  $ radosgw_admin --help
  unrecognized arg --help
  usage: radosgw_admin <cmd> [options...]
  commands:
    user create                create a new user
    user modify                modify user
    user info                  get user info
    user rm                    remove user
    buckets list               list buckets
    bucket unlink              unlink bucket from specified user
    policy                     read bucket/object policy
    log show                   dump a log from specific bucket, date
  options:
     --uid=<id>                S3 uid
     --os-user=<group:name>    OpenStack user
     --email=<email>
     --auth_uid=<auid>         librados uid
     --secret=<key>            S3 key
     --os-secret=<key>         OpenStack key
     --display-name=<name>
     --bucket=<bucket>
     --object=<object>
     --date=<yyyy-mm-dd>
  --conf/-c        Read configuration from the given configuration file
  -D               Run in the foreground.
  -f               Run in foreground. Show all log messages on stderr.
  --id             set ID
  --name           set ID.TYPE
  --version        show version and quit
  
  [1]
