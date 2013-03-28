  $ radosgw-admin --help
  usage: radosgw-admin <cmd> [options...]
  commands:
    user create                create a new user
    user modify                modify user
    user info                  get user info
    user rm                    remove user
    user suspend               suspend a user
    user enable                reenable user after suspension
    user check                 check user info
    caps add                   add user capabilities
    caps rm                    remove user capabilities
    subuser create             create a new subuser
    subuser modify             modify subuser
    subuser rm                 remove subuser
    key create                 create access key
    key rm                     remove access key
    bucket list                list buckets
    bucket link                link bucket to specified user
    bucket unlink              unlink bucket from specified user
    bucket stats               returns bucket statistics
    bucket rm                  remove bucket
    bucket check               check bucket index
    object rm                  remove object
    object unlink              unlink object from bucket index
    zone info                  show zone params info
    pool add                   add an existing pool for data placement
    pool rm                    remove an existing pool from data placement set
    pools list                 list placement active set
    policy                     read bucket/object policy
    log list                   list log objects
    log show                   dump a log from specific object or (bucket + date
                               + bucket-id)
    log rm                     remove log object
    usage show                 show usage (by user, date range)
    usage trim                 trim usage (by user, date range)
    temp remove                remove temporary objects that were created up to
                               specified date (and optional time)
    gc list                    dump expired garbage collection objects
    gc process                 manually process garbage
  options:
     --uid=<id>                user id
     --subuser=<name>          subuser name
     --access-key=<key>        S3 access key
     --email=<email>
     --secret=<key>            specify secret key
     --gen-access-key          generate random access key (for S3)
     --gen-secret              generate random secret key
     --key-type=<type>         key type, options are: swift, s3
     --access=<access>         Set access permissions for sub-user, should be one
                               of read, write, readwrite, full
     --display-name=<name>
     --bucket=<bucket>
     --pool=<pool>
     --object=<object>
     --date=<date>
     --start-date=<date>
     --end-date=<date>
     --bucket-id=<bucket-id>
     --fix                     besides checking bucket index, will also fix it
     --check-objects           bucket check: rebuilds bucket index according to
                               actual objects state
     --format=<format>         specify output format for certain operations: xml,
                               json
     --purge-data              when specified, user removal will also purge all the
                               user data
     --purge-keys              when specified, subuser removal will also purge all the
                               subuser keys
     --purge-objects           remove a bucket's objects before deleting it
                               (NOTE: required to delete a non-empty bucket)
     --show-log-entries=<flag> enable/disable dump of log entries on log show
     --show-log-sum=<flag>     enable/disable dump of log summation on log show
     --skip-zero-entries       log show only dumps entries that don't have zero value
                               in one of the numeric field
     --categories=<list>       comma separated list of categories, used in usage show
     --caps=<caps>             list of caps (e.g., "usage=read, write; user=read"
     --yes-i-really-mean-it    required for certain operations
  
  <date> := "YYYY-MM-DD[ hh:mm:ss]"
  
    --conf/-c        Read configuration from the given configuration file
    --id/-i          set ID portion of my name
    --name/-n        set name (TYPE.ID)
    --version        show version and quit
  
  [1]
