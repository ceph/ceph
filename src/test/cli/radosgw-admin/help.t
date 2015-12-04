  $ radosgw-admin --help
  usage: radosgw-admin <cmd> [options...]
  commands:
    user create                create a new user
    user modify                modify user
    user info                  get user info
    user rm                    remove user
    user suspend               suspend a user
    user enable                re-enable user after suspension
    user check                 check user info
    user stats                 show user stats as accounted by quota subsystem
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
    objects expire             run expired objects cleanup
    quota set                  set quota params
    quota enable               enable quota
    quota disable              disable quota
    region get                 show region info
    regions list               list all regions set on this cluster
    region set                 set region info (requires infile)
    region default             set default region
    region-map get             show region-map
    region-map set             set region-map (requires infile)
    zone get                   show zone cluster params
    zone set                   set zone cluster params (requires infile)
    zone list                  list all zones set on this cluster
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
    gc list                    dump expired garbage collection objects (specify
                               --include-all to list all entries, including unexpired)
    gc process                 manually process garbage
    metadata get               get metadata info
    metadata put               put metadata info
    metadata rm                remove metadata info
    metadata list              list metadata info
    mdlog list                 list metadata log
    mdlog trim                 trim metadata log (use start-date, end-date or
                               start-marker, end-marker)
    bilog list                 list bucket index log
    bilog trim                 trim bucket index log (use start-marker, end-marker)
    datalog list               list data log
    datalog trim               trim data log
    opstate list               list stateful operations entries (use client_id,
                               op_id, object)
    opstate set                set state on an entry (use client_id, op_id, object, state)
    opstate renew              renew state on an entry (use client_id, op_id, object)
    opstate rm                 remove entry (use client_id, op_id, object)
    replicalog get             get replica metadata log entry
    replicalog update          update replica metadata log entry
    replicalog delete          delete replica metadata log entry
  options:
     --uid=<id>                user id
     --subuser=<name>          subuser name
     --access-key=<key>        S3 access key
     --email=<email>
     --secret/--secret-key=<key>
                               specify secret key
     --gen-access-key          generate random access key (for S3)
     --gen-secret              generate random secret key
     --key-type=<type>         key type, options are: swift, s3
     --temp-url-key[-2]=<key>  temp url key
     --access=<access>         Set access permissions for sub-user, should be one
                               of read, write, readwrite, full
     --display-name=<name>
     --max_buckets             max number of buckets for a user
     --system                  set the system flag on the user
     --bucket=<bucket>
     --pool=<pool>
     --object=<object>
     --date=<date>
     --start-date=<date>
     --end-date=<date>
     --bucket-id=<bucket-id>
     --shard-id=<shard-id>     optional for mdlog list
                               required for: 
                                 mdlog trim
                                 replica mdlog get/delete
                                 replica datalog get/delete
     --metadata-key=<key>      key to retrieve metadata from with metadata get
     --rgw-region=<region>     region in which radosgw is running
     --rgw-zone=<zone>         zone in which radosgw is running
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
     --sync-stats              option to 'user stats', update user stats with current
                               stats reported by user's buckets indexes
     --show-log-entries=<flag> enable/disable dump of log entries on log show
     --show-log-sum=<flag>     enable/disable dump of log summation on log show
     --skip-zero-entries       log show only dumps entries that don't have zero value
                               in one of the numeric field
     --infile                  specify a file to read in when setting data
     --state=<state string>    specify a state for the opstate set command
     --replica-log-type        replica log type (metadata, data, bucket), required for
                               replica log operations
     --categories=<list>       comma separated list of categories, used in usage show
     --caps=<caps>             list of caps (e.g., "usage=read, write; user=read"
     --yes-i-really-mean-it    required for certain operations
     --reset-regions           reset regionmap when regionmap update
  <date> := "YYYY-MM-DD[ hh:mm:ss]"
  
  Quota options:
     --bucket                  specified bucket for quota command
     --max-objects             specify max objects (negative value to disable)
     --max-size                specify max size (in bytes, negative value to disable)
     --quota-scope             scope of quota (bucket, user)
  
    --conf/-c FILE    read configuration from the given configuration file
    --id/-i ID        set ID portion of my name
    --name/-n TYPE.ID set name
    --cluster NAME    set cluster name (default: ceph)
    --setuser USER    set uid to user or uid (and gid to user's gid)
    --setgroup GROUP  set gid to group or gid
    --version         show version and quit
  
  [1]
 
