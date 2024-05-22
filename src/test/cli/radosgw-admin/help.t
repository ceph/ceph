  $ radosgw-admin --help
  usage: radosgw-admin <cmd> [options...]
  commands:
    user create                      create a new user
    user modify                      modify user
    user info                        get user info
    user rename                      rename user
    user rm                          remove user
    user suspend                     suspend a user
    user enable                      re-enable user after suspension
    user check                       check user info
    user stats                       show user stats as accounted by quota subsystem
    user list                        list users
    caps add                         add user capabilities
    caps rm                          remove user capabilities
    subuser create                   create a new subuser
    subuser modify                   modify subuser
    subuser rm                       remove subuser
    key create                       create access key
    key rm                           remove access key
    bucket list                      list buckets (specify --allow-unordered for faster, unsorted listing)
    bucket limit check               show bucket sharding stats
    bucket link                      link bucket to specified user
    bucket unlink                    unlink bucket from specified user
    bucket stats                     returns bucket statistics
    bucket rm                        remove bucket
    bucket check                     check bucket index by verifying size and object count stats
    bucket check olh                 check for olh index entries and objects that are pending removal
    bucket check unlinked            check for object versions that are not visible in a bucket listing 
    bucket chown                     link bucket to specified user and update its object ACLs
    bucket reshard                   reshard bucket
    bucket rewrite                   rewrite all objects in the specified bucket
    bucket sync checkpoint           poll a bucket's sync status until it catches up to its remote
    bucket sync disable              disable bucket sync
    bucket sync enable               enable bucket sync
    bucket radoslist                 list rados objects backing bucket's objects
    bi get                           retrieve bucket index object entries
    bi put                           store bucket index object entries
    bi list                          list raw bucket index entries
    bi purge                         purge bucket index entries
    object rm                        remove object
    object put                       put object
    object stat                      stat an object for its metadata
    object unlink                    unlink object from bucket index
    object rewrite                   rewrite the specified object
    object reindex                   reindex the object(s) indicated by --bucket and either --object or --objects-file
    objects expire                   run expired objects cleanup
    objects expire-stale list        list stale expired objects (caused by reshard)
    objects expire-stale rm          remove stale expired objects
    period rm                        remove a period
    period get                       get period info
    period get-current               get current period info
    period pull                      pull a period
    period push                      push a period
    period list                      list all periods
    period update                    update the staging period
    period commit                    commit the staging period
    quota set                        set quota params
    quota enable                     enable quota
    quota disable                    disable quota
    ratelimit get                    get ratelimit params
    ratelimit set                    set ratelimit params
    ratelimit enable                 enable ratelimit
    ratelimit disable                disable ratelimit
    global quota get                 view global quota params
    global quota set                 set global quota params
    global quota enable              enable a global quota
    global quota disable             disable a global quota
    global ratelimit get             view global ratelimit params
    global ratelimit set             set global ratelimit params
    global ratelimit enable          enable a ratelimit quota
    global ratelimit disable         disable a ratelimit quota
    realm create                     create a new realm
    realm rm                         remove a realm
    realm get                        show realm info
    realm get-default                get default realm name
    realm list                       list realms
    realm list-periods               list all realm periods
    realm rename                     rename a realm
    realm set                        set realm info (requires infile)
    realm default                    set realm as default
    realm pull                       pull a realm and its current period
    zonegroup add                    add a zone to a zonegroup
    zonegroup create                 create a new zone group info
    zonegroup default                set default zone group
    zonegroup delete                 delete a zone group info
    zonegroup get                    show zone group info
    zonegroup modify                 modify an existing zonegroup
    zonegroup set                    set zone group info (requires infile)
    zonegroup rm                     remove a zone from a zonegroup
    zonegroup rename                 rename a zone group
    zonegroup list                   list all zone groups set on this cluster
    zonegroup placement list         list zonegroup's placement targets
    zonegroup placement get          get a placement target of a specific zonegroup
    zonegroup placement add          add a placement target id to a zonegroup
    zonegroup placement modify       modify a placement target of a specific zonegroup
    zonegroup placement rm           remove a placement target from a zonegroup
    zonegroup placement default      set a zonegroup's default placement target
    zone create                      create a new zone
    zone rm                          remove a zone
    zone get                         show zone cluster params
    zone modify                      modify an existing zone
    zone set                         set zone cluster params (requires infile)
    zone list                        list all zones set on this cluster
    zone rename                      rename a zone
    zone placement list              list zone's placement targets
    zone placement get               get a zone placement target
    zone placement add               add a zone placement target
    zone placement modify            modify a zone placement target
    zone placement rm                remove a zone placement target
    metadata sync status             get metadata sync status
    metadata sync init               init metadata sync
    metadata sync run                run metadata sync
    data sync status                 get data sync status of the specified source zone
    data sync init                   init data sync for the specified source zone
    data sync run                    run data sync for the specified source zone
    pool add                         add an existing pool for data placement
    pool rm                          remove an existing pool from data placement set
    pools list                       list placement active set
    policy                           read bucket/object policy
    log list                         list log objects
    log show                         dump a log from specific object or (bucket + date + bucket-id)
                                     (NOTE: required to specify formatting of date to "YYYY-MM-DD-hh")
    log rm                           remove log object
    usage show                       show usage (by user, by bucket, date range)
    usage trim                       trim usage (by user, by bucket, date range)
    usage clear                      reset all the usage stats for the cluster
    gc list                          dump expired garbage collection objects (specify
                                     --include-all to list all entries, including unexpired)
    gc process                       manually process garbage (specify
                                     --include-all to process all entries, including unexpired)
    lc list                          list all bucket lifecycle progress
    lc get                           get a lifecycle bucket configuration
    lc process                       manually process lifecycle
    lc reshard fix                   fix LC for a resharded bucket
    metadata get                     get metadata info
    metadata put                     put metadata info
    metadata rm                      remove metadata info
    metadata list                    list metadata info
    mdlog list                       list metadata log
    mdlog autotrim                   auto trim metadata log
    mdlog trim                       trim metadata log (use marker)
    mdlog status                     read metadata log status
    bilog list                       list bucket index log
    bilog trim                       trim bucket index log (use start-marker, end-marker)
    bilog status                     read bucket index log status
    bilog autotrim                   auto trim bucket index log
    datalog list                     list data log
    datalog trim                     trim data log
    datalog status                   read data log status
    datalog type                     change datalog type to --log_type={fifo,omap}
    orphans find                     deprecated -- init and run search for leaked rados objects (use job-id, pool)
    orphans finish                   deprecated -- clean up search for leaked rados objects
    orphans list-jobs                deprecated -- list the current job-ids for orphans search
      * the three 'orphans' sub-commands are now deprecated; consider using the `rgw-orphan-list` tool
    role create                      create a AWS role for use with STS
    role delete                      remove a role
    role get                         get a role
    role list                        list roles with specified path prefix
    role-trust-policy modify         modify the assume role policy of an existing role
    role-policy put                  add/update permission policy to role
    role-policy list                 list policies attached to a role
    role-policy get                  get the specified inline policy document embedded with the given role
    role-policy delete               remove policy attached to a role
    role update                      update max_session_duration of a role
    reshard add                      schedule a resharding of a bucket
    reshard list                     list all bucket resharding or scheduled to be resharded
    reshard status                   read bucket resharding status
    reshard process                  process of scheduled reshard jobs
    reshard cancel                   cancel resharding a bucket
    reshard stale-instances list     list stale-instances from bucket resharding
    reshard stale-instances delete   cleanup stale-instances from bucket resharding
    sync error list                  list sync error
    sync error trim                  trim sync error
    mfa create                       create a new MFA TOTP token
    mfa list                         list MFA TOTP tokens
    mfa get                          show MFA TOTP token
    mfa remove                       delete MFA TOTP token
    mfa check                        check MFA TOTP token
    mfa resync                       re-sync MFA TOTP token
    topic list                       list bucket notifications topics
    topic get                        get a bucket notifications topic
    topic rm                         remove a bucket notifications topic
    topic stats                      get a bucket notifications persistent topic stats (i.e. reservations, entries & size)
    script put                       upload a Lua script to a context
    script get                       get the Lua script of a context
    script rm                        remove the Lua scripts of a context
    script-package add               add a Lua package to the scripts allowlist
    script-package rm                remove a Lua package from the scripts allowlist
    script-package list              get the Lua packages allowlist
    script-package reload            install/remove Lua packages according to allowlist
    notification list                list bucket notifications configuration
    notification get                 get a bucket notifications configuration
    notification rm                  remove a bucket notifications configuration
  options:
     --tenant=<tenant>                 tenant name
     --user_ns=<namespace>             namespace of user (oidc in case of users authenticated with oidc provider)
     --uid=<id>                        user id
     --new-uid=<id>                    new user id
     --subuser=<name>                  subuser name
     --access-key=<key>                S3 access key
     --email=<email>                   user's email address
     --secret/--secret-key=<key>       specify secret key
     --gen-access-key                  generate random access key (for S3)
     --gen-secret                      generate random secret key
     --key-type=<type>                 key type, options are: swift, s3
     --key-active=<bool>               activate or deactivate a key
     --temp-url-key[-2]=<key>          temp url key
     --access=<access>                 Set access permissions for sub-user, should be one
                                       of read, write, readwrite, full
     --display-name=<name>             user's display name
     --max-buckets                     max number of buckets for a user
     --admin                           set the admin flag on the user
     --system                          set the system flag on the user
     --op-mask                         set the op mask on the user
     --bucket=<bucket>                 Specify the bucket name. Also used by the quota command.
     --pool=<pool>                     Specify the pool name. Also used to scan for leaked rados objects.
     --object=<object>                 object name
     --objects-file=<file>             file containing a list of object names to process
     --object-version=<version>        object version
     --date=<date>                     date in the format yyyy-mm-dd
     --start-date=<date>               start date in the format yyyy-mm-dd
     --end-date=<date>                 end date in the format yyyy-mm-dd
     --bucket-id=<bucket-id>           bucket id
     --bucket-new-name=<bucket>        for bucket link: optional new name
     --shard-id=<shard-id>             optional for:
                                         mdlog list
                                         data sync status
                                       required for:
                                         mdlog trim
     --gen=<gen-id>                    optional for:
                                         bilog list
                                         bilog trim
                                         bilog status
     --max-entries=<entries>           max entries for listing operations
     --metadata-key=<key>              key to retrieve metadata from with metadata get
     --remote=<remote>                 zone or zonegroup id of remote gateway
     --period=<id>                     period id
     --url=<url>                       url for pushing/pulling period/realm
     --epoch=<number>                  period epoch
     --commit                          commit the period during 'period update'
     --staging                         get staging period info
     --master                          set as master
     --master-zone=<id>                master zone id
     --rgw-realm=<name>                realm name
     --realm-id=<id>                   realm id
     --realm-new-name=<name>           realm new name
     --rgw-zonegroup=<name>            zonegroup name
     --zonegroup-id=<id>               zonegroup id
     --zonegroup-new-name=<name>       zonegroup new name
     --rgw-zone=<name>                 name of zone in which radosgw is running
     --zone-id=<id>                    zone id
     --zone-new-name=<name>            zone new name
     --source-zone                     specify the source zone (for data sync)
     --default                         set entity (realm, zonegroup, zone) as default
     --read-only                       set zone as read-only (when adding to zonegroup)
     --redirect-zone                   specify zone id to redirect when response is 404 (not found)
     --placement-id                    placement id for zonegroup placement commands
     --storage-class                   storage class for zonegroup placement commands
     --tags=<list>                     list of tags for zonegroup placement add and modify commands
     --tags-add=<list>                 list of tags to add for zonegroup placement modify command
     --tags-rm=<list>                  list of tags to remove for zonegroup placement modify command
     --endpoints=<list>                zone endpoints
     --index-pool=<pool>               placement target index pool
     --data-pool=<pool>                placement target data pool
     --data-extra-pool=<pool>          placement target data extra (non-ec) pool
     --placement-index-type=<type>     placement target index type (normal, indexless, or #id)
     --placement-inline-data=<true>    set whether the placement target is configured to store a data
                                       chunk inline in head objects
     --compression=<type>              placement target compression type (plugin name or empty/none)
     --tier-type=<type>                zone tier type
     --tier-config=<k>=<v>[,...]       set zone tier config keys, values
     --tier-config-rm=<k>[,...]        unset zone tier config keys
     --sync-from-all[=false]           set/reset whether zone syncs from all zonegroup peers
     --sync-from=[zone-name][,...]     set list of zones to sync from
     --sync-from-rm=[zone-name][,...]  remove zones from list of zones to sync from
     --bucket-index-max-shards         override a zone/zonegroup's default bucket index shard count
     --fix                             besides checking bucket index, will also fix it
     --check-objects                   bucket check: rebuilds bucket index according to actual objects state
     --format=<format>                 specify output format for certain operations: xml, json
     --purge-data                      when specified, user removal will also purge all the
                                       user data
     --purge-keys                      when specified, subuser removal will also purge all the
                                       subuser keys
     --purge-objects                   remove a bucket's objects before deleting it
                                       (NOTE: required to delete a non-empty bucket)
     --sync-stats                      option to 'user stats', update user stats with current
                                       stats reported by user's buckets indexes
     --reset-stats                     option to 'user stats', reset stats in accordance with user buckets
     --show-config                     show configuration
     --show-log-entries=<flag>         enable/disable dump of log entries on log show
     --show-log-sum=<flag>             enable/disable dump of log summation on log show
     --skip-zero-entries               log show only dumps entries that don't have zero value
                                       in one of the numeric field
     --infile=<file>                   file to read in when setting data
     --categories=<list>               comma separated list of categories, used in usage show
     --caps=<caps>                     list of caps (e.g., "usage=read, write; user=read")
     --op-mask=<op-mask>               permission of user's operations (e.g., "read, write, delete, *")
     --yes-i-really-mean-it            required for certain operations
     --warnings-only                   when specified with bucket limit check, list
                                       only buckets nearing or over the current max
                                       objects per shard value
     --bypass-gc                       when specified with bucket deletion, triggers
                                       object deletions by not involving GC
     --inconsistent-index              when specified with bucket deletion and bypass-gc set to true,
                                       ignores bucket index consistency
     --min-rewrite-size                min object size for bucket rewrite (default 4M)
     --max-rewrite-size                max object size for bucket rewrite (default ULLONG_MAX)
     --min-rewrite-stripe-size         min stripe size for object rewrite (default 0)
     --trim-delay-ms                   time interval in msec to limit the frequency of sync error log entries trimming operations,
                                       the trimming process will sleep the specified msec for every 1000 entries trimmed
     --max-concurrent-ios              maximum concurrent ios for bucket operations (default: 32)
     --enable-feature                  enable a zone/zonegroup feature
     --disable-feature                 disable a zone/zonegroup feature
  
  <date> := "YYYY-MM-DD[ hh:mm:ss]"
  
  Quota options:
     --max-objects                 specify max objects (negative value to disable)
     --max-size                    specify max size (in B/K/M/G/T, negative value to disable)
     --quota-scope                 scope of quota (bucket, user)
  
  Rate limiting options:
     --max-read-ops                specify max requests per minute for READ ops per RGW (GET and HEAD request methods), 0 means unlimited
     --max-read-bytes              specify max bytes per minute for READ ops per RGW (GET and HEAD request methods), 0 means unlimited
     --max-write-ops               specify max requests per minute for WRITE ops per RGW (Not GET or HEAD request methods), 0 means unlimited
     --max-write-bytes             specify max bytes per minute for WRITE ops per RGW (Not GET or HEAD request methods), 0 means unlimited
     --ratelimit-scope             scope of rate limiting: bucket, user, anonymous
                                   anonymous can be configured only with global rate limit
  
  Orphans search options:
     --num-shards                  num of shards to use for keeping the temporary scan info
     --orphan-stale-secs           num of seconds to wait before declaring an object to be an orphan (default: 86400)
     --job-id                      set the job id (for orphans find)
     --detail                      detailed mode, log and stat head objects as well
  
  Orphans list-jobs options:
     --extra-info                  provide extra info in job list
  
  Role options:
     --role-name                   name of the role to create
     --path                        path to the role
     --assume-role-policy-doc      the trust relationship policy document that grants an entity permission to assume the role
     --policy-name                 name of the policy document
     --policy-doc                  permission policy document
     --path-prefix                 path prefix for filtering roles
  
  MFA options:
     --totp-serial                 a string that represents the ID of a TOTP token
     --totp-seed                   the secret seed that is used to calculate the TOTP
     --totp-seconds                the time resolution that is being used for TOTP generation
     --totp-window                 the number of TOTP tokens that are checked before and after the current token when validating token
     --totp-pin                    the valid value of a TOTP token at a certain time
  
  Bucket notifications options:
     --topic                       bucket notifications topic name
     --notification-id             bucket notifications id
  
  Script options:
     --context                     context in which the script runs. one of: prerequest, postrequest, background, getdata, putdata
     --package                     name of the Lua package that should be added/removed to/from the allowlist
     --allow-compilation           package is allowed to compile C code as part of its installation
  
  Bucket check olh/unlinked options:
     --min-age-hours               minimum age of unlinked objects to consider for bucket check unlinked (default: 1)
     --dump-keys                   when specified, all keys identified as problematic are printed to stdout
     --hide-progress               when specified, per-shard progress details are not printed to stderr
  
  radoslist options:
     --rgw-obj-fs                  the field separator that will separate the rados object name from the rgw object name;
                                   additionally rados objects for incomplete multipart uploads will not be output
  
  Bucket list objects options:
     --max-entries                 max number of entries listed (default 1000)
     --marker                      the marker used to specify on which entry the listing begins, default none (i.e., very first entry)
  
    --conf/-c FILE    read configuration from the given configuration file
    --id ID           set ID portion of my name
    --name/-n TYPE.ID set name
    --cluster NAME    set cluster name (default: ceph)
    --setuser USER    set uid to user or uid (and gid to user's gid)
    --setgroup GROUP  set gid to group or gid
    --version         show version and quit
  
