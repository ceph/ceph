# TODO help should not fail
  $ ceph --help
  usage:
   ceph [options] [command]
   ceph -s     cluster status summary
   ceph -w     running cluster summary and events
  
  If no commands are specified, enter interactive mode.
  
  CLUSTER COMMANDS
    ceph health [detail]
    ceph quorum_status
    ceph df [detail]
    ceph -m <mon-ip-or-host> mon_status
  
  AUTHENTICATION (AUTH) COMMANDS
    ceph auth get-or-create[-key] <name> [capsys1 capval1 [...]]
    ceph auth del <name>
    ceph auth list
  
  METADATA SERVER (MDS) COMMANDS
    ceph mds stat
    ceph mds tell <mds-id or *> injectargs '--<switch> <value> [--<switch> <value>...]'
    ceph mds add_data_pool <pool-id>
  
  MONITOR (MON) COMMANDS
    ceph mon add <name> <ip>[:<port>]
    ceph mon remove <name>
    ceph mon stat
    ceph mon tell <mon-id or *> injectargs '--<switch> <value> [--<switch> <value>...]'
  
  OBJECT STORAGE DEVICE (OSD) COMMANDS
    ceph osd dump [--format=json]
    ceph osd ls [--format=json]
    ceph osd tree
    ceph osd map <pool-name> <object-name>
    ceph osd down <osd-id>
    ceph osd in <osd-id>
    ceph osd out <osd-id>
    ceph osd set <noout|noin|nodown|noup|noscrub|nodeep-scrub>
    ceph osd unset <noout|noin|nodown|noup|noscrub|nodeep-scrub>
    ceph osd pause
    ceph osd unpause
    ceph osd tell <osd-id or *> injectargs '--<switch> <value> [--<switch> <value>...]'
    ceph osd getcrushmap -o <file>
    ceph osd getmap -o <file>
    ceph osd crush set <osd-id> <weight> <loc1> [<loc2> ...]
    ceph osd crush add <osd-id> <weight> <loc1> [<loc2> ...]
    ceph osd crush create-or-move <osd-id> <initial-weight> <loc1> [<loc2> ...]
    ceph osd crush rm <name> [ancestor]
    ceph osd crush move <bucketname> <loc1> [<loc2> ...]
    ceph osd crush link <bucketname> <loc1> [<loc2> ...]
    ceph osd crush unlink <bucketname> [ancestor]
    ceph osd crush add-bucket <bucketname> <type>
    ceph osd crush reweight <name> <weight>
    ceph osd crush tunables <legacy|argonaut|bobtail|optimal|default>
    ceph osd create [<uuid>]
    ceph osd rm <osd-id> [<osd-id>...]
    ceph osd lost [--yes-i-really-mean-it]
    ceph osd reweight <osd-id> <weight>
    ceph osd blacklist add <address>[:source_port] [time]
    ceph osd blacklist rm <address>[:source_port]
    ceph osd pool mksnap <pool> <snapname>
    ceph osd pool rmsnap <pool> <snapname>
    ceph osd pool create <pool> <pg_num> [<pgp_num>]
    ceph osd pool delete <pool> [<pool> --yes-i-really-really-mean-it]
    ceph osd pool rename <pool> <new pool name>
    ceph osd pool set <pool> <field> <value>
    ceph osd scrub <osd-id>
    ceph osd deep-scrub <osd-id>
    ceph osd repair <osd-id>
    ceph osd tell <osd-id or *> bench [bytes per write] [total bytes]
  
  PLACEMENT GROUP (PG) COMMANDS
    ceph pg dump
    ceph pg <pg-id> query
    ceph pg scrub <pg-id>
    ceph pg deep-scrub <pg-id>
    ceph pg map <pg-id>
  
  OPTIONS
    -o <file>        Write out to <file>
    -i <file>        Read input from <file> (for some commands)
    --conf/-c        Read configuration from the given configuration file
    --id/-i          set ID portion of my name
    --name/-n        set name (TYPE.ID)
    --version        show version and quit
  
  [1]
