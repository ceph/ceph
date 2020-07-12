  $ rbd --help
  usage: rbd <command> ...
  
  Command-line interface for managing Ceph RBD images.
  
  Positional arguments:
    <command>
      bench                             Simple benchmark.
      children                          Display children of an image or its
                                        snapshot.
      clone                             Clone a snapshot into a CoW child image.
      config global get                 Get a global-level configuration override.
      config global list (... ls)       List global-level configuration overrides.
      config global remove (... rm)     Remove a global-level configuration
                                        override.
      config global set                 Set a global-level configuration override.
      config image get                  Get an image-level configuration override.
      config image list (... ls)        List image-level configuration overrides.
      config image remove (... rm)      Remove an image-level configuration
                                        override.
      config image set                  Set an image-level configuration override.
      config pool get                   Get a pool-level configuration override.
      config pool list (... ls)         List pool-level configuration overrides.
      config pool remove (... rm)       Remove a pool-level configuration
                                        override.
      config pool set                   Set a pool-level configuration override.
      copy (cp)                         Copy src image to dest.
      create                            Create an empty image.
      deep copy (deep cp)               Deep copy src image to dest.
      device list (showmapped)          List mapped rbd images.
      device map (map)                  Map an image to a block device.
      device unmap (unmap)              Unmap a rbd device.
      diff                              Print extents that differ since a
                                        previous snap, or image creation.
      disk-usage (du)                   Show disk usage stats for pool, image or
                                        snapshot.
      export                            Export image to file.
      export-diff                       Export incremental diff to file.
      feature disable                   Disable the specified image feature.
      feature enable                    Enable the specified image feature.
      flatten                           Fill clone with parent data (make it
                                        independent).
      group create                      Create a group.
      group image add                   Add an image to a group.
      group image list (... ls)         List images in a group.
      group image remove (... rm)       Remove an image from a group.
      group list (group ls)             List rbd groups.
      group remove (group rm)           Delete a group.
      group rename                      Rename a group within pool.
      group snap create                 Make a snapshot of a group.
      group snap list (... ls)          List snapshots of a group.
      group snap remove (... rm)        Remove a snapshot from a group.
      group snap rename                 Rename group's snapshot.
      group snap rollback               Rollback group to snapshot.
      image-meta get                    Image metadata get the value associated
                                        with the key.
      image-meta list (image-meta ls)   Image metadata list keys with values.
      image-meta remove (image-meta rm) Image metadata remove the key and value
                                        associated.
      image-meta set                    Image metadata set key with value.
      import                            Import image from file.
      import-diff                       Import an incremental diff.
      info                              Show information about image size,
                                        striping, etc.
      journal client disconnect         Flag image journal client as disconnected.
      journal export                    Export image journal.
      journal import                    Import image journal.
      journal info                      Show information about image journal.
      journal inspect                   Inspect image journal for structural
                                        errors.
      journal reset                     Reset image journal.
      journal status                    Show status of image journal.
      list (ls)                         List rbd images.
      lock add                          Take a lock on an image.
      lock list (lock ls)               Show locks held on an image.
      lock remove (lock rm)             Release a lock on an image.
      merge-diff                        Merge two diff exports together.
      migration abort                   Cancel interrupted image migration.
      migration commit                  Commit image migration.
      migration execute                 Execute image migration.
      migration prepare                 Prepare image migration.
      mirror image demote               Demote an image to non-primary for RBD
                                        mirroring.
      mirror image disable              Disable RBD mirroring for an image.
      mirror image enable               Enable RBD mirroring for an image.
      mirror image promote              Promote an image to primary for RBD
                                        mirroring.
      mirror image resync               Force resync to primary image for RBD
                                        mirroring.
      mirror image snapshot             Create RBD mirroring image snapshot.
      mirror image status               Show RBD mirroring status for an image.
      mirror pool demote                Demote all primary images in the pool.
      mirror pool disable               Disable RBD mirroring by default within a
                                        pool.
      mirror pool enable                Enable RBD mirroring by default within a
                                        pool.
      mirror pool info                  Show information about the pool mirroring
                                        configuration.
      mirror pool peer add              Add a mirroring peer to a pool.
      mirror pool peer bootstrap create Create a peer bootstrap token to import
                                        in a remote cluster
      mirror pool peer bootstrap import Import a peer bootstrap token created
                                        from a remote cluster
      mirror pool peer remove           Remove a mirroring peer from a pool.
      mirror pool peer set              Update mirroring peer settings.
      mirror pool promote               Promote all non-primary images in the
                                        pool.
      mirror pool status                Show status for all mirrored images in
                                        the pool.
      mirror snapshot schedule add      Add mirror snapshot schedule.
      mirror snapshot schedule list (... ls)
                                        List mirror snapshot schedule.
      mirror snapshot schedule remove (... rm)
                                        Remove mirror snapshot schedule.
      mirror snapshot schedule status   Show mirror snapshot schedule status.
      namespace create                  Create an RBD image namespace.
      namespace list (namespace ls)     List RBD image namespaces.
      namespace remove (namespace rm)   Remove an RBD image namespace.
      object-map check                  Verify the object map is correct.
      object-map rebuild                Rebuild an invalid object map.
      perf image iostat                 Display image IO statistics.
      perf image iotop                  Display a top-like IO monitor.
      pool init                         Initialize pool for use by RBD.
      pool stats                        Display pool statistics.
      remove (rm)                       Delete an image.
      rename (mv)                       Rename image within pool.
      resize                            Resize (expand or shrink) image.
      snap create (snap add)            Create a snapshot.
      snap limit clear                  Remove snapshot limit.
      snap limit set                    Limit the number of snapshots.
      snap list (snap ls)               Dump list of image snapshots.
      snap protect                      Prevent a snapshot from being deleted.
      snap purge                        Delete all unprotected snapshots.
      snap remove (snap rm)             Delete a snapshot.
      snap rename                       Rename a snapshot.
      snap rollback (snap revert)       Rollback image to snapshot.
      snap unprotect                    Allow a snapshot to be deleted.
      sparsify                          Reclaim space for zeroed image extents.
      status                            Show the status of this image.
      trash list (trash ls)             List trash images.
      trash move (trash mv)             Move an image to the trash.
      trash purge                       Remove all expired images from trash.
      trash purge schedule add          Add trash purge schedule.
      trash purge schedule list (... ls)
                                        List trash purge schedule.
      trash purge schedule remove (... rm)
                                        Remove trash purge schedule.
      trash purge schedule status       Show trash purge schedule status.
      trash remove (trash rm)           Remove an image from trash.
      trash restore                     Restore an image from trash.
      watch                             Watch events on image.
  
  Optional arguments:
    -c [ --conf ] arg                   path to cluster configuration
    --cluster arg                       cluster name
    --id arg                            client id (without 'client.' prefix)
    -n [ --name ] arg                   client name
    -m [ --mon_host ] arg               monitor host
    -K [ --keyfile ] arg                path to secret key
    -k [ --keyring ] arg                path to keyring
  
  See 'rbd help <command>' for help on a specific command.
  $ rbd help | grep '^    [a-z]' | sed 's/^    \([a-z -]*[a-z]\).*/\1/g' | while read -r line; do echo rbd help $line ; rbd help $line; done
  rbd help bench
  usage: rbd bench [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                   [--io-size <io-size>] [--io-threads <io-threads>] 
                   [--io-total <io-total>] [--io-pattern <io-pattern>] 
                   [--rw-mix-read <rw-mix-read>] --io-type <io-type> 
                   <image-spec> 
  
  Simple benchmark.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --io-size arg        IO size (in B/K/M/G/T) [default: 4K]
    --io-threads arg     ios in flight [default: 16]
    --io-total arg       total size for IO (in B/K/M/G/T) [default: 1G]
    --io-pattern arg     IO pattern (rand, seq, or full-seq) [default: seq]
    --rw-mix-read arg    read proportion in readwrite (<= 100) [default: 50]
    --io-type arg        IO type (read, write, or readwrite(rw))
  
  rbd help children
  usage: rbd children [--pool <pool>] [--namespace <namespace>] 
                      [--image <image>] [--snap <snap>] [--snap-id <snap-id>] 
                      [--all] [--descendants] [--format <format>] 
                      [--pretty-format] 
                      <image-or-snap-spec> 
  
  Display children of an image or its snapshot.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example:
                          [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --namespace arg       namespace name
    --image arg           image name
    --snap arg            snapshot name
    --snap-id arg         snapshot id
    -a [ --all ]          list all children (include trash)
    --descendants         include all descendants
    --format arg          output format (plain, json, or xml) [default: plain]
    --pretty-format       pretty formatting (json and xml)
  
  rbd help clone
  usage: rbd clone [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                   [--snap <snap>] [--dest-pool <dest-pool>] 
                   [--dest-namespace <dest-namespace>] [--dest <dest>] 
                   [--order <order>] [--object-size <object-size>] 
                   [--image-feature <image-feature>] [--image-shared] 
                   [--stripe-unit <stripe-unit>] [--stripe-count <stripe-count>] 
                   [--data-pool <data-pool>] 
                   [--mirror-image-mode <mirror-image-mode>] 
                   [--journal-splay-width <journal-splay-width>] 
                   [--journal-object-size <journal-object-size>] 
                   [--journal-pool <journal-pool>] 
                   <source-snap-spec> <dest-image-spec> 
  
  Clone a snapshot into a CoW child image.
  
  Positional arguments
    <source-snap-spec>        source snapshot specification
                              (example:
                              [<pool-name>/[<namespace>/]]<image-name>@<snap-name>
                              )
    <dest-image-spec>         destination image specification
                              (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg         source pool name
    --namespace arg           source namespace name
    --image arg               source image name
    --snap arg                source snapshot name
    --dest-pool arg           destination pool name
    --dest-namespace arg      destination namespace name
    --dest arg                destination image name
    --object-size arg         object size in B/K/M [4K <= object size <= 32M]
    --image-feature arg       image features
                              [layering(+), exclusive-lock(+*), object-map(+*),
                              deep-flatten(+-), journaling(*)]
    --image-shared            shared image
    --stripe-unit arg         stripe unit in B/K/M
    --stripe-count arg        stripe count
    --data-pool arg           data pool
    --mirror-image-mode arg   mirror image mode [journal or snapshot]
    --journal-splay-width arg number of active journal objects
    --journal-object-size arg size of journal objects [4K <= size <= 64M]
    --journal-pool arg        pool for journal objects
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (-) supports disabling-only on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help config global get
  usage: rbd config global get <config-entity> <key> 
  
  Get a global-level configuration override.
  
  Positional arguments
    <config-entity>      config entity (global, client, client.<id>)
    <key>                config key
  
  rbd help config global list
  usage: rbd config global list [--format <format>] [--pretty-format] 
                                <config-entity> 
  
  List global-level configuration overrides.
  
  Positional arguments
    <config-entity>      config entity (global, client, client.<id>)
  
  Optional arguments
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help config global remove
  usage: rbd config global remove <config-entity> <key> 
  
  Remove a global-level configuration override.
  
  Positional arguments
    <config-entity>      config entity (global, client, client.<id>)
    <key>                config key
  
  rbd help config global set
  usage: rbd config global set <config-entity> <key> <value> 
  
  Set a global-level configuration override.
  
  Positional arguments
    <config-entity>      config entity (global, client, client.<id>)
    <key>                config key
    <value>              config value
  
  rbd help config image get
  usage: rbd config image get [--pool <pool>] [--namespace <namespace>] 
                              [--image <image>] 
                              <image-spec> <key> 
  
  Get an image-level configuration override.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <key>                config key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help config image list
  usage: rbd config image list [--pool <pool>] [--namespace <namespace>] 
                               [--image <image>] [--format <format>] 
                               [--pretty-format] 
                               <image-spec> 
  
  List image-level configuration overrides.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help config image remove
  usage: rbd config image remove [--pool <pool>] [--namespace <namespace>] 
                                 [--image <image>] 
                                 <image-spec> <key> 
  
  Remove an image-level configuration override.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <key>                config key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help config image set
  usage: rbd config image set [--pool <pool>] [--namespace <namespace>] 
                              [--image <image>] 
                              <image-spec> <key> <value> 
  
  Set an image-level configuration override.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <key>                config key
    <value>              config value
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help config pool get
  usage: rbd config pool get <pool-name> <key> 
  
  Get a pool-level configuration override.
  
  Positional arguments
    <pool-name>          pool name
    <key>                config key
  
  rbd help config pool list
  usage: rbd config pool list [--format <format>] [--pretty-format] 
                              <pool-name> 
  
  List pool-level configuration overrides.
  
  Positional arguments
    <pool-name>          pool name
  
  Optional arguments
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help config pool remove
  usage: rbd config pool remove <pool-name> <key> 
  
  Remove a pool-level configuration override.
  
  Positional arguments
    <pool-name>          pool name
    <key>                config key
  
  rbd help config pool set
  usage: rbd config pool set <pool-name> <key> <value> 
  
  Set a pool-level configuration override.
  
  Positional arguments
    <pool-name>          pool name
    <key>                config key
    <value>              config value
  
  rbd help copy
  usage: rbd copy [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                  [--snap <snap>] [--dest-pool <dest-pool>] 
                  [--dest-namespace <dest-namespace>] [--dest <dest>] 
                  [--order <order>] [--object-size <object-size>] 
                  [--image-feature <image-feature>] [--image-shared] 
                  [--stripe-unit <stripe-unit>] [--stripe-count <stripe-count>] 
                  [--data-pool <data-pool>] 
                  [--mirror-image-mode <mirror-image-mode>] 
                  [--journal-splay-width <journal-splay-width>] 
                  [--journal-object-size <journal-object-size>] 
                  [--journal-pool <journal-pool>] [--sparse-size <sparse-size>] 
                  [--no-progress] 
                  <source-image-or-snap-spec> <dest-image-spec> 
  
  Copy src image to dest.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/[<namespace>/]]<image-name>[@<snap-n
                                 ame>])
    <dest-image-spec>            destination image specification
                                 (example:
                                 [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --namespace arg              source namespace name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --dest-pool arg              destination pool name
    --dest-namespace arg         destination namespace name
    --dest arg                   destination image name
    --object-size arg            object size in B/K/M [4K <= object size <= 32M]
    --image-feature arg          image features
                                 [layering(+), exclusive-lock(+*),
                                 object-map(+*), deep-flatten(+-), journaling(*)]
    --image-shared               shared image
    --stripe-unit arg            stripe unit in B/K/M
    --stripe-count arg           stripe count
    --data-pool arg              data pool
    --mirror-image-mode arg      mirror image mode [journal or snapshot]
    --journal-splay-width arg    number of active journal objects
    --journal-object-size arg    size of journal objects [4K <= size <= 64M]
    --journal-pool arg           pool for journal objects
    --sparse-size arg            sparse size in B/K/M [default: 4K]
    --no-progress                disable progress output
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (-) supports disabling-only on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help create
  usage: rbd create [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                    [--image-format <image-format>] [--new-format] 
                    [--order <order>] [--object-size <object-size>] 
                    [--image-feature <image-feature>] [--image-shared] 
                    [--stripe-unit <stripe-unit>] 
                    [--stripe-count <stripe-count>] [--data-pool <data-pool>] 
                    [--mirror-image-mode <mirror-image-mode>] 
                    [--journal-splay-width <journal-splay-width>] 
                    [--journal-object-size <journal-object-size>] 
                    [--journal-pool <journal-pool>] 
                    [--thick-provision] --size <size> [--no-progress] 
                    <image-spec> 
  
  Create an empty image.
  
  Positional arguments
    <image-spec>              image specification
                              (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg         pool name
    --namespace arg           namespace name
    --image arg               image name
    --image-format arg        image format [default: 2]
    --object-size arg         object size in B/K/M [4K <= object size <= 32M]
    --image-feature arg       image features
                              [layering(+), exclusive-lock(+*), object-map(+*),
                              deep-flatten(+-), journaling(*)]
    --image-shared            shared image
    --stripe-unit arg         stripe unit in B/K/M
    --stripe-count arg        stripe count
    --data-pool arg           data pool
    --mirror-image-mode arg   mirror image mode [journal or snapshot]
    --journal-splay-width arg number of active journal objects
    --journal-object-size arg size of journal objects [4K <= size <= 64M]
    --journal-pool arg        pool for journal objects
    --thick-provision         fully allocate storage and zero image
    -s [ --size ] arg         image size (in M/G/T) [default: M]
    --no-progress             disable progress output
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (-) supports disabling-only on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help deep copy
  usage: rbd deep copy [--pool <pool>] [--namespace <namespace>] 
                       [--image <image>] [--snap <snap>] 
                       [--dest-pool <dest-pool>] 
                       [--dest-namespace <dest-namespace>] [--dest <dest>] 
                       [--order <order>] [--object-size <object-size>] 
                       [--image-feature <image-feature>] [--image-shared] 
                       [--stripe-unit <stripe-unit>] 
                       [--stripe-count <stripe-count>] [--data-pool <data-pool>] 
                       [--mirror-image-mode <mirror-image-mode>] 
                       [--journal-splay-width <journal-splay-width>] 
                       [--journal-object-size <journal-object-size>] 
                       [--journal-pool <journal-pool>] [--flatten] 
                       [--no-progress] 
                       <source-image-or-snap-spec> <dest-image-spec> 
  
  Deep copy src image to dest.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/[<namespace>/]]<image-name>[@<snap-n
                                 ame>])
    <dest-image-spec>            destination image specification
                                 (example:
                                 [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --namespace arg              source namespace name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --dest-pool arg              destination pool name
    --dest-namespace arg         destination namespace name
    --dest arg                   destination image name
    --object-size arg            object size in B/K/M [4K <= object size <= 32M]
    --image-feature arg          image features
                                 [layering(+), exclusive-lock(+*),
                                 object-map(+*), deep-flatten(+-), journaling(*)]
    --image-shared               shared image
    --stripe-unit arg            stripe unit in B/K/M
    --stripe-count arg           stripe count
    --data-pool arg              data pool
    --mirror-image-mode arg      mirror image mode [journal or snapshot]
    --journal-splay-width arg    number of active journal objects
    --journal-object-size arg    size of journal objects [4K <= size <= 64M]
    --journal-pool arg           pool for journal objects
    --flatten                    fill clone with parent data (make it independent)
    --no-progress                disable progress output
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (-) supports disabling-only on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help device list
  usage: rbd device list [--device-type <device-type>] [--format <format>] 
                         [--pretty-format] 
  
  List mapped rbd images.
  
  Optional arguments
    -t [ --device-type ] arg device type [ggate, krbd (default), nbd]
    --format arg             output format (plain, json, or xml) [default: plain]
    --pretty-format          pretty formatting (json and xml)
  
  rbd help device map
  usage: rbd device map [--device-type <device-type>] [--pool <pool>] 
                        [--namespace <namespace>] [--image <image>] 
                        [--snap <snap>] [--read-only] [--exclusive] [--quiesce] 
                        [--quiesce-hook <quiesce-hook>] [--options <options>] 
                        <image-or-snap-spec> 
  
  Map an image to a block device.
  
  Positional arguments
    <image-or-snap-spec>     image or snapshot specification
                             (example:
                             [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>
                             ])
  
  Optional arguments
    -t [ --device-type ] arg device type [ggate, krbd (default), nbd]
    -p [ --pool ] arg        pool name
    --namespace arg          namespace name
    --image arg              image name
    --snap arg               snapshot name
    --read-only              map read-only
    --exclusive              disable automatic exclusive lock transitions
    --quiesce                use quiesce hooks
    --quiesce-hook arg       quiesce hook path
    -o [ --options ] arg     device specific options
  
  rbd help device unmap
  usage: rbd device unmap [--device-type <device-type>] [--pool <pool>] 
                          [--image <image>] [--snap <snap>] [--options <options>] 
                          <image-or-snap-or-device-spec> 
  
  Unmap a rbd device.
  
  Positional arguments
    <image-or-snap-or-device-spec>  image, snapshot, or device specification
                                    [<pool-name>/]<image-name>[@<snap-name>] or
                                    <device-path>
  
  Optional arguments
    -t [ --device-type ] arg        device type [ggate, krbd (default), nbd]
    -p [ --pool ] arg               pool name
    --image arg                     image name
    --snap arg                      snapshot name
    -o [ --options ] arg            device specific options
  
  rbd help diff
  usage: rbd diff [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                  [--snap <snap>] [--from-snap <from-snap>] [--whole-object] 
                  [--format <format>] [--pretty-format] 
                  <image-or-snap-spec> 
  
  Print extents that differ since a previous snap, or image creation.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example:
                          [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --namespace arg       namespace name
    --image arg           image name
    --snap arg            snapshot name
    --from-snap arg       snapshot starting point
    --whole-object        compare whole object
    --format arg          output format (plain, json, or xml) [default: plain]
    --pretty-format       pretty formatting (json and xml)
  
  rbd help disk-usage
  usage: rbd disk-usage [--pool <pool>] [--namespace <namespace>] 
                        [--image <image>] [--snap <snap>] [--format <format>] 
                        [--pretty-format] [--from-snap <from-snap>] [--exact] 
                        [--merge-snapshots] 
                        <image-or-snap-spec> 
  
  Show disk usage stats for pool, image or snapshot.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example:
                          [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --namespace arg       namespace name
    --image arg           image name
    --snap arg            snapshot name
    --format arg          output format (plain, json, or xml) [default: plain]
    --pretty-format       pretty formatting (json and xml)
    --from-snap arg       snapshot starting point
    --exact               compute exact disk usage (slow)
    --merge-snapshots     merge snapshot sizes with its image
  
  rbd help export
  usage: rbd export [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                    [--snap <snap>] [--path <path>] [--no-progress] 
                    [--export-format <export-format>] 
                    <source-image-or-snap-spec> <path-name> 
  
  Export image to file.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/[<namespace>/]]<image-name>[@<snap-n
                                 ame>])
    <path-name>                  export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --namespace arg              source namespace name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --path arg                   export file (or '-' for stdout)
    --no-progress                disable progress output
    --export-format arg          format of image file
  
  rbd help export-diff
  usage: rbd export-diff [--pool <pool>] [--namespace <namespace>] 
                         [--image <image>] [--snap <snap>] [--path <path>] 
                         [--from-snap <from-snap>] [--whole-object] 
                         [--no-progress] 
                         <source-image-or-snap-spec> <path-name> 
  
  Export incremental diff to file.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/[<namespace>/]]<image-name>[@<snap-n
                                 ame>])
    <path-name>                  export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --namespace arg              source namespace name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --path arg                   export file (or '-' for stdout)
    --from-snap arg              snapshot starting point
    --whole-object               compare whole object
    --no-progress                disable progress output
  
  rbd help feature disable
  usage: rbd feature disable [--pool <pool>] [--namespace <namespace>] 
                             [--image <image>] 
                             <image-spec> <features> [<features> ...]
  
  Disable the specified image feature.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <features>           image features
                         [exclusive-lock, object-map, journaling]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help feature enable
  usage: rbd feature enable [--pool <pool>] [--namespace <namespace>] 
                            [--image <image>] 
                            [--journal-splay-width <journal-splay-width>] 
                            [--journal-object-size <journal-object-size>] 
                            [--journal-pool <journal-pool>] 
                            <image-spec> <features> [<features> ...]
  
  Enable the specified image feature.
  
  Positional arguments
    <image-spec>              image specification
                              (example: [<pool-name>/[<namespace>/]]<image-name>)
    <features>                image features
                              [exclusive-lock, object-map, journaling]
  
  Optional arguments
    -p [ --pool ] arg         pool name
    --namespace arg           namespace name
    --image arg               image name
    --journal-splay-width arg number of active journal objects
    --journal-object-size arg size of journal objects [4K <= size <= 64M]
    --journal-pool arg        pool for journal objects
  
  rbd help flatten
  usage: rbd flatten [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                     [--no-progress] 
                     <image-spec> 
  
  Fill clone with parent data (make it independent).
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --no-progress        disable progress output
  
  rbd help group create
  usage: rbd group create [--pool <pool>] [--namespace <namespace>] 
                          [--group <group>] 
                          <group-spec> 
  
  Create a group.
  
  Positional arguments
    <group-spec>         group specification
                         (example: [<pool-name>/[<namespace>/]]<group-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --group arg          group name
  
  rbd help group image add
  usage: rbd group image add [--group-pool <group-pool>] 
                             [--group-namespace <group-namespace>] 
                             [--group <group>] [--image-pool <image-pool>] 
                             [--image-namespace <image-namespace>] 
                             [--image <image>] [--pool <pool>] 
                             <group-spec> <image-spec> 
  
  Add an image to a group.
  
  Positional arguments
    <group-spec>          group specification
                          (example: [<pool-name>/[<namespace>/]]<group-name>)
    <image-spec>          image specification
                          (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    --group-pool arg      group pool name
    --group-namespace arg group namespace name
    --group arg           group name
    --image-pool arg      image pool name
    --image-namespace arg image namespace name
    --image arg           image name
    -p [ --pool ] arg     pool name unless overridden
  
  rbd help group image list
  usage: rbd group image list [--format <format>] [--pretty-format] 
                              [--pool <pool>] [--namespace <namespace>] 
                              [--group <group>] 
                              <group-spec> 
  
  List images in a group.
  
  Positional arguments
    <group-spec>         group specification
                         (example: [<pool-name>/[<namespace>/]]<group-name>)
  
  Optional arguments
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --group arg          group name
  
  rbd help group image remove
  usage: rbd group image remove [--group-pool <group-pool>] 
                                [--group-namespace <group-namespace>] 
                                [--group <group>] [--image-pool <image-pool>] 
                                [--image-namespace <image-namespace>] 
                                [--image <image>] [--pool <pool>] 
                                [--image-id <image-id>] 
                                <group-spec> <image-spec> 
  
  Remove an image from a group.
  
  Positional arguments
    <group-spec>          group specification
                          (example: [<pool-name>/[<namespace>/]]<group-name>)
    <image-spec>          image specification
                          (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    --group-pool arg      group pool name
    --group-namespace arg group namespace name
    --group arg           group name
    --image-pool arg      image pool name
    --image-namespace arg image namespace name
    --image arg           image name
    -p [ --pool ] arg     pool name unless overridden
    --image-id arg        image id
  
  rbd help group list
  usage: rbd group list [--pool <pool>] [--namespace <namespace>] 
                        [--format <format>] [--pretty-format] 
                        <pool-spec> 
  
  List rbd groups.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help group remove
  usage: rbd group remove [--pool <pool>] [--namespace <namespace>] 
                          [--group <group>] 
                          <group-spec> 
  
  Delete a group.
  
  Positional arguments
    <group-spec>         group specification
                         (example: [<pool-name>/[<namespace>/]]<group-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --group arg          group name
  
  rbd help group rename
  usage: rbd group rename [--pool <pool>] [--namespace <namespace>] 
                          [--group <group>] [--dest-pool <dest-pool>] 
                          [--dest-namespace <dest-namespace>] 
                          [--dest-group <dest-group>] 
                          <source-group-spec> <dest-group-spec> 
  
  Rename a group within pool.
  
  Positional arguments
    <source-group-spec>  source group specification
                         (example: [<pool-name>/[<namespace>/]]<group-name>)
    <dest-group-spec>    destination group specification
                         (example: [<pool-name>/[<namespace>/]]<group-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --namespace arg      source namespace name
    --group arg          source group name
    --dest-pool arg      destination pool name
    --dest-namespace arg destination namespace name
    --dest-group arg     destination group name
  
  rbd help group snap create
  usage: rbd group snap create [--pool <pool>] [--namespace <namespace>] 
                               [--group <group>] [--snap <snap>] 
                               <group-snap-spec> 
  
  Make a snapshot of a group.
  
  Positional arguments
    <group-snap-spec>    group specification
                         (example:
                         [<pool-name>/[<namespace>/]]<group-name>@<snap-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --group arg          group name
    --snap arg           snapshot name
  
  rbd help group snap list
  usage: rbd group snap list [--format <format>] [--pretty-format] 
                             [--pool <pool>] [--namespace <namespace>] 
                             [--group <group>] 
                             <group-spec> 
  
  List snapshots of a group.
  
  Positional arguments
    <group-spec>         group specification
                         (example: [<pool-name>/[<namespace>/]]<group-name>)
  
  Optional arguments
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --group arg          group name
  
  rbd help group snap remove
  usage: rbd group snap remove [--pool <pool>] [--namespace <namespace>] 
                               [--group <group>] [--snap <snap>] 
                               <group-snap-spec> 
  
  Remove a snapshot from a group.
  
  Positional arguments
    <group-snap-spec>    group specification
                         (example:
                         [<pool-name>/[<namespace>/]]<group-name>@<snap-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --group arg          group name
    --snap arg           snapshot name
  
  rbd help group snap rename
  usage: rbd group snap rename [--pool <pool>] [--namespace <namespace>] 
                               [--group <group>] [--snap <snap>] 
                               [--dest-snap <dest-snap>] 
                               <group-snap-spec> <dest-snap> 
  
  Rename group's snapshot.
  
  Positional arguments
    <group-snap-spec>    group specification
                         (example:
                         [<pool-name>/[<namespace>/]]<group-name>@<snap-name>)
    <dest-snap>          destination snapshot name
                         (example: <snap-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --group arg          group name
    --snap arg           snapshot name
    --dest-snap arg      destination snapshot name
  
  rbd help group snap rollback
  usage: rbd group snap rollback [--no-progress] [--pool <pool>] 
                                 [--namespace <namespace>] [--group <group>] 
                                 [--snap <snap>] 
                                 <group-snap-spec> 
  
  Rollback group to snapshot.
  
  Positional arguments
    <group-snap-spec>    group specification
                         (example:
                         [<pool-name>/[<namespace>/]]<group-name>@<snap-name>)
  
  Optional arguments
    --no-progress        disable progress output
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --group arg          group name
    --snap arg           snapshot name
  
  rbd help image-meta get
  usage: rbd image-meta get [--pool <pool>] [--namespace <namespace>] 
                            [--image <image>] 
                            <image-spec> <key> 
  
  Image metadata get the value associated with the key.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <key>                image meta key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help image-meta list
  usage: rbd image-meta list [--pool <pool>] [--namespace <namespace>] 
                             [--image <image>] [--format <format>] 
                             [--pretty-format] 
                             <image-spec> 
  
  Image metadata list keys with values.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help image-meta remove
  usage: rbd image-meta remove [--pool <pool>] [--namespace <namespace>] 
                               [--image <image>] 
                               <image-spec> <key> 
  
  Image metadata remove the key and value associated.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <key>                image meta key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help image-meta set
  usage: rbd image-meta set [--pool <pool>] [--namespace <namespace>] 
                            [--image <image>] 
                            <image-spec> <key> <value> 
  
  Image metadata set key with value.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <key>                image meta key
    <value>              image meta value
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help import
  usage: rbd import [--path <path>] [--dest-pool <dest-pool>] 
                    [--dest-namespace <dest-namespace>] [--dest <dest>] 
                    [--image-format <image-format>] [--new-format] 
                    [--order <order>] [--object-size <object-size>] 
                    [--image-feature <image-feature>] [--image-shared] 
                    [--stripe-unit <stripe-unit>] 
                    [--stripe-count <stripe-count>] [--data-pool <data-pool>] 
                    [--mirror-image-mode <mirror-image-mode>] 
                    [--journal-splay-width <journal-splay-width>] 
                    [--journal-object-size <journal-object-size>] 
                    [--journal-pool <journal-pool>] 
                    [--sparse-size <sparse-size>] [--no-progress] 
                    [--export-format <export-format>] [--pool <pool>] 
                    [--image <image>] 
                    <path-name> <dest-image-spec> 
  
  Import image from file.
  
  Positional arguments
    <path-name>               import file (or '-' for stdin)
    <dest-image-spec>         destination image specification
                              (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    --path arg                import file (or '-' for stdin)
    --dest-pool arg           destination pool name
    --dest-namespace arg      destination namespace name
    --dest arg                destination image name
    --image-format arg        image format [default: 2]
    --object-size arg         object size in B/K/M [4K <= object size <= 32M]
    --image-feature arg       image features
                              [layering(+), exclusive-lock(+*), object-map(+*),
                              deep-flatten(+-), journaling(*)]
    --image-shared            shared image
    --stripe-unit arg         stripe unit in B/K/M
    --stripe-count arg        stripe count
    --data-pool arg           data pool
    --mirror-image-mode arg   mirror image mode [journal or snapshot]
    --journal-splay-width arg number of active journal objects
    --journal-object-size arg size of journal objects [4K <= size <= 64M]
    --journal-pool arg        pool for journal objects
    --sparse-size arg         sparse size in B/K/M [default: 4K]
    --no-progress             disable progress output
    --export-format arg       format of image file
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (-) supports disabling-only on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help import-diff
  usage: rbd import-diff [--path <path>] [--pool <pool>] 
                         [--namespace <namespace>] [--image <image>] 
                         [--sparse-size <sparse-size>] [--no-progress] 
                         <path-name> <image-spec> 
  
  Import an incremental diff.
  
  Positional arguments
    <path-name>          import file (or '-' for stdin)
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    --path arg           import file (or '-' for stdin)
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --sparse-size arg    sparse size in B/K/M [default: 4K]
    --no-progress        disable progress output
  
  rbd help info
  usage: rbd info [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                  [--snap <snap>] [--image-id <image-id>] [--format <format>] 
                  [--pretty-format] 
                  <image-or-snap-spec> 
  
  Show information about image size, striping, etc.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example:
                          [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --namespace arg       namespace name
    --image arg           image name
    --snap arg            snapshot name
    --image-id arg        image id
    --format arg          output format (plain, json, or xml) [default: plain]
    --pretty-format       pretty formatting (json and xml)
  
  rbd help journal client disconnect
  usage: rbd journal client disconnect [--pool <pool>] [--namespace <namespace>] 
                                       [--image <image>] [--journal <journal>] 
                                       [--client-id <client-id>] 
                                       <journal-spec> 
  
  Flag image journal client as disconnected.
  
  Positional arguments
    <journal-spec>       journal specification
                         (example: [<pool-name>/[<namespace>/]]<journal-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --journal arg        journal name
    --client-id arg      client ID (or leave unspecified to disconnect all)
  
  rbd help journal export
  usage: rbd journal export [--pool <pool>] [--namespace <namespace>] 
                            [--image <image>] [--journal <journal>] 
                            [--path <path>] [--verbose] [--no-error] 
                            <source-journal-spec> <path-name> 
  
  Export image journal.
  
  Positional arguments
    <source-journal-spec>  source journal specification
                           (example: [<pool-name>/[<namespace>/]]<journal-name>)
    <path-name>            export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg      source pool name
    --namespace arg        source namespace name
    --image arg            source image name
    --journal arg          source journal name
    --path arg             export file (or '-' for stdout)
    --verbose              be verbose
    --no-error             continue after error
  
  rbd help journal import
  usage: rbd journal import [--path <path>] [--dest-pool <dest-pool>] 
                            [--dest-namespace <dest-namespace>] [--dest <dest>] 
                            [--dest-journal <dest-journal>] [--verbose] 
                            [--no-error] 
                            <path-name> <dest-journal-spec> 
  
  Import image journal.
  
  Positional arguments
    <path-name>          import file (or '-' for stdin)
    <dest-journal-spec>  destination journal specification
                         (example: [<pool-name>/[<namespace>/]]<journal-name>)
  
  Optional arguments
    --path arg           import file (or '-' for stdin)
    --dest-pool arg      destination pool name
    --dest-namespace arg destination namespace name
    --dest arg           destination image name
    --dest-journal arg   destination journal name
    --verbose            be verbose
    --no-error           continue after error
  
  rbd help journal info
  usage: rbd journal info [--pool <pool>] [--namespace <namespace>] 
                          [--image <image>] [--journal <journal>] 
                          [--format <format>] [--pretty-format] 
                          <journal-spec> 
  
  Show information about image journal.
  
  Positional arguments
    <journal-spec>       journal specification
                         (example: [<pool-name>/[<namespace>/]]<journal-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --journal arg        journal name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help journal inspect
  usage: rbd journal inspect [--pool <pool>] [--namespace <namespace>] 
                             [--image <image>] [--journal <journal>] [--verbose] 
                             <journal-spec> 
  
  Inspect image journal for structural errors.
  
  Positional arguments
    <journal-spec>       journal specification
                         (example: [<pool-name>/[<namespace>/]]<journal-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --journal arg        journal name
    --verbose            be verbose
  
  rbd help journal reset
  usage: rbd journal reset [--pool <pool>] [--namespace <namespace>] 
                           [--image <image>] [--journal <journal>] 
                           <journal-spec> 
  
  Reset image journal.
  
  Positional arguments
    <journal-spec>       journal specification
                         (example: [<pool-name>/[<namespace>/]]<journal-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --journal arg        journal name
  
  rbd help journal status
  usage: rbd journal status [--pool <pool>] [--namespace <namespace>] 
                            [--image <image>] [--journal <journal>] 
                            [--format <format>] [--pretty-format] 
                            <journal-spec> 
  
  Show status of image journal.
  
  Positional arguments
    <journal-spec>       journal specification
                         (example: [<pool-name>/[<namespace>/]]<journal-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --journal arg        journal name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help list
  usage: rbd list [--long] [--pool <pool>] [--namespace <namespace>] 
                  [--format <format>] [--pretty-format] 
                  <pool-spec> 
  
  List rbd images.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -l [ --long ]        long listing format
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help lock add
  usage: rbd lock add [--pool <pool>] [--namespace <namespace>] 
                      [--image <image>] [--shared <shared>] 
                      <image-spec> <lock-id> 
  
  Take a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <lock-id>            unique lock id
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --shared arg         shared lock tag
  
  rbd help lock list
  usage: rbd lock list [--pool <pool>] [--namespace <namespace>] 
                       [--image <image>] [--format <format>] [--pretty-format] 
                       <image-spec> 
  
  Show locks held on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help lock remove
  usage: rbd lock remove [--pool <pool>] [--namespace <namespace>] 
                         [--image <image>] 
                         <image-spec> <lock-id> <locker> 
  
  Release a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <lock-id>            unique lock id
    <locker>             locker client
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help merge-diff
  usage: rbd merge-diff [--path <path>] [--no-progress] 
                        <diff1-path> <diff2-path> <path-name> 
  
  Merge two diff exports together.
  
  Positional arguments
    <diff1-path>         path to first diff (or '-' for stdin)
    <diff2-path>         path to second diff
    <path-name>          path to merged diff (or '-' for stdout)
  
  Optional arguments
    --path arg           path to merged diff (or '-' for stdout)
    --no-progress        disable progress output
  
  rbd help migration abort
  usage: rbd migration abort [--pool <pool>] [--namespace <namespace>] 
                             [--image <image>] [--no-progress] 
                             <image-spec> 
  
  Cancel interrupted image migration.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --no-progress        disable progress output
  
  rbd help migration commit
  usage: rbd migration commit [--pool <pool>] [--namespace <namespace>] 
                              [--image <image>] [--no-progress] [--force] 
                              <image-spec> 
  
  Commit image migration.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --no-progress        disable progress output
    --force              proceed even if the image has children
  
  rbd help migration execute
  usage: rbd migration execute [--pool <pool>] [--namespace <namespace>] 
                               [--image <image>] [--no-progress] 
                               <image-spec> 
  
  Execute image migration.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --no-progress        disable progress output
  
  rbd help migration prepare
  usage: rbd migration prepare [--pool <pool>] [--namespace <namespace>] 
                               [--image <image>] [--dest-pool <dest-pool>] 
                               [--dest-namespace <dest-namespace>] 
                               [--dest <dest>] [--image-format <image-format>] 
                               [--new-format] [--order <order>] 
                               [--object-size <object-size>] 
                               [--image-feature <image-feature>] 
                               [--image-shared] [--stripe-unit <stripe-unit>] 
                               [--stripe-count <stripe-count>] 
                               [--data-pool <data-pool>] 
                               [--mirror-image-mode <mirror-image-mode>] 
                               [--journal-splay-width <journal-splay-width>] 
                               [--journal-object-size <journal-object-size>] 
                               [--journal-pool <journal-pool>] [--flatten] 
                               <source-image-spec> <dest-image-spec> 
  
  Prepare image migration.
  
  Positional arguments
    <source-image-spec>       source image specification
                              (example: [<pool-name>/[<namespace>/]]<image-name>)
    <dest-image-spec>         destination image specification
                              (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg         source pool name
    --namespace arg           source namespace name
    --image arg               source image name
    --dest-pool arg           destination pool name
    --dest-namespace arg      destination namespace name
    --dest arg                destination image name
    --image-format arg        image format [default: 2]
    --object-size arg         object size in B/K/M [4K <= object size <= 32M]
    --image-feature arg       image features
                              [layering(+), exclusive-lock(+*), object-map(+*),
                              deep-flatten(+-), journaling(*)]
    --image-shared            shared image
    --stripe-unit arg         stripe unit in B/K/M
    --stripe-count arg        stripe count
    --data-pool arg           data pool
    --mirror-image-mode arg   mirror image mode [journal or snapshot]
    --journal-splay-width arg number of active journal objects
    --journal-object-size arg size of journal objects [4K <= size <= 64M]
    --journal-pool arg        pool for journal objects
    --flatten                 fill clone with parent data (make it independent)
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (-) supports disabling-only on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help mirror image demote
  usage: rbd mirror image demote [--pool <pool>] [--namespace <namespace>] 
                                 [--image <image>] 
                                 <image-spec> 
  
  Demote an image to non-primary for RBD mirroring.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help mirror image disable
  usage: rbd mirror image disable [--force] [--pool <pool>] 
                                  [--namespace <namespace>] [--image <image>] 
                                  <image-spec> 
  
  Disable RBD mirroring for an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    --force              disable even if not primary
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help mirror image enable
  usage: rbd mirror image enable [--pool <pool>] [--namespace <namespace>] 
                                 [--image <image>] 
                                 <image-spec> <mode> 
  
  Enable RBD mirroring for an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <mode>               mirror image mode (journal or snapshot) [default:
                         journal]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help mirror image promote
  usage: rbd mirror image promote [--force] [--pool <pool>] 
                                  [--namespace <namespace>] [--image <image>] 
                                  <image-spec> 
  
  Promote an image to primary for RBD mirroring.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    --force              promote even if not cleanly demoted by remote cluster
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help mirror image resync
  usage: rbd mirror image resync [--pool <pool>] [--namespace <namespace>] 
                                 [--image <image>] 
                                 <image-spec> 
  
  Force resync to primary image for RBD mirroring.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help mirror image snapshot
  usage: rbd mirror image snapshot [--pool <pool>] [--namespace <namespace>] 
                                   [--image <image>] [--skip-quiesce] 
                                   [--ignore-quiesce-error] 
                                   <image-spec> 
  
  Create RBD mirroring image snapshot.
  
  Positional arguments
    <image-spec>            image specification
                            (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg       pool name
    --namespace arg         namespace name
    --image arg             image name
    --skip-quiesce          do not run quiesce hooks
    --ignore-quiesce-error  ignore quiesce hook error
  
  rbd help mirror image status
  usage: rbd mirror image status [--pool <pool>] [--namespace <namespace>] 
                                 [--image <image>] [--format <format>] 
                                 [--pretty-format] 
                                 <image-spec> 
  
  Show RBD mirroring status for an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help mirror pool demote
  usage: rbd mirror pool demote [--pool <pool>] [--namespace <namespace>] 
                                <pool-spec> 
  
  Demote all primary images in the pool.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
  
  rbd help mirror pool disable
  usage: rbd mirror pool disable [--pool <pool>] [--namespace <namespace>] 
                                 <pool-spec> 
  
  Disable RBD mirroring by default within a pool.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
  
  rbd help mirror pool enable
  usage: rbd mirror pool enable [--pool <pool>] [--namespace <namespace>] 
                                [--site-name <site-name>] 
                                <pool-spec> <mode> 
  
  Enable RBD mirroring by default within a pool.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
    <mode>               mirror mode [image or pool]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --site-name arg      local site name
  
  rbd help mirror pool info
  usage: rbd mirror pool info [--pool <pool>] [--namespace <namespace>] 
                              [--format <format>] [--pretty-format] [--all] 
                              <pool-spec> 
  
  Show information about the pool mirroring configuration.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
    --all                list all attributes
  
  rbd help mirror pool peer add
  usage: rbd mirror pool peer add [--pool <pool>] 
                                  [--remote-client-name <remote-client-name>] 
                                  [--remote-cluster <remote-cluster>] 
                                  [--remote-mon-host <remote-mon-host>] 
                                  [--remote-key-file <remote-key-file>] 
                                  [--direction <direction>] 
                                  <pool-name> <remote-cluster-spec> 
  
  Add a mirroring peer to a pool.
  
  Positional arguments
    <pool-name>              pool name
    <remote-cluster-spec>    remote cluster spec
                             (example: [<client name>@]<cluster name>)
  
  Optional arguments
    -p [ --pool ] arg        pool name
    --remote-client-name arg remote client name
    --remote-cluster arg     remote cluster name
    --remote-mon-host arg    remote mon host(s)
    --remote-key-file arg    path to file containing remote key
    --direction arg          mirroring direction (rx-only, rx-tx)
                             [default: rx-tx]
  
  rbd help mirror pool peer bootstrap create
  usage: rbd mirror pool peer bootstrap create
                                        [--pool <pool>] [--site-name <site-name>] 
                                        <pool-name> 
  
  Create a peer bootstrap token to import in a remote cluster
  
  Positional arguments
    <pool-name>          pool name
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --site-name arg      local site name
  
  rbd help mirror pool peer bootstrap import
  usage: rbd mirror pool peer bootstrap import
                                        [--pool <pool>] 
                                        [--site-name <site-name>] 
                                        [--token-path <token-path>] 
                                        [--direction <direction>] 
                                        <pool-name> <token-path> 
  
  Import a peer bootstrap token created from a remote cluster
  
  Positional arguments
    <pool-name>          pool name
    <token-path>         bootstrap token file (or '-' for stdin)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --site-name arg      local site name
    --token-path arg     bootstrap token file (or '-' for stdin)
    --direction arg      mirroring direction (rx-only, rx-tx)
                         [default: rx-tx]
  
  rbd help mirror pool peer remove
  usage: rbd mirror pool peer remove [--pool <pool>] 
                                     <pool-name> <uuid> 
  
  Remove a mirroring peer from a pool.
  
  Positional arguments
    <pool-name>          pool name
    <uuid>               peer uuid
  
  Optional arguments
    -p [ --pool ] arg    pool name
  
  rbd help mirror pool peer set
  usage: rbd mirror pool peer set [--pool <pool>] 
                                  <pool-name> <uuid> <key> <value> 
  
  Update mirroring peer settings.
  
  Positional arguments
    <pool-name>          pool name
    <uuid>               peer uuid
    <key>                peer parameter
                         (direction, site-name, client, mon-host, key-file)
    <value>              new value for specified key
                         (rx-only, tx-only, or rx-tx for direction)
  
  Optional arguments
    -p [ --pool ] arg    pool name
  
  rbd help mirror pool promote
  usage: rbd mirror pool promote [--force] [--pool <pool>] 
                                 [--namespace <namespace>] 
                                 <pool-spec> 
  
  Promote all non-primary images in the pool.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    --force              promote even if not cleanly demoted by remote cluster
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
  
  rbd help mirror pool status
  usage: rbd mirror pool status [--pool <pool>] [--namespace <namespace>] 
                                [--format <format>] [--pretty-format] [--verbose] 
                                <pool-spec> 
  
  Show status for all mirrored images in the pool.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
    --verbose            be verbose
  
  rbd help mirror snapshot schedule add
  usage: rbd mirror snapshot schedule add
                                        [--pool <pool>] 
                                        [--namespace <namespace>] 
                                        [--image <image>] 
                                        <interval> <start-time> 
  
  Add mirror snapshot schedule.
  
  Positional arguments
    <interval>           schedule interval
    <start-time>         schedule start time
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help mirror snapshot schedule list
  usage: rbd mirror snapshot schedule list
                                        [--pool <pool>] 
                                        [--namespace <namespace>] 
                                        [--image <image>] [--recursive] 
                                        [--format <format>] [--pretty-format] 
  
  List mirror snapshot schedule.
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    -R [ --recursive ]   list all schedules
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help mirror snapshot schedule remove
  usage: rbd mirror snapshot schedule remove
                                        [--pool <pool>] 
                                        [--namespace <namespace>] 
                                        [--image <image>] 
                                        <interval> <start-time> 
  
  Remove mirror snapshot schedule.
  
  Positional arguments
    <interval>           schedule interval
    <start-time>         schedule start time
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help mirror snapshot schedule status
  usage: rbd mirror snapshot schedule status
                                        [--pool <pool>] 
                                        [--namespace <namespace>] 
                                        [--image <image>] [--format <format>] 
                                        [--pretty-format] 
  
  Show mirror snapshot schedule status.
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help namespace create
  usage: rbd namespace create [--pool <pool>] [--namespace <namespace>] 
                              <pool-spec> 
  
  Create an RBD image namespace.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
  
  rbd help namespace list
  usage: rbd namespace list [--pool <pool>] [--format <format>] [--pretty-format] 
                            <pool-name> 
  
  List RBD image namespaces.
  
  Positional arguments
    <pool-name>          pool name
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help namespace remove
  usage: rbd namespace remove [--pool <pool>] [--namespace <namespace>] 
                              <pool-spec> 
  
  Remove an RBD image namespace.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
  
  rbd help object-map check
  usage: rbd object-map check [--pool <pool>] [--namespace <namespace>] 
                              [--image <image>] [--snap <snap>] [--no-progress] 
                              <image-or-snap-spec> 
  
  Verify the object map is correct.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example:
                          [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --namespace arg       namespace name
    --image arg           image name
    --snap arg            snapshot name
    --no-progress         disable progress output
  
  rbd help object-map rebuild
  usage: rbd object-map rebuild [--pool <pool>] [--namespace <namespace>] 
                                [--image <image>] [--snap <snap>] [--no-progress] 
                                <image-or-snap-spec> 
  
  Rebuild an invalid object map.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example:
                          [<pool-name>/[<namespace>/]]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --namespace arg       namespace name
    --image arg           image name
    --snap arg            snapshot name
    --no-progress         disable progress output
  
  rbd help perf image iostat
  usage: rbd perf image iostat [--pool <pool>] [--namespace <namespace>] 
                               [--iterations <iterations>] [--sort-by <sort-by>] 
                               [--format <format>] [--pretty-format] 
                               <pool-spec> 
  
  Display image IO statistics.
  
  Positional arguments
    <pool-spec>                pool specification
                               (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg          pool name
    --namespace arg            namespace name
    --iterations arg           iterations of metric collection [> 0]
    --sort-by arg (=write_ops) sort-by IO metric (write-ops, read-ops,
                               write-bytes, read-bytes, write-latency,
                               read-latency) [default: write-ops]
    --format arg               output format (plain, json, or xml) [default:
                               plain]
    --pretty-format            pretty formatting (json and xml)
  
  rbd help perf image iotop
  usage: rbd perf image iotop [--pool <pool>] [--namespace <namespace>] 
                              <pool-spec> 
  
  Display a top-like IO monitor.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
  
  rbd help pool init
  usage: rbd pool init [--pool <pool>] [--force] 
                       <pool-name> 
  
  Initialize pool for use by RBD.
  
  Positional arguments
    <pool-name>          pool name
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --force              force initialize pool for RBD use if registered by
                         another application
  
  rbd help pool stats
  usage: rbd pool stats [--pool <pool>] [--namespace <namespace>] 
                        [--format <format>] [--pretty-format] 
                        <pool-spec> 
  
  Display pool statistics.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  Note: legacy v1 images are not included in stats
  rbd help remove
  usage: rbd remove [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                    [--no-progress] 
                    <image-spec> 
  
  Delete an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --no-progress        disable progress output
  
  rbd help rename
  usage: rbd rename [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                    [--dest-pool <dest-pool>] 
                    [--dest-namespace <dest-namespace>] [--dest <dest>] 
                    <source-image-spec> <dest-image-spec> 
  
  Rename image within pool.
  
  Positional arguments
    <source-image-spec>  source image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --namespace arg      source namespace name
    --image arg          source image name
    --dest-pool arg      destination pool name
    --dest-namespace arg destination namespace name
    --dest arg           destination image name
  
  rbd help resize
  usage: rbd resize [--pool <pool>] [--namespace <namespace>] 
                    [--image <image>] --size <size> [--allow-shrink] 
                    [--no-progress] 
                    <image-spec> 
  
  Resize (expand or shrink) image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    -s [ --size ] arg    image size (in M/G/T) [default: M]
    --allow-shrink       permit shrinking
    --no-progress        disable progress output
  
  rbd help snap create
  usage: rbd snap create [--pool <pool>] [--namespace <namespace>] 
                         [--image <image>] [--snap <snap>] [--skip-quiesce] 
                         [--ignore-quiesce-error] [--no-progress] 
                         <snap-spec> 
  
  Create a snapshot.
  
  Positional arguments
    <snap-spec>             snapshot specification
                            (example:
                            [<pool-name>/[<namespace>/]]<image-name>@<snap-name>)
  
  Optional arguments
    -p [ --pool ] arg       pool name
    --namespace arg         namespace name
    --image arg             image name
    --snap arg              snapshot name
    --skip-quiesce          do not run quiesce hooks
    --ignore-quiesce-error  ignore quiesce hook error
    --no-progress           disable progress output
  
  rbd help snap limit clear
  usage: rbd snap limit clear [--pool <pool>] [--namespace <namespace>] 
                              [--image <image>] 
                              <image-spec> 
  
  Remove snapshot limit.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
  rbd help snap limit set
  usage: rbd snap limit set [--pool <pool>] [--namespace <namespace>] 
                            [--image <image>] [--limit <limit>] 
                            <image-spec> 
  
  Limit the number of snapshots.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --limit arg          maximum allowed snapshot count
  
  rbd help snap list
  usage: rbd snap list [--pool <pool>] [--namespace <namespace>] 
                       [--image <image>] [--image-id <image-id>] 
                       [--format <format>] [--pretty-format] [--all] 
                       <image-spec> 
  
  Dump list of image snapshots.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --image-id arg       image id
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
    -a [ --all ]         list snapshots from all namespaces
  
  rbd help snap protect
  usage: rbd snap protect [--pool <pool>] [--namespace <namespace>] 
                          [--image <image>] [--snap <snap>] 
                          <snap-spec> 
  
  Prevent a snapshot from being deleted.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example:
                         [<pool-name>/[<namespace>/]]<image-name>@<snap-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --snap arg           snapshot name
  
  rbd help snap purge
  usage: rbd snap purge [--pool <pool>] [--namespace <namespace>] 
                        [--image <image>] [--image-id <image-id>] [--no-progress] 
                        <image-spec> 
  
  Delete all unprotected snapshots.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --image-id arg       image id
    --no-progress        disable progress output
  
  rbd help snap remove
  usage: rbd snap remove [--pool <pool>] [--namespace <namespace>] 
                         [--image <image>] [--snap <snap>] 
                         [--image-id <image-id>] [--snap-id <snap-id>] 
                         [--no-progress] [--force] 
                         <snap-spec> 
  
  Delete a snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example:
                         [<pool-name>/[<namespace>/]]<image-name>@<snap-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --snap arg           snapshot name
    --image-id arg       image id
    --snap-id arg        snapshot id
    --no-progress        disable progress output
    --force              flatten children and unprotect snapshot if needed.
  
  rbd help snap rename
  usage: rbd snap rename [--pool <pool>] [--namespace <namespace>] 
                         [--image <image>] [--snap <snap>] 
                         [--dest-pool <dest-pool>] 
                         [--dest-namespace <dest-namespace>] [--dest <dest>] 
                         [--dest-snap <dest-snap>] 
                         <source-snap-spec> <dest-snap-spec> 
  
  Rename a snapshot.
  
  Positional arguments
    <source-snap-spec>   source snapshot specification
                         (example:
                         [<pool-name>/[<namespace>/]]<image-name>@<snap-name>)
    <dest-snap-spec>     destination snapshot specification
                         (example:
                         [<pool-name>/[<namespace>/]]<image-name>@<snap-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --namespace arg      source namespace name
    --image arg          source image name
    --snap arg           source snapshot name
    --dest-pool arg      destination pool name
    --dest-namespace arg destination namespace name
    --dest arg           destination image name
    --dest-snap arg      destination snapshot name
  
  rbd help snap rollback
  usage: rbd snap rollback [--pool <pool>] [--namespace <namespace>] 
                           [--image <image>] [--snap <snap>] [--no-progress] 
                           <snap-spec> 
  
  Rollback image to snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example:
                         [<pool-name>/[<namespace>/]]<image-name>@<snap-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --snap arg           snapshot name
    --no-progress        disable progress output
  
  rbd help snap unprotect
  usage: rbd snap unprotect [--pool <pool>] [--namespace <namespace>] 
                            [--image <image>] [--snap <snap>] 
                            [--image-id <image-id>] 
                            <snap-spec> 
  
  Allow a snapshot to be deleted.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example:
                         [<pool-name>/[<namespace>/]]<image-name>@<snap-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --snap arg           snapshot name
    --image-id arg       image id
  
  rbd help sparsify
  usage: rbd sparsify [--pool <pool>] [--namespace <namespace>] 
                      [--image <image>] [--no-progress] 
                      [--sparse-size <sparse-size>] 
                      <image-spec> 
  
  Reclaim space for zeroed image extents.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --no-progress        disable progress output
    --sparse-size arg    sparse size in B/K/M [default: 4K]
  
  rbd help status
  usage: rbd status [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                    [--format <format>] [--pretty-format] 
                    <image-spec> 
  
  Show the status of this image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help trash list
  usage: rbd trash list [--pool <pool>] [--namespace <namespace>] [--all] 
                        [--long] [--format <format>] [--pretty-format] 
                        <pool-spec> 
  
  List trash images.
  
  Positional arguments
    <pool-spec>          pool specification
                         (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    -a [ --all ]         list images from all sources
    -l [ --long ]        long listing format
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help trash move
  usage: rbd trash move [--pool <pool>] [--namespace <namespace>] 
                        [--image <image>] [--expires-at <expires-at>] 
                        <image-spec> 
  
  Move an image to the trash.
  
  Positional arguments
    <image-spec>            image specification
                            (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg       pool name
    --namespace arg         namespace name
    --image arg             image name
    --expires-at arg (=now) set the expiration time of an image so it can be
                            purged when it is stale
  
  rbd help trash purge
  usage: rbd trash purge [--pool <pool>] [--namespace <namespace>] 
                         [--no-progress] [--expired-before <expired-before>] 
                         [--threshold <threshold>] 
                         <pool-spec> 
  
  Remove all expired images from trash.
  
  Positional arguments
    <pool-spec>           pool specification
                          (example: <pool-name>[/<namespace>]
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --namespace arg       namespace name
    --no-progress         disable progress output
    --expired-before date purges images that expired before the given date
    --threshold arg       purges images until the current pool data usage is
                          reduced to X%, value range: 0.0-1.0
  
  rbd help trash purge schedule add
  usage: rbd trash purge schedule add [--pool <pool>] [--namespace <namespace>] 
                                      <interval> <start-time> 
  
  Add trash purge schedule.
  
  Positional arguments
    <interval>           schedule interval
    <start-time>         schedule start time
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
  
  rbd help trash purge schedule list
  usage: rbd trash purge schedule list [--pool <pool>] [--namespace <namespace>] 
                                       [--recursive] [--format <format>] 
                                       [--pretty-format] 
  
  List trash purge schedule.
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    -R [ --recursive ]   list all schedules
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help trash purge schedule remove
  usage: rbd trash purge schedule remove
                                        [--pool <pool>] [--namespace <namespace>] 
                                        <interval> <start-time> 
  
  Remove trash purge schedule.
  
  Positional arguments
    <interval>           schedule interval
    <start-time>         schedule start time
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
  
  rbd help trash purge schedule status
  usage: rbd trash purge schedule status
                                        [--pool <pool>] 
                                        [--namespace <namespace>] 
                                        [--format <format>] [--pretty-format] 
  
  Show trash purge schedule status.
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --format arg         output format (plain, json, or xml) [default: plain]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help trash remove
  usage: rbd trash remove [--pool <pool>] [--namespace <namespace>] 
                          [--image-id <image-id>] [--no-progress] [--force] 
                          <image-id> 
  
  Remove an image from trash.
  
  Positional arguments
    <image-id>           image id
                         (example: [<pool-name>/[<namespace>/]]<image-id>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image-id arg       image id
    --no-progress        disable progress output
    --force              force remove of non-expired delayed images
  
  rbd help trash restore
  usage: rbd trash restore [--pool <pool>] [--namespace <namespace>] 
                           [--image-id <image-id>] [--image <image>] 
                           <image-id> 
  
  Restore an image from trash.
  
  Positional arguments
    <image-id>           image id
                         (example: [<pool-name>/]<image-id>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image-id arg       image id
    --image arg          image name
  
  rbd help watch
  usage: rbd watch [--pool <pool>] [--namespace <namespace>] [--image <image>] 
                   <image-spec> 
  
  Watch events on image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/[<namespace>/]]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --namespace arg      namespace name
    --image arg          image name
  
