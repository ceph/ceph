  $ rbd --help
  usage: rbd <command> ...
  
  Command-line interface for managing Ceph RBD images.
  
  Positional arguments:
    <command>
      bench-write                 Simple write benchmark.
      children                    Display children of snapshot.
      clone                       Clone a snapshot into a COW child image.
      copy (cp)                   Copy src image to dest.
      create                      Create an empty image.
      diff                        Print extents that differ since a previous
                                  snap, or image creation.
      disk-usage (du)             Show disk usage stats for pool, image or
                                  snapshot
      export                      Export image to file.
      export-diff                 Export incremental diff to file.
      feature disable             Disable the specified image feature.
      feature enable              Enable the specified image feature.
      flatten                     Fill clone with parent data (make it
                                  independent).
      image-meta get              Image metadata get the value associated with
                                  the key.
      image-meta list             Image metadata list keys with values.
      image-meta remove           Image metadata remove the key and value
                                  associated.
      image-meta set              Image metadata set key with value.
      import                      Import image from file.
      import-diff                 Import an incremental diff.
      info                        Show information about image size, striping,
                                  etc.
      list (ls)                   List rbd images.
      lock add                    Take a lock on an image.
      lock list (lock ls)         Show locks held on an image.
      lock remove (lock rm)       Release a lock on an image.
      map                         Map image to a block device using the kernel.
      merge-diff                  Merge two diff exports together.
      nbd list (nbd ls)           List the nbd devices already used.
      nbd map                     Map image to a nbd device.
      nbd unmap                   Unmap a nbd device.
      object-map rebuild          Rebuild an invalid object map.
      remove (rm)                 Delete an image.
      rename (mv)                 Rename image within pool.
      resize                      Resize (expand or shrink) image.
      showmapped                  Show the rbd images mapped by the kernel.
      snap create (snap add)      Create a snapshot.
      snap list (snap ls)         Dump list of image snapshots.
      snap protect                Prevent a snapshot from being deleted.
      snap purge                  Deletes all snapshots.
      snap remove (snap rm)       Deletes a snapshot.
      snap rename                 Rename a snapshot.
      snap rollback (snap revert) Rollback image to snapshot.
      snap unprotect              Allow a snapshot to be deleted.
      status                      Show the status of this image.
      unmap                       Unmap a rbd device that was used by the kernel.
      watch                       Watch events on image.
  
  Optional arguments:
    -c [ --conf ] arg     path to cluster configuration
    --cluster arg         cluster name
    --id arg              client id (without 'client.' prefix)
    --user arg            client id (without 'client.' prefix)
    -n [ --name ] arg     client name
    -m [ --mon_host ] arg monitor host
    --secret arg          path to secret key (deprecated)
    -K [ --keyfile ] arg  path to secret key
    -k [ --keyring ] arg  path to keyring
  
  See 'rbd help <command>' for help on a specific command.
  $ rbd help | grep '^    [a-z]' | sed 's/^    \([a-z -]*[a-z]\).*/\1/g' | while read -r line; do echo rbd help $line ; rbd help $line; done
  rbd help bench-write
  usage: rbd bench-write [--pool <pool>] [--image <image>] [--io-size <io-size>] 
                         [--io-threads <io-threads>] [--io-total <io-total>] 
                         [--io-pattern <io-pattern>] 
                         <image-spec> 
  
  Simple write benchmark.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --io-size arg        write size (in B/K/M/G/T)
    --io-threads arg     ios in flight
    --io-total arg       total size to write (in B/K/M/G/T)
    --io-pattern arg     write pattern (rand or seq)
  
  rbd help children
  usage: rbd children [--pool <pool>] [--image <image>] [--snap <snap>] 
                      [--format <format>] [--pretty-format] 
                      <snap-spec> 
  
  Display children of snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help clone
  usage: rbd clone [--pool <pool>] [--image <image>] [--snap <snap>] 
                   [--dest-pool <dest-pool>] [--dest <dest>] [--order <order>] 
                   [--image-feature <image-feature>] [--image-shared] 
                   [--stripe-unit <stripe-unit>] [--stripe-count <stripe-count>] 
                   <source-snap-spec> <dest-image-spec> 
  
  Clone a snapshot into a COW child image.
  
  Positional arguments
    <source-snap-spec>   source snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --image arg          source image name
    --snap arg           source snapshot name
    --dest-pool arg      destination pool name
    --dest arg           destination image name
    --order arg          object order [12 <= order <= 25]
    --image-feature arg  image features
                         [layering(+), striping(+), exclusive-lock(*),
                         object-map(*), fast-diff(*), deep-flatten, journaling(*)]
    --image-shared       shared image
    --stripe-unit arg    stripe unit
    --stripe-count arg   stripe count
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help copy
  usage: rbd copy [--pool <pool>] [--image <image>] [--snap <snap>] 
                  [--dest-pool <dest-pool>] [--dest <dest>] [--no-progress] 
                  <source-image-or-snap-spec> <dest-image-spec> 
  
  Copy src image to dest.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <dest-image-spec>            destination image specification
                                 (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --dest-pool arg              destination pool name
    --dest arg                   destination image name
    --no-progress                disable progress output
  
  rbd help create
  usage: rbd create [--pool <pool>] [--image <image>] 
                    [--image-format <image-format>] [--new-format] 
                    [--order <order>] [--image-feature <image-feature>] 
                    [--image-shared] [--stripe-unit <stripe-unit>] 
                    [--stripe-count <stripe-count>] --size <size> 
                    <image-spec> 
  
  Create an empty image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --image-format arg   image format [1 or 2]
    --new-format         use image format 2
                         (deprecated)
    --order arg          object order [12 <= order <= 25]
    --image-feature arg  image features
                         [layering(+), striping(+), exclusive-lock(*),
                         object-map(*), fast-diff(*), deep-flatten, journaling(*)]
    --image-shared       shared image
    --stripe-unit arg    stripe unit
    --stripe-count arg   stripe count
    -s [ --size ] arg    image size (in M/G/T)
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help diff
  usage: rbd diff [--pool <pool>] [--image <image>] [--snap <snap>] 
                  [--from-snap <from-snap>] [--whole-object] [--format <format>] 
                  [--pretty-format] 
                  <image-or-snap-spec> 
  
  Print extents that differ since a previous snap, or image creation.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    --from-snap arg       snapshot starting point
    --whole-object        compare whole object
    --format arg          output format [plain, json, or xml]
    --pretty-format       pretty formatting (json and xml)
  
  rbd help disk-usage
  usage: rbd disk-usage [--pool <pool>] [--image <image>] [--snap <snap>] 
                        [--format <format>] [--pretty-format] 
                        <image-or-snap-spec> 
  
  Show disk usage stats for pool, image or snapshot
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    --format arg          output format [plain, json, or xml]
    --pretty-format       pretty formatting (json and xml)
  
  rbd help export
  usage: rbd export [--pool <pool>] [--image <image>] [--snap <snap>] 
                    [--path <path>] [--no-progress] 
                    <source-image-or-snap-spec> <path-name> 
  
  Export image to file.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <path-name>                  export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --path arg                   export file (or '-' for stdout)
    --no-progress                disable progress output
  
  rbd help export-diff
  usage: rbd export-diff [--pool <pool>] [--image <image>] [--snap <snap>] 
                         [--path <path>] [--from-snap <from-snap>] 
                         [--whole-object] [--no-progress] 
                         <source-image-or-snap-spec> <path-name> 
  
  Export incremental diff to file.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <path-name>                  export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --path arg                   export file (or '-' for stdout)
    --from-snap arg              snapshot starting point
    --whole-object               compare whole object
    --no-progress                disable progress output
  
  rbd help feature disable
  usage: rbd feature disable [--pool <pool>] [--image <image>] 
                             <image-spec> <features> [<features> ...]
  
  Disable the specified image feature.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <features>           image features
                         [layering, striping, exclusive-lock, object-map,
                         fast-diff, deep-flatten, journaling]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  rbd help feature enable
  usage: rbd feature enable [--pool <pool>] [--image <image>] 
                            <image-spec> <features> [<features> ...]
  
  Enable the specified image feature.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <features>           image features
                         [layering, striping, exclusive-lock, object-map,
                         fast-diff, deep-flatten, journaling]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  rbd help flatten
  usage: rbd flatten [--pool <pool>] [--image <image>] [--no-progress] 
                     <image-spec> 
  
  Fill clone with parent data (make it independent).
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --no-progress        disable progress output
  
  rbd help image-meta get
  usage: rbd image-meta get [--pool <pool>] [--image <image>] 
                            <image-spec> <key> 
  
  Image metadata get the value associated with the key.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  rbd help image-meta list
  usage: rbd image-meta list [--pool <pool>] [--image <image>] 
                             [--format <format>] [--pretty-format] 
                             <image-spec> 
  
  Image metadata list keys with values.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help image-meta remove
  usage: rbd image-meta remove [--pool <pool>] [--image <image>] 
                               <image-spec> <key> 
  
  Image metadata remove the key and value associated.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  rbd help image-meta set
  usage: rbd image-meta set [--pool <pool>] [--image <image>] 
                            <image-spec> <key> <value> 
  
  Image metadata set key with value.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
    <value>              image meta value
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  rbd help import
  usage: rbd import [--path <path>] [--dest-pool <dest-pool>] [--dest <dest>] 
                    [--image-format <image-format>] [--new-format] 
                    [--order <order>] [--image-feature <image-feature>] 
                    [--image-shared] [--stripe-unit <stripe-unit>] 
                    [--stripe-count <stripe-count>] [--no-progress] 
                    [--pool <pool>] [--image <image>] 
                    <path-name> <dest-image-spec> 
  
  Import image from file.
  
  Positional arguments
    <path-name>          import file (or '-' for stdin)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    --path arg           import file (or '-' for stdin)
    --dest-pool arg      destination pool name
    --dest arg           destination image name
    --image-format arg   image format [1 or 2]
    --new-format         use image format 2
                         (deprecated)
    --order arg          object order [12 <= order <= 25]
    --image-feature arg  image features
                         [layering(+), striping(+), exclusive-lock(*),
                         object-map(*), fast-diff(*), deep-flatten, journaling(*)]
    --image-shared       shared image
    --stripe-unit arg    stripe unit
    --stripe-count arg   stripe count
    --no-progress        disable progress output
    -p [ --pool ] arg    pool name (deprecated)
    --image arg          image name (deprecated)
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (+) enabled by default for new images if features not specified
  
  rbd help import-diff
  usage: rbd import-diff [--path <path>] [--pool <pool>] [--image <image>] 
                         [--no-progress] 
                         <path-name> <image-spec> 
  
  Import an incremental diff.
  
  Positional arguments
    <path-name>          import file (or '-' for stdin)
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    --path arg           import file (or '-' for stdin)
    -p [ --pool ] arg    pool name
    --image arg          image name
    --no-progress        disable progress output
  
  rbd help info
  usage: rbd info [--pool <pool>] [--image <image>] [--snap <snap>] 
                  [--format <format>] [--pretty-format] 
                  <image-or-snap-spec> 
  
  Show information about image size, striping, etc.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    --format arg          output format [plain, json, or xml]
    --pretty-format       pretty formatting (json and xml)
  
  rbd help list
  usage: rbd list [--long] [--pool <pool>] [--format <format>] [--pretty-format] 
                  <pool-name> 
  
  List rbd images.
  
  Positional arguments
    <pool-name>          pool name
  
  Optional arguments
    -l [ --long ]        long listing format
    -p [ --pool ] arg    pool name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help lock add
  usage: rbd lock add [--pool <pool>] [--image <image>] [--shared <shared>] 
                      <image-spec> <lock-id> 
  
  Take a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <lock-id>            unique lock id
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --shared arg         shared lock tag
  
  rbd help lock list
  usage: rbd lock list [--pool <pool>] [--image <image>] [--format <format>] 
                       [--pretty-format] 
                       <image-spec> 
  
  Show locks held on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help lock remove
  usage: rbd lock remove [--pool <pool>] [--image <image>] 
                         <image-spec> <lock-id> <locker> 
  
  Release a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <lock-id>            unique lock id
    <locker>             locker client
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  rbd help map
  usage: rbd map [--pool <pool>] [--image <image>] [--snap <snap>] 
                 [--options <options>] [--read-only] 
                 <image-or-snap-spec> 
  
  Map image to a block device using the kernel.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    -o [ --options ] arg  mapping options
    --read-only           mount read-only
  
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
  
  rbd help nbd list
  usage: rbd nbd list 
  
  List the nbd devices already used.
  
  rbd help nbd map
  usage: rbd nbd map [--pool <pool>] [--image <image>] [--snap <snap>] 
                     [--read-only] [--device <device>] 
                     <image-or-snap-spec> 
  
  Map image to a nbd device.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    --read-only           mount read-only
    --device arg          specify nbd device
  
  rbd help nbd unmap
  usage: rbd nbd unmap 
                       <device-spec> 
  
  Unmap a nbd device.
  
  Positional arguments
    <device-spec>        specify nbd device
  
  rbd help object-map rebuild
  usage: rbd object-map rebuild [--pool <pool>] [--image <image>] 
                                [--snap <snap>] [--no-progress] 
                                <image-or-snap-spec> 
  
  Rebuild an invalid object map.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    --no-progress         disable progress output
  
  rbd help remove
  usage: rbd remove [--pool <pool>] [--image <image>] [--no-progress] 
                    <image-spec> 
  
  Delete an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --no-progress        disable progress output
  
  rbd help rename
  usage: rbd rename [--pool <pool>] [--image <image>] [--dest-pool <dest-pool>] 
                    [--dest <dest>] 
                    <source-image-spec> <dest-image-spec> 
  
  Rename image within pool.
  
  Positional arguments
    <source-image-spec>  source image specification
                         (example: [<pool-name>/]<image-name>)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --image arg          source image name
    --dest-pool arg      destination pool name
    --dest arg           destination image name
  
  rbd help resize
  usage: rbd resize [--pool <pool>] [--image <image>] --size <size> 
                    [--allow-shrink] [--no-progress] 
                    <image-spec> 
  
  Resize (expand or shrink) image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    -s [ --size ] arg    image size (in M/G/T)
    --allow-shrink       permit shrinking
    --no-progress        disable progress output
  
  rbd help showmapped
  usage: rbd showmapped [--format <format>] [--pretty-format] 
  
  Show the rbd images mapped by the kernel.
  
  Optional arguments
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help snap create
  usage: rbd snap create [--pool <pool>] [--image <image>] [--snap <snap>] 
                         <snap-spec> 
  
  Create a snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  rbd help snap list
  usage: rbd snap list [--pool <pool>] [--image <image>] [--format <format>] 
                       [--pretty-format] 
                       <image-spec> 
  
  Dump list of image snapshots.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help snap protect
  usage: rbd snap protect [--pool <pool>] [--image <image>] [--snap <snap>] 
                          <snap-spec> 
  
  Prevent a snapshot from being deleted.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  rbd help snap purge
  usage: rbd snap purge [--pool <pool>] [--image <image>] [--no-progress] 
                        <image-spec> 
  
  Deletes all snapshots.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --no-progress        disable progress output
  
  rbd help snap remove
  usage: rbd snap remove [--pool <pool>] [--image <image>] [--snap <snap>] 
                         <snap-spec> 
  
  Deletes a snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  rbd help snap rename
  usage: rbd snap rename [--pool <pool>] [--image <image>] [--snap <snap>] 
                         [--dest-pool <dest-pool>] [--dest <dest>] 
                         <source-snap-spec> <dest-snap-spec> 
  
  Rename a snapshot.
  
  Positional arguments
    <source-snap-spec>   source snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
    <dest-snap-spec>     destination snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --image arg          source image name
    --snap arg           source snapshot name
    --dest-pool arg      destination pool name
    --dest arg           destination image name
  
  rbd help snap rollback
  usage: rbd snap rollback [--pool <pool>] [--image <image>] [--snap <snap>] 
                           [--no-progress] 
                           <snap-spec> 
  
  Rollback image to snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
    --no-progress        disable progress output
  
  rbd help snap unprotect
  usage: rbd snap unprotect [--pool <pool>] [--image <image>] [--snap <snap>] 
                            <snap-spec> 
  
  Allow a snapshot to be deleted.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  rbd help status
  usage: rbd status [--pool <pool>] [--image <image>] [--format <format>] 
                    [--pretty-format] 
                    <image-spec> 
  
  Show the status of this image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  rbd help unmap
  usage: rbd unmap [--pool <pool>] [--image <image>] [--snap <snap>] 
                   <image-or-snap-or-device-spec> 
  
  Unmap a rbd device that was used by the kernel.
  
  Positional arguments
    <image-or-snap-or-device-spec>  image, snapshot, or device specification
                                    [<pool-name>/]<image-name>[@<snapshot-name>]
                                    or <device-path>
  
  Optional arguments
    -p [ --pool ] arg               pool name
    --image arg                     image name
    --snap arg                      snapshot name
  
  rbd help watch
  usage: rbd watch [--pool <pool>] [--image <image>] 
                   <image-spec> 
  
  Watch events on image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
