  $ rbd --help
  usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...
  where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:
    (ls | list) [-l | --long ] [pool-name] list rbd images
                                                (-l includes snapshots/clones)
    (du | disk-usage) [--image <name>] [pool-name]
                                                show pool image disk usage stats
    info <image-name>                           show information about image size,
                                                striping, etc.
    create [--order <bits>] [--image-features <features>] [--image-shared]
           --size <M/G/T> <image-name>          create an empty image
    clone [--order <bits>] [--image-features <features>] [--image-shared]
          <parentsnap> <clonename>              clone a snapshot into a COW
                                                child image
    children <snap-name>                        display children of snapshot
    flatten <image-name>                        fill clone with parent data
                                                (make it independent)
    resize --size <M/G/T> <image-name>          resize (expand or contract) image
    rm <image-name>                             delete an image
    export <image-name> <path>                  export image to file
                                                "-" for stdout
    import [--image-features <features>] [--image-shared]
           <path> <image-name>                  import image from file (dest
                                                defaults as the filename part
                                                of file). "-" for stdin
    diff [--from-snap <snap-name>] [--object-extents] <image-name>
                                                print extents that differ since
                                                a previous snap, or image creation
    export-diff [--from-snap <snap-name>] [--object-extents] <image-name> <path>
                                                export an incremental diff to
                                                path, or "-" for stdout
    merge-diff <diff1> <diff2> <path>           merge <diff1> and <diff2> into
                                                <path>, <diff1> could be "-"
                                                for stdin, and <path> could be "-"
                                                for stdout
    import-diff <path> <image-name>             import an incremental diff from
                                                path or "-" for stdin
    (cp | copy) <src> <dest>                    copy src image to dest
    (mv | rename) <src> <dest>                  rename src image to dest
    image-meta list <image-name>                image metadata list keys with values
    image-meta get <image-name> <key>           image metadata get the value associated with the key
    image-meta set <image-name> <key> <value>   image metadata set key with value
    image-meta remove <image-name> <key>        image metadata remove the key and value associated
    object-map rebuild <image-name>             rebuild an invalid object map
    snap ls <image-name>                        dump list of image snapshots
    snap create <snap-name>                     create a snapshot
    snap rollback <snap-name>                   rollback image to snapshot
    snap rm <snap-name>                         deletes a snapshot
    snap purge <image-name>                     deletes all snapshots
    snap protect <snap-name>                    prevent a snapshot from being deleted
    snap unprotect <snap-name>                  allow a snapshot to be deleted
    watch <image-name>                          watch events on image
    status <image-name>                         show the status of this image
    map <image-name>                            map image to a block device
                                                using the kernel
    unmap <image-name> | <device>               unmap a rbd device that was
                                                mapped by the kernel
    showmapped                                  show the rbd images mapped
                                                by the kernel
    feature disable <image-name> <feature>      disable the specified image feature
    feature enable <image-name> <feature>       enable the specified image feature
    lock list <image-name>                      show locks held on an image
    lock add <image-name> <id> [--shared <tag>] take a lock called id on an image
    lock remove <image-name> <id> <locker>      release a lock on an image
    bench-write <image-name>                    simple write benchmark
                   --io-size <bytes>              write size
                   --io-threads <num>             ios in flight
                   --io-total <bytes>             total bytes to write
                   --io-pattern <seq|rand>        write pattern
  
  <image-name>, <snap-name> are [pool/]name[@snap], or you may specify
  individual pieces of names with -p/--pool, --image, and/or --snap.
  
  Other input options:
    -p, --pool <pool>                  source pool name
    --image <image-name>               image name
    --dest <image-name>                destination [pool and] image name
    --snap <snap-name>                 snapshot name
    --dest-pool <name>                 destination pool name
    --path <path-name>                 path name for import/export
    -s, --size <size in M/G/T>         size of image for create and resize
    --order <bits>                     the object size in bits; object size will be
                                       (1 << order) bytes. Default is 22 (4 MB).
    --image-format <format-number>     format to use when creating an image
                                       format 1 is the original format
                                       format 2 supports cloning (default)
    --image-feature <feature>          optional format 2 feature to enable.
                                       use multiple times to enable multiple features
    --image-shared                     image will be used concurrently (disables
                                       RBD exclusive lock and dependent features)
    --stripe-unit <size-in-bytes>      size (in bytes) of a block of data
    --stripe-count <num>               number of consecutive objects in a stripe
    --id <username>                    rados user (without 'client.'prefix) to
                                       authenticate as
    --keyfile <path>                   file containing secret key for use with cephx
    --shared <tag>                     take a shared (rather than exclusive) lock
    --format <output-format>           output format (default: plain, json, xml)
    --pretty-format                    make json or xml output more readable
    --no-progress                      do not show progress for long-running commands
    -o, --options <map-options>        options to use when mapping an image
    --read-only                        set device readonly when mapping image
    --allow-shrink                     allow shrinking of an image when resizing
  
  Supported image features:
    layering (+), striping (+), exclusive-lock (*), object-map (*), fast-diff (*), deep-flatten
  
    (*) supports enabling/disabling on existing images
    (+) enabled by default for new images if features are not specified
