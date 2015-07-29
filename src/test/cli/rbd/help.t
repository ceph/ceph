  $ rbd --help
  usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...
  where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:
    (ls | list) [-l | --long ] [pool-name]      list rbd images
                                                (-l includes snapshots/clones)
    (du | disk-usage) [<image-spec> | <snap-spec>]
                                                show disk usage stats for pool,
                                                image or snapshot
    info <image-spec> | <snap-spec>             show information about image size,
                                                striping, etc.
    create [--order <bits>] [--image-features <features>] [--image-shared]
           --size <M/G/T> <image-spec>          create an empty image
    clone [--order <bits>] [--image-features <features>] [--image-shared]
           <parent-snap-spec> <child-image-spec>
                                                clone a snapshot into a COW
                                                child image
    children <snap-spec>                        display children of snapshot
    flatten <image-spec>                        fill clone with parent data
                                                (make it independent)
    resize --size <M/G/T> <image-spec>          resize (expand or contract) image
    rm <image-spec>                             delete an image
    export (<image-spec> | <snap-spec>) [<path>]
                                                export image to file
                                                "-" for stdout
    import [--image-features <features>] [--image-shared]
           <path> [<image-spec>]                import image from file
                                                "-" for stdin
                                                "rbd/$(basename <path>)" is
                                                assumed for <image-spec> if
                                                omitted
    diff [--from-snap <snap-name>] [--whole-object]
           <image-spec> | <snap-spec>           print extents that differ since
                                                a previous snap, or image creation
    export-diff [--from-snap <snap-name>] [--whole-object]
           (<image-spec> | <snap-spec>) <path>  export an incremental diff to
                                                path, or "-" for stdout
    merge-diff <diff1> <diff2> <path>           merge <diff1> and <diff2> into
                                                <path>, <diff1> could be "-"
                                                for stdin, and <path> could be "-"
                                                for stdout
    import-diff <path> <image-spec>             import an incremental diff from
                                                path or "-" for stdin
    (cp | copy) (<src-image-spec> | <src-snap-spec>) <dest-image-spec>
                                                copy src image to dest
    (mv | rename) <src-image-spec> <dest-image-spec>
                                                rename src image to dest
    image-meta list <image-spec>                image metadata list keys with values
    image-meta get <image-spec> <key>           image metadata get the value associated with the key
    image-meta set <image-spec> <key> <value>   image metadata set key with value
    image-meta remove <image-spec> <key>        image metadata remove the key and value associated
    object-map rebuild <image-spec> | <snap-spec>
                                                rebuild an invalid object map
    snap ls <image-spec>                        dump list of image snapshots
    snap create <snap-spec>                     create a snapshot
    snap rollback <snap-spec>                   rollback image to snapshot
    snap rm <snap-spec>                         deletes a snapshot
    snap purge <image-spec>                     deletes all snapshots
    snap protect <snap-spec>                    prevent a snapshot from being deleted
    snap unprotect <snap-spec>                  allow a snapshot to be deleted
    watch <image-spec>                          watch events on image
    status <image-spec>                         show the status of this image
    map <image-spec> | <snap-spec>              map image to a block device
                                                using the kernel
    unmap <image-spec> | <snap-spec> | <device> unmap a rbd device that was
                                                mapped by the kernel
    showmapped                                  show the rbd images mapped
                                                by the kernel
    feature disable <image-spec> <feature>      disable the specified image feature
    feature enable <image-spec> <feature>       enable the specified image feature
    lock list <image-spec>                      show locks held on an image
    lock add <image-spec> <id> [--shared <tag>] take a lock called id on an image
    lock remove <image-spec> <id> <locker>      release a lock on an image
    bench-write <image-spec>                    simple write benchmark
                   --io-size <bytes>              write size
                   --io-threads <num>             ios in flight
                   --io-total <bytes>             total bytes to write
                   --io-pattern <seq|rand>        write pattern
  
  <image-spec> is [<pool-name>]/<image-name>,
  <snap-spec> is [<pool-name>]/<image-name>@<snap-name>,
  or you may specify individual pieces of names with -p/--pool <pool-name>,
  --image <image-name> and/or --snap <snap-name>.
  
  Other input options:
    -p, --pool <pool-name>             source pool name
    --dest-pool <pool-name>            destination pool name
    --image <image-name>               image name
    --dest <image-name>                destination image name
    --snap <snap-name>                 snapshot name
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
    --keyring <path>                   file containing keyring for use with cephx
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
