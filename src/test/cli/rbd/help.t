  $ rbd --help
  usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...
  where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:
    (ls | list) [-l | --long ] [pool-name] list rbd images
                                                (-l includes snapshots/clones)
    info <image-name>                           show information about image size,
                                                striping, etc.
    create [--order <bits>] --size <MB> <name>  create an empty image
    clone [--order <bits>] <parentsnap> <clonename>
                                                clone a snapshot into a COW
                                                child image
    children <snap-name>                        display children of snapshot
    flatten <image-name>                        fill clone with parent data
                                                (make it independent)
    resize --size <MB> <image-name>             resize (expand or contract) image
    rm <image-name>                             delete an image
    export <image-name> <path>                  export image to file
                                                "-" for stdout
    import <path> <image-name>                  import image from file
                                                (dest defaults
                                                 as the filename part of file)
                                                "-" for stdin
    diff <image-name> [--from-snap <snap-name>] print extents that differ since
                                                a previous snap, or image creation
    export-diff <image-name> [--from-snap <snap-name>] <path>
                                                export an incremental diff to
                                                path, or "-" for stdout
    import-diff <path> <image-name>             import an incremental diff from
                                                path or "-" for stdin
    (cp | copy) <src> <dest>                    copy src image to dest
    (mv | rename) <src> <dest>                  rename src image to dest
    snap ls <image-name>                        dump list of image snapshots
    snap create <snap-name>                     create a snapshot
    snap rollback <snap-name>                   rollback image to snapshot
    snap rm <snap-name>                         deletes a snapshot
    snap purge <image-name>                     deletes all snapshots
    snap protect <snap-name>                    prevent a snapshot from being deleted
    snap unprotect <snap-name>                  allow a snapshot to be deleted
    watch <image-name>                          watch events on image
    map <image-name>                            map image to a block device
                                                using the kernel
    unmap <device>                              unmap a rbd device that was
                                                mapped by the kernel
    showmapped                                  show the rbd images mapped
                                                by the kernel
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
    --size <size in MB>                size of image for create and resize
    --order <bits>                     the object size in bits; object size will be
                                       (1 << order) bytes. Default is 22 (4 MB).
    --image-format <format-number>     format to use when creating an image
                                       format 1 is the original format (default)
                                       format 2 supports cloning
    --id <username>                    rados user (without 'client.'prefix) to
                                       authenticate as
    --keyfile <path>                   file containing secret key for use with cephx
    --shared <tag>                     take a shared (rather than exclusive) lock
    --format <output-format>           output format (default: plain, json, xml)
    --pretty-format                    make json or xml output more readable
    --no-settle                        do not wait for udevadm to settle on map/unmap
    --no-progress                      do not show progress for long-running commands
