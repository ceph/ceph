  $ rbd --help
  usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...
  where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:
    (ls | list) [pool-name]                     list rbd images
    info [--snap <name>] <image-name>           show information about image size,
                                                striping, etc.
    create [--order <bits>] --size <MB> <name>  create an empty image
    resize --size <MB> <image-name>             resize (expand or contract) image
    rm <image-name>                             delete an image
    export [--snap <name>] <image-name> <path>  export image to file
    import <path> <dst-image>                   import image from file (dest defaults
                                                as the filename part of file)
    (cp | copy) [--snap <name>] <src> <dest>    copy src image to dest
    (mv | rename) <src> <dest>                  rename src image to dest
    snap ls <image-name>                        dump list of image snapshots
    snap create --snap <name> <image-name>      create a snapshot
    snap rollback --snap <name> <image-name>    rollback image head to snapshot
    snap rm --snap <name> <image-name>          deletes a snapshot
    snap purge <image-name>                     deletes all snapshots
    watch <image-name>                          watch events on image
    map <image-name>                            map the image to a block device
                                                using the kernel
    unmap <device>                              unmap a rbd device that was
                                                mapped by the kernel
    showmapped                                  show the rbd images mapped
                                                by the kernel
  
  Other input options:
    -p, --pool <pool>            source pool name
    --image <image-name>         image name
    --dest <name>                destination [pool and] image name
    --snap <snapname>            specify snapshot name
    --dest-pool <name>           destination pool name
    --path <path-name>           path name for import/export (if not specified)
    --size <size in MB>          size parameter for create and resize commands
    --order <bits>               the object size in bits, such that the objects
                                 are (1 << order) bytes. Default is 22 (4 MB).
  
  For the map command:
    --user <username>            rados user to authenticate as
    --secret <path>              file containing secret key for use with cephx
