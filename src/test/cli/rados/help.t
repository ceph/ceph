# TODO help should not fail
  $ rados --help
  usage: rados [options] [commands]
  Commands:
     lspools                         list pools
     df                              show per-pool and total usage
  
  Pool commands:
     get <obj-name> [outfile]         fetch object
     put <obj-name> [infile]          write object
     create <obj-name>                create object
     rm <obj-name>                    remove object
     listxattr <obj-name>
     getxattr <obj-name> attr
     setxattr <obj-name> attr val
     rmxattr <obj-name> attr
     stat objname                     stat the named object
     ls                               list objects in pool
  
     chown 123                        change the pool owner to auid 123
     mapext <obj-name>
     mkpool <pool-name> [123[ 4]]     create pool <pool-name>'
                                      [with auid 123[and using crush rule 4]]
     rmpool <pool-name>               remove pool <pool-name>'
     mkpool <pool-name>               create the pool <pool-name>
     lssnap                           list snaps
     mksnap <snap-name>               create snap <snap-name>
     rmsnap <snap-name>               remove snap <snap-name>
     rollback <obj-name> <snap-name>  roll back object to snap <snap-name>
  
     import <dir>                     import pool from a directory
     export <dir>                     export pool into a directory
     bench <seconds> write|seq|rand [-t concurrent_operations]
                                      default is 16 concurrent IOs and 4 MB op size
  
  Options:
     -p pool
     --pool=pool
          select given pool by name
     -b op_size
          set the size of write ops for put or benchmarking   -s name
     --snap name
          select given snap name for (read) IO
     -i infile
     -o outfile
          specify input or output file (for certain commands)
     --create-pool
          create the pool that was specified
  [1]
