# TODO be user-friendly
  $ osdmaptool --help
   usage: [--print] [--createsimple <numosd> [--clobber] [--pg_bits <bitsperosd>]] <mapfilename>
     --export-crush <file>   write osdmap's crush map to <file>
     --import-crush <file>   replace osdmap's crush map with <file>
     --test-map-pgs [--pool <poolid>] [--pg_num <pg_num>] map all pgs
     --test-map-pgs-dump [--pool <poolid>] map all pgs
     --test-map-pgs-dump-all [--pool <poolid>] map all pgs to osds
     --health                dump health checks
     --mark-up-in            mark osds up and in (but do not persist)
     --with-default-pool     include default pool when creating map
     --clear-temp            clear pg_temp and primary_temp
     --test-random           do random placements
     --test-map-pg <pgid>    map a pgid to osds
     --test-map-object <objectname> [--pool <poolid>] map an object to osds
     --upmap-cleanup <file>  clean up pg_upmap[_items] entries, writing
                             commands to <file> [default: - for stdout]
     --upmap <file>          calculate pg upmap entries to balance pg layout
                             writing commands to <file> [default: - for stdout]
     --upmap-max <max-count> set max upmap entries to calculate [default: 100]
     --upmap-deviation <max-deviation>
                             max deviation from target [default: .01]
     --upmap-pool <poolname> restrict upmap balancing to 1 or more pools
     --upmap-save            write modified OSDMap with upmap changes
  [1]
