# TODO be user-friendly
  $ osdmaptool --help
   usage: [--print] <mapfilename>
     --create-from-conf      creates an osd map with default configurations
     --createsimple <numosd> [--clobber] [--pg-bits <bitsperosd>] [--pgp-bits <bits>] creates a relatively generic OSD map with <numosd> devices
     --pgp-bits <bits>       pgp_num map attribute will be shifted by <bits>
     --pg-bits <bits>        pg_num map attribute will be shifted by <bits>
     --clobber               allows osdmaptool to overwrite <mapfilename> if it already exists
     --export-crush <file>   write osdmap's crush map to <file>
     --import-crush <file>   replace osdmap's crush map with <file>
     --health                dump health checks
     --test-map-pgs [--pool <poolid>] [--pg_num <pg_num>] [--range-first <first> --range-last <last>] map all pgs
     --test-map-pgs-dump [--pool <poolid>] [--range-first <first> --range-last <last>] map all pgs
     --test-map-pgs-dump-all [--pool <poolid>] [--range-first <first> --range-last <last>] map all pgs to osds
     --mark-up-in            mark osds up and in (but do not persist)
     --mark-out <osdid>      mark an osd as out (but do not persist)
     --mark-up <osdid>       mark an osd as up (but do not persist)
     --mark-in <osdid>       mark an osd as in (but do not persist)
     --with-default-pool     include default pool when creating map
     --clear-temp            clear pg_temp and primary_temp
     --clean-temps           clean pg_temps
     --test-random           do random placements
     --test-map-pg <pgid>    map a pgid to osds
     --test-map-object <objectname> [--pool <poolid>] map an object to osds
     --upmap-cleanup <file>  clean up pg_upmap[_items] entries, writing
                             commands to <file> [default: - for stdout]
     --upmap <file>          calculate pg upmap entries to balance pg layout
                             writing commands to <file> [default: - for stdout]
     --upmap-max <max-count> set max upmap entries to calculate [default: 10]
     --upmap-deviation <max-deviation>
                             max deviation from target [default: 5]
     --upmap-pool <poolname> restrict upmap balancing to 1 or more pools
     --upmap-active          Act like an active balancer, keep applying changes until balanced
     --dump <format>         displays the map in plain text when <format> is 'plain', 'json' if specified format is not supported
     --tree                  displays a tree of the map
     --test-crush [--range-first <first> --range-last <last>] map pgs to acting osds
     --adjust-crush-weight <osdid:weight>[,<osdid:weight>,<...>] change <osdid> CRUSH <weight> (but do not persist)
     --save                  write modified osdmap with upmap or crush-adjust changes
     --read <file>           calculate pg upmap entries to balance pg primaries
     --read-pool <poolname>  specify which pool the read balancer should adjust
     --osd-size-aware        account for devices of different sizes, applicable to read mode only
     --vstart                prefix upmap and read output with './bin/'
  [1]
