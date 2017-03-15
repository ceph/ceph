  $ osdmaptool
  osdmaptool: must specify osdmap filename
   usage: [--print] [--createsimple <numosd> [--clobber] [--pg_bits <bitsperosd>]] <mapfilename>
     --export-crush <file>   write osdmap's crush map to <file>
     --import-crush <file>   replace osdmap's crush map with <file>
     --test-map-pgs [--pool <poolid>] [--pg_num <pg_num>] map all pgs
     --test-map-pgs-dump [--pool <poolid>] map all pgs
     --test-map-pgs-dump-all [--pool <poolid>] map all pgs to osds
     --mark-up-in            mark osds up and in (but do not persist)
     --clear-temp            clear pg_temp and primary_temp
     --test-random           do random placements
     --test-map-pg <pgid>    map a pgid to osds
     --test-map-object <objectname> [--pool <poolid>] map an object to osds
     --remap-cleanup <file>  clean up pg_remap[_items] entries, writing
                             commands to <file> [default: - for stdout]
     --remap <file>          calculate pg remap entries to balance pg layout
                             writing commands to <file> [default: - for stdout]
     --remap-max <max-count> set max remap entries to calculate [default: 100]
     --remap-deviation <max-deviation>
                             max deviation from target [default: .01]
     --remap-pool <poolname> restrict remap balancing to 1 or more pools
  [1]
