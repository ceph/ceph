  $ osdmaptool
  osdmaptool: must specify osdmap filename
   usage: [--print] [--createsimple <numosd> [--clobber] [--pg_bits <bitsperosd>]] <mapfilename>
     --export-crush <file>   write osdmap's crush map to <file>
     --import-crush <file>   replace osdmap's crush map with <file>
     --test-map-pgs [--pool <poolid>] map all pgs
     --test-map-pgs-dump [--pool <poolid>] map all pgs
     --mark-up-in            mark osds up and in (but do not persist)
     --clear-temp            clear pg_temp and primary_temp
     --test-random           do random placements
     --test-map-pg <pgid>    map a pgid to osds
     --test-map-object <objectname> [--pool <poolid>] map an object to osds
  [1]
