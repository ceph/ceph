# TODO help should not fail
  $ crushtool --help
  usage: crushtool ...
     --decompile|-d map    decompile a crush map to source
     --compile|-c map.txt  compile a map from source
     [-o outfile [--clobber]]
                           specify output for for (de)compilation
     --build --num_osds N layer1 ...
                           build a new map, where each 'layer' is
                             'name (uniform|straw|list|tree) size'
     -i mapfn --test       test a range of inputs on the map
        [--min-x x] [--max-x x] [--x x]
        [--min-rule r] [--max-rule r] [--rule r]
        [--num-rep n]
        [--batches b]
      --simulate           simulate placements using a RNG
        [--weight|-w devno weight]
                           where weight is 0 to 1.0
     -i mapfn --add-item id weight name [--loc type name ...]
                           insert an item into the hierarchy at the
                           given location
     -i mapfn --update-item id weight name [--loc type name ...]
                           insert or move an item into the hierarchy at the
                           given location
     -i mapfn --remove-item name
                           remove the given item
     -i mapfn --reweight-item name weight
                           reweight a given item (and adjust ancestor
                           weights as needed)
     -i mapfn --reweight   recalculate all bucket weights
     --show-utilization    show OSD usage
     --show utilization-all
                           include zero weight items
     --show-statistics     show chi squared statistics
     --show-bad-mappings   show bad mappings
     --show-choose-tries   show choose tries histogram
     --set-choose-local-tries N
                           set choose local retries before re-descent
     --set-choose-local-fallback-tries N
                           set choose local retries using fallback
                           permutation before re-descent
     --set-choose-total-tries N
                           set choose total descent attempts
  [1]
