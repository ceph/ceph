# TODO help should not fail
  $ crushtool --help
  usage: crushtool ...
     --decompile|-d map    decompile a crush map to source
     --compile|-c map.txt  compile a map from source
     [-o outfile [--clobber]]
                           specify output for for (de)compilation
     --build --num_osd N layer1 ...
                           build a new map, where each 'layer' is
                             'name (uniform|straw|list|tree) size'
     --test mapfn          test a range of inputs on the map
        [--min-x x] [--max-x x] [--x x]
        [--min-rule r] [--max-rule r] [--rule r]
        [--num-rep n]
        [--weight|-w devno weight]
                           where weight is 0 to 1.0
  [1]
