  $ cp "$TESTDIR/need_tree_order.crush" .
  $ crushtool -c need_tree_order.crush -o nto.compiled
  $ crushtool -d nto.compiled -o nto.conf
  $ crushtool -c nto.conf -o nto.recompiled

# as the input file is actually exactly what decompilation will spit
# back out, comments and all, and the compiled format is completely
# deterministic, we can compare the files to make sure everything
# worked
  $ cmp need_tree_order.crush nto.conf
  $ cmp nto.compiled nto.recompiled

  $ crushtool -c "$TESTDIR/missing-bucket.crushmap.txt"
  in rule 'rule-bad' item 'root-404' not defined
  [1]
