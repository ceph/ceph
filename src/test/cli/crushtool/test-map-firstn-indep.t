  $ crushtool -c "$TESTDIR/test-map-firstn-indep.txt" -o "$TESTDIR/test-map-firstn-indep.crushmap"
  $ crushtool -i "$TESTDIR/test-map-firstn-indep.crushmap" --test --rule 0 --x 1 --show-bad-mappings
  bad mapping rule 0 x 1 num_rep 9 result [93,80,88,87,56,50,53,72]
  bad mapping rule 0 x 1 num_rep 10 result [93,80,88,87,56,50,53,72]
  $ crushtool -i "$TESTDIR/test-map-firstn-indep.crushmap" --test --rule 1 --x 1 --show-bad-mappings
  bad mapping rule 1 x 1 num_rep 3 result [93,56]
  bad mapping rule 1 x 1 num_rep 4 result [93,56]
  bad mapping rule 1 x 1 num_rep 5 result [93,56]
  bad mapping rule 1 x 1 num_rep 6 result [93,56]
  bad mapping rule 1 x 1 num_rep 7 result [93,56]
  bad mapping rule 1 x 1 num_rep 8 result [93,56]
  bad mapping rule 1 x 1 num_rep 9 result [93,56]
  bad mapping rule 1 x 1 num_rep 10 result [93,56]
  $ rm -f "$TESTDIR/test-map-firstn-indep.crushmap"
