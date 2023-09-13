  $ crushtool -c "$TESTDIR/bad-mappings.crushmap.txt" -o "$TESTDIR/bad-mappings.crushmap"
  $ crushtool -i "$TESTDIR/bad-mappings.crushmap" --test --show-bad-mappings --rule 0 --x 1 --num-rep 10
  bad mapping rule 0 x 1 num_rep 10 result [4,0,2,3,1]
  $ crushtool -i "$TESTDIR/bad-mappings.crushmap" --test --show-bad-mappings --rule 1 --x 1 --num-rep 10
  bad mapping rule 1 x 1 num_rep 10 result [4,0,2,1,3,2147483647,2147483647,2147483647,2147483647,2147483647]
  $ rm -f "$TESTDIR/bad-mappings.crushmap"
