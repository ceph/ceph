  $ crushtool -c "$TESTDIR/check-names.empty.crushmap.txt" -o "$TESTDIR/check-names.empty.crushmap"
  $ crushtool -i "$TESTDIR/check-names.empty.crushmap" --check 0
  unknown type name: item#0
  [1]
  $ rm -f "$TESTDIR/check-names.empty.crushmap"
