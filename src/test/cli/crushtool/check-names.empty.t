  $ crushtool -c "$TESTDIR/check-names.empty.crushmap.txt" -o "$TESTDIR/check-names.empty.crushmap"
  $ crushtool -i "$TESTDIR/check-names.empty.crushmap" --check-names
  unknown type name: item#0
  $ rm -f "$TESTDIR/check-names.empty.crushmap"
