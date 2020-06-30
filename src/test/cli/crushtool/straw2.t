  $ crushtool -c $TESTDIR/straw2.txt -o straw2
  $ crushtool -d straw2 -o straw2.txt.new
  $ diff -b $TESTDIR/straw2.txt straw2.txt.new
  $ rm straw2 straw2.txt.new
