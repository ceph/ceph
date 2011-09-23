  $ crushtool -i "$TESTDIR/simple.template" --add-item 0 1.0 device0 --loc host host0 --loc cluster cluster0 -o one > /dev/null
  $ crushtool -i one --add-item 1 1.0 device1 --loc host host0 --loc cluster cluster0 -o two > /dev/null
  $ crushtool -d two -o final
  $ cmp final "$TESTDIR/simple.template.out"
