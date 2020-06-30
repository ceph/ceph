  $ crushtool -i "$TESTDIR/tree.template" --add-item 0 1.0 device0 --loc host host0 --loc cluster cluster0 -o one > /dev/null
  $ crushtool -i one   --add-item 1 1.0 device1 --loc host host0 --loc cluster cluster0 -o two   > /dev/null
  $ crushtool -i two   --add-item 2 1.0 device2 --loc host host0 --loc cluster cluster0 -o tree  > /dev/null
  $ crushtool -i tree  --add-item 3 1.0 device3 --loc host host0 --loc cluster cluster0 -o four  > /dev/null
  $ crushtool -i four  --add-item 4 1.0 device4 --loc host host0 --loc cluster cluster0 -o five  > /dev/null
  $ crushtool -i five  --add-item 5 1.0 device5 --loc host host0 --loc cluster cluster0 -o six   > /dev/null
  $ crushtool -i six   --add-item 6 1.0 device6 --loc host host0 --loc cluster cluster0 -o seven > /dev/null
  $ crushtool -i seven --add-item 7 1.0 device7 --loc host host0 --loc cluster cluster0 -o eight > /dev/null
  $ crushtool -d eight -o final
  $ cmp final "$TESTDIR/tree.template.final"
