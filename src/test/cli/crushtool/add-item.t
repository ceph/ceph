  $ crushtool -i "$TESTDIR/simple.template" --add-item 0 1.0 device0 --loc host host0 --loc cluster cluster0 -o one > /dev/null
  $ crushtool -i one --add-item 1 1.0 device1 --loc host host0 --loc cluster cluster0 -o two > /dev/null
  $ crushtool -d two -o final
  $ cmp final "$TESTDIR/simple.template.two"
  $ crushtool -i two --add-item 1 1.0 device1 --loc host host0 --loc cluster cluster0 -o three 2>/dev/null >/dev/null || echo FAIL
  FAIL
  $ crushtool -i two --remove-item device1 -o four > /dev/null
  $ crushtool -d four -o final
  $ cmp final "$TESTDIR/simple.template.four"
  $ crushtool -i two --update-item 1 2.0 osd1 --loc host host1 --loc cluster cluster0 -o five > /dev/null
  $ crushtool -d five -o final
  $ cmp final "$TESTDIR/simple.template.five"
  $ crushtool -i five --update-item 1 2.0 osd1 --loc host host1 --loc cluster cluster0 -o six > /dev/null
  $ crushtool -i five --show-location 1
  cluster\tcluster0 (esc)
  host\thost1 (esc)
  $ crushtool -d six -o final
  $ cmp final "$TESTDIR/simple.template.five"
