  $ crushtool -i "$TESTDIR/simple.template" --add-item 0 1.0 device0 --loc host host0 --loc cluster cluster0 -o one > /dev/null

#
# add device0 into host=fake, the weight of device0 in host=host0 is 1.0, the weight of device0 in host=fake is 2.0
#

  $ crushtool -i one --add-item 0 2.0 device0 --loc host fake --loc cluster cluster0 -o two > /dev/null
  $ crushtool -d two -o final
  $ cmp final "$TESTDIR/simple.template.adj.two"

#
# update the weight of device0 in host=host0, it will not affect the weight of device0 in host=fake
#

  $ crushtool -i two --update-item 0 3.0 device0 --loc host host0 --loc cluster cluster0 -o three > /dev/null
  $ crushtool -d three -o final
  $ cmp final "$TESTDIR/simple.template.adj.three"
