  $ crushtool -c "$TESTDIR/multitype.before" -o mt > /dev/null
  $ crushtool -i mt --reweight-item osd0 2.0 -o mt > /dev/null
  $ crushtool -i mt --reweight-item osd3 2.0 -o mt > /dev/null
  $ crushtool -i mt --reweight-item osd6 2.0 -o mt > /dev/null
  $ crushtool -i mt --reweight-item osd7 .5 -o mt > /dev/null
  $ crushtool -d mt -o final
  $ diff final "$TESTDIR/multitype.after"
  $ rm mt final
