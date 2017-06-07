  $ crushtool -i "$TESTDIR/simple.template" --add-item 0 1.0 device0 --loc host host0 --loc cluster cluster0 -o one > /dev/null
  $ crushtool -i one --add-item 1 1.0 device1 --loc host host0 --loc cluster cluster0 -o two > /dev/null
  $ crushtool -i two --create-simple-rule simple-rule cluster0 host firstn -o two > /dev/null
  $ crushtool -d two
  # begin crush map
  
  # devices
  device 0 device0
  device 1 device1
  
  # types
  type 0 device
  type 1 host
  type 2 cluster
  
  # buckets
  host host0 {
  \tid -2\t\t# do not change unnecessarily (esc)
  \t# weight 2.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem device0 weight 1.000 (esc)
  \titem device1 weight 1.000 (esc)
  }
  cluster cluster0 {
  \tid -1\t\t# do not change unnecessarily (esc)
  \t# weight 2.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem host0 weight 2.000 (esc)
  }
  
  # rules
  rule data {
  \truleset 0 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule metadata {
  \truleset 1 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule rbd {
  \truleset 2 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule simple-rule {
  \truleset 3 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  
  # end crush map
  $ crushtool -i two --remove-rule simple-rule -o two > /dev/null
  $ crushtool -d two
  # begin crush map
  
  # devices
  device 0 device0
  device 1 device1
  
  # types
  type 0 device
  type 1 host
  type 2 cluster
  
  # buckets
  host host0 {
  \tid -2\t\t# do not change unnecessarily (esc)
  \t# weight 2.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem device0 weight 1.000 (esc)
  \titem device1 weight 1.000 (esc)
  }
  cluster cluster0 {
  \tid -1\t\t# do not change unnecessarily (esc)
  \t# weight 2.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem host0 weight 2.000 (esc)
  }
  
  # rules
  rule data {
  \truleset 0 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule metadata {
  \truleset 1 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule rbd {
  \truleset 2 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  
  # end crush map
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
