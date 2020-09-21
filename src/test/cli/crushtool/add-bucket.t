  $ crushtool -i "$TESTDIR/simple.template" --add-bucket host0 host --loc cluster cluster0 -o map0 > /dev/null
  $ crushtool -i map0 --add-bucket host1 host -o map1 > /dev/null
  $ crushtool -i map1 --move host1 --loc cluster cluster0 -o map2 > /dev/null
  $ crushtool -i map2 --add-item 1 1.0 device1 --loc cluster cluster0 -o map3 > /dev/null
  $ crushtool -i map3 --move device1 --loc host host0 -o map4 > /dev/null
  $ crushtool -d map4
  # begin crush map
  
  # devices
  device 1 device1
  
  # types
  type 0 device
  type 1 host
  type 2 cluster
  
  # buckets
  host host0 {
  \tid -2\t\t# do not change unnecessarily (esc)
  \t# weight 1.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem device1 weight 1.000 (esc)
  }
  host host1 {
  \tid -3\t\t# do not change unnecessarily (esc)
  \t# weight 0.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  }
  cluster cluster0 {
  \tid -1\t\t# do not change unnecessarily (esc)
  \t# weight 1.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem host0 weight 1.000 (esc)
  \titem host1 weight 0.000 (esc)
  }
  
  # rules
  rule data {
  \tid 0 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule metadata {
  \tid 1 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule rbd {
  \tid 2 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take cluster0 (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  
  # end crush map
