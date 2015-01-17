  $ map="$TESTDIR/build.crushmap"

#
# display the crush tree by default
#
  $ crushtool --outfn "$map" --build --num_osds 5 node straw 2 rack straw 1 root straw 0
  .* (re)
  ID\tWEIGHT\tTYPE NAME (esc)
  -7\t5.00000\troot root (esc)
  -4\t2.00000\t\track rack0 (esc)
  -1\t2.00000\t\t\tnode node0 (esc)
  0\t1.00000\t\t\t\tosd.0 (esc)
  1\t1.00000\t\t\t\tosd.1 (esc)
  -5\t2.00000\t\track rack1 (esc)
  -2\t2.00000\t\t\tnode node1 (esc)
  2\t1.00000\t\t\t\tosd.2 (esc)
  3\t1.00000\t\t\t\tosd.3 (esc)
  -6\t1.00000\t\track rack2 (esc)
  -3\t1.00000\t\t\tnode node2 (esc)
  4\t1.00000\t\t\t\tosd.4 (esc)
  

#  
# silence all messages with --debug-crush 0
#
  $ CEPH_ARGS="--debug-crush 0" crushtool --outfn "$map" --build --num_osds 5 node straw 2 rack straw 1 root straw 0

#
# display a warning if there is more than one root
#
  $ crushtool --outfn "$map" --build --num_osds 5 node straw 2 rack straw 1 
  .* (re)
  ID\tWEIGHT\tTYPE NAME (esc)
  -6\t1.00000\track rack2 (esc)
  -3\t1.00000\t\tnode node2 (esc)
  4\t1.00000\t\t\tosd.4 (esc)
  -5\t2.00000\track rack1 (esc)
  -2\t2.00000\t\tnode node1 (esc)
  2\t1.00000\t\t\tosd.2 (esc)
  3\t1.00000\t\t\tosd.3 (esc)
  -4\t2.00000\track rack0 (esc)
  -1\t2.00000\t\tnode node0 (esc)
  0\t1.00000\t\t\tosd.0 (esc)
  1\t1.00000\t\t\tosd.1 (esc)
  
  .* The crush rulesets will use the root rack0 (re)
  and ignore the others.
  There are 3 roots, they can be
  grouped into a single root by appending something like:
    root straw 0
  
#
# crush rulesets are generated using the OSDMap helpers
#
  $ CEPH_ARGS="--debug-crush 0" crushtool --outfn "$map" --set-straw-calc-version 0 --build --num_osds 1 root straw 0
  $ crushtool -o "$map.txt" -d "$map"
  $ cat "$map.txt"
  # begin crush map
  tunable choose_local_tries 0
  tunable choose_local_fallback_tries 0
  tunable choose_total_tries 50
  tunable chooseleaf_descend_once 1
  
  # devices
  device 0 device0
  
  # types
  type 0 device
  type 1 root
  
  # buckets
  root root {
  \tid -1\t\t# do not change unnecessarily (esc)
  \t# weight 1.000 (esc)
  \talg straw (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem device0 weight 1.000 (esc)
  }
  
  # rules
  rule replicated_ruleset {
  \truleset 0 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take root (esc)
  \tstep chooseleaf firstn 0 type root (esc)
  \tstep emit (esc)
  }
  
  # end crush map
  $ rm "$map" "$map.txt"

#
# Wrong number of arguments 
#
  $ crushtool --outfn "$map" --debug-crush 0 --build --num_osds 5 node straw 0
  remaining args: [--debug-crush,0,node,straw,0]
  layers must be specified with 3-tuples of (name, buckettype, size)
  [1]

# Local Variables:
# compile-command: "cd ../../.. ; make crushtool && test/run-cli-tests"
# End:
