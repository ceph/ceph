# This detects the incorrect mapping (due to off by one error in
# linear search) that caused #1594
  $ crushtool -i "$TESTDIR/five-devices.crushmap" --test --x 3 --rule 2 --force 3 -v --weight 1 0 --weight 2 0 --weight 4 0
  devices weights (hex): [10000,0,0,10000,0]
  rule 2 (rbd), x = 3..3
  rule 2 x 3 [3,0]
   device 0:\t1 (esc)
   device 1:\t0 (esc)
   device 2:\t0 (esc)
   device 3:\t1 (esc)
   device 4:\t0 (esc)
   result size 2x:\t1 (esc)
