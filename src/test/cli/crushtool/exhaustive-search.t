# This detects the incorrect mapping (due to off by one error in
# linear search) that caused #1594
  $ crushtool -i "$TESTDIR/five-devices.crushmap" --test --x 3 --rule 2 -v --weight 1 0 --weight 2 0 --weight 4 0
  rule 2 (rbd), x = 3..3, numrep = 1..10
  rule 2 (rbd) num_rep 1 result size == 1:\t1/1 (esc)
  rule 2 (rbd) num_rep 2 result size == 2:\t1/1 (esc)
  rule 2 (rbd) num_rep 3 result size == 2:\t1/1 (esc)
  rule 2 (rbd) num_rep 4 result size == 2:\t1/1 (esc)
  rule 2 (rbd) num_rep 5 result size == 2:\t1/1 (esc)
  rule 2 (rbd) num_rep 6 result size == 2:\t1/1 (esc)
  rule 2 (rbd) num_rep 7 result size == 2:\t1/1 (esc)
  rule 2 (rbd) num_rep 8 result size == 2:\t1/1 (esc)
  rule 2 (rbd) num_rep 9 result size == 2:\t1/1 (esc)
  rule 2 (rbd) num_rep 10 result size == 2:\t1/1 (esc)
