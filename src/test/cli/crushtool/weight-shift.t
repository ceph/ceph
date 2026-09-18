#
# The text format speaks raw weights, so it has to carry weight_shift along
# with them: without it every weight in the map would stand for a different
# amount of capacity than it was written for.
#

#
# weight_shift survives a compile / decompile round trip
#

  $ crushtool -c "$TESTDIR/weight-shift.crushmap.txt" -o compiled
  $ crushtool -d compiled | grep -c "tunable weight_shift 4"
  1
  $ crushtool -d compiled | grep -c "item device0 weight 62.50000"
  1
  $ crushtool -d compiled -o decompiled
  $ crushtool -c decompiled -o recompiled
  $ cmp compiled recompiled

#
# CRUSH_MAX_DEVICE_WEIGHT bounds the capacity a single device stands for, not
# the raw number in the map, so it scales with the shift: at shift 4 the 1000
# TiB limit is a raw weight of 62.5, and the map above sits exactly on it.
# One raw unit more is over the limit.  See large-device-weight.t for the
# shift 0 case.
#

  $ crushtool -c "$TESTDIR/weight-shift-too-large-device.crushmap.txt" -o compiled-too-large
  device weight limited to 1000 TiB, which at weight_shift 4 is a raw weight of 62.5
  [1]

#
# the shift itself is bounded by CRUSH_MAX_WEIGHT_SHIFT
#

  $ crushtool -c "$TESTDIR/weight-shift-too-large-shift.crushmap.txt" -o compiled-bad-shift
  weight_shift must be between 0 and 16
  [1]
