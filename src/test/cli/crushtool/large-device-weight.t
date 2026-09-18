#
# CRUSH_MAX_DEVICE_WEIGHT is enforced by the crush compiler only, so it can
# only be exercised via 'crushtool -c'.  The limit is 1000, raised from 100 to
# accommodate devices larger than 100 TiB.
#

#
# a device weight well above the old limit of 100 compiles, and survives a
# compile / decompile round trip unchanged
#

  $ crushtool -c "$TESTDIR/large-device-weight.crushmap.txt" -o compiled
  $ crushtool -d compiled | grep -c "item device0 weight 123.00000"
  1
  $ crushtool -d compiled -o decompiled
  $ crushtool -c decompiled -o recompiled
  $ cmp compiled recompiled

#
# a device weight exactly at the limit is accepted
#

  $ crushtool -c "$TESTDIR/max-device-weight.crushmap.txt" -o compiled-max
  $ crushtool -d compiled-max | grep -c "item device0 weight 1000.00000"
  1

#
# a device weight above the limit is rejected
#

  $ crushtool -c "$TESTDIR/too-large-device-weight.crushmap.txt" -o compiled-too-large
  device weight limited to 1000
  [1]
