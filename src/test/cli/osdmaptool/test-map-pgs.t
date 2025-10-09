  $ NUM_OSDS=500
  $ POOL_COUNT=1 # data + metadata + rbd
  $ SIZE=3
  $ PG_BITS=4
#
# create an osdmap with a few hundred devices and a realistic crushmap
#
  $ OSD_MAP="osdmap"
  $ osdmaptool --osd_pool_default_size $SIZE --pg_bits $PG_BITS --createsimple $NUM_OSDS "$OSD_MAP" > /dev/null --with-default-pool
  osdmaptool: osdmap file 'osdmap'
  $ CRUSH_MAP="crushmap"
  $ CEPH_ARGS="--debug-crush 0" crushtool --outfn "$CRUSH_MAP" --build --num_osds $NUM_OSDS node straw 10 rack straw 10 root straw 0
  $ osdmaptool --import-crush "$CRUSH_MAP" "$OSD_MAP" > /dev/null
  osdmaptool: osdmap file 'osdmap'
  $ OUT="$TESTDIR/out"
# 
# --test-map-pgs
#
  $ osdmaptool --mark-up-in --test-map-pgs "$OSD_MAP" > "$OUT"
  osdmaptool: osdmap file 'osdmap'
  $ PG_NUM=$(($NUM_OSDS << $PG_BITS))
  $ grep "pg_num $PG_NUM" "$OUT" || cat $OUT
  pool 1 pg_num 8000
  $ TOTAL=$((POOL_COUNT * $PG_NUM))
  $ grep -E "size $SIZE[[:space:]]$TOTAL" $OUT || cat $OUT
  size 3\t8000 (esc)
  $ STATS_CRUSH=$(grep '^ avg ' "$OUT")
# 
# --test-map-pgs --test-random is expected to change nothing regarding the totals
#
  $ osdmaptool --mark-up-in --test-random --test-map-pgs "$OSD_MAP" > "$OUT"
  osdmaptool: osdmap file 'osdmap'
  $ PG_NUM=$(($NUM_OSDS << $PG_BITS))
  $ grep "pg_num $PG_NUM" "$OUT" || cat $OUT
  pool 1 pg_num 8000
  $ TOTAL=$((POOL_COUNT * $PG_NUM))
  $ grep -E "size $SIZE[[:space:]]$TOTAL" $OUT || cat $OUT
  size 3\t8000 (esc)
  $ STATS_RANDOM=$(grep '^ avg ' "$OUT")
#
# cleanup
#
  $ rm -f "$CRUSH_MAP" "$OSD_MAP" "$OUT"
