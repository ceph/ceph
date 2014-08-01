  $ osdmaptool --createsimple 3 myosdmap
  *osdmaptool: osdmap file 'myosdmap' (glob)
  *osdmaptool: writing epoch 1 to myosdmap (glob)

#
# --test-map-object / --pool
#
  $ osdmaptool myosdmap --test-map-object foo --pool
  Option --pool requires an argument.
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool bar
  strict_strtoll: expected integer, got: 'bar'
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool 123
  *osdmaptool: osdmap file 'myosdmap' (glob)
  There is no pool 123
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool 0
  *osdmaptool: osdmap file 'myosdmap' (glob)
   object 'foo' \-\> 0\..* (re)

  $ osdmaptool myosdmap --test-map-object foo
  *osdmaptool: osdmap file 'myosdmap' (glob)
  *osdmaptool: assuming pool 0 (use --pool to override) (glob)
   object 'foo' \-\> 0\..* (re)

#
# --test-map-pgs / --pool
#
  $ osdmaptool myosdmap --test-map-pgs --pool
  Option --pool requires an argument.
  [1]

  $ osdmaptool myosdmap --test-map-pgs --pool baz
  strict_strtoll: expected integer, got: 'baz'
  [1]

  $ osdmaptool myosdmap --test-map-pgs --pool 123
  *osdmaptool: osdmap file 'myosdmap' (glob)
  There is no pool 123
  [1]

  $ osdmaptool myosdmap --mark-up-in --test-map-pgs --pool 0 | grep pool
  *osdmaptool: osdmap file 'myosdmap' (glob)
  pool 0 pg_num .* (re)

  $ osdmaptool myosdmap --mark-up-in --test-map-pgs | grep pool
  *osdmaptool: osdmap file 'myosdmap' (glob)
  pool 0 pg_num .* (re)
