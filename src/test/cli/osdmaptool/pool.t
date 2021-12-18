  $ osdmaptool --createsimple 3 --with-default-pool -o myosdmap
  osdmaptool: writing epoch 0 to myosdmap

#
# --test-map-object / --pool
#
  $ osdmaptool myosdmap --test-map-object foo --pool
  Option --pool requires an argument.
  
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool bar
  The option value 'bar' is invalid for --pool
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool 123
  osdmaptool: input osdmap file 'myosdmap'
  There is no pool 123
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool 1
  osdmaptool: input osdmap file 'myosdmap'
   object 'foo' \-\> 1\..* (re)

  $ osdmaptool myosdmap --test-map-object foo
  osdmaptool: input osdmap file 'myosdmap'
  osdmaptool: assuming pool 1 (use --pool to override)
   object 'foo' \-\> 1\..* (re)

#
# --test-map-pgs / --pool
#
  $ osdmaptool myosdmap --test-map-pgs --pool
  Option --pool requires an argument.
  
  [1]

  $ osdmaptool myosdmap --test-map-pgs --pool baz
  The option value 'baz' is invalid for --pool
  [1]

  $ osdmaptool myosdmap --test-map-pgs --pool 123
  osdmaptool: input osdmap file 'myosdmap'
  There is no pool 123
  [1]

  $ osdmaptool myosdmap --mark-up-in --test-map-pgs --pool 1 | grep pool
  osdmaptool: input osdmap file 'myosdmap'
  pool 1 pg_num .* (re)

  $ osdmaptool myosdmap --mark-up-in --test-map-pgs | grep pool
  osdmaptool: input osdmap file 'myosdmap'
  pool 1 pg_num .* (re)
