  $ osdmaptool --createsimple 3 myosdmap --with-default-pool
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: writing epoch 1 to myosdmap

#
# --test-map-object / --pool
#
  $ osdmaptool myosdmap --test-map-object foo --pool
  Option --pool requires an argument.
  
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool bar
  The option value 'bar' is invalid
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool 123
  osdmaptool: osdmap file 'myosdmap'
  There is no pool 123
  [1]

  $ osdmaptool myosdmap --test-map-object foo --pool 1
  osdmaptool: osdmap file 'myosdmap'
   object 'foo' \-\> 1\..* (re)

  $ osdmaptool myosdmap --test-map-object foo
  osdmaptool: osdmap file 'myosdmap'
  osdmaptool: assuming pool 1 (use --pool to override)
   object 'foo' \-\> 1\..* (re)

#
# --test-map-pgs / --pool
#
  $ osdmaptool myosdmap --test-map-pgs --pool
  Option --pool requires an argument.
  
  [1]

  $ osdmaptool myosdmap --test-map-pgs --pool baz
  The option value 'baz' is invalid
  [1]

  $ osdmaptool myosdmap --test-map-pgs --pool 123
  osdmaptool: osdmap file 'myosdmap'
  There is no pool 123
  [1]

  $ osdmaptool myosdmap --mark-up-in --test-map-pgs --pool 1 | grep pool
  osdmaptool: osdmap file 'myosdmap'
  pool 1 pg_num .* (re)

  $ osdmaptool myosdmap --mark-up-in --test-map-pgs | grep pool
  osdmaptool: osdmap file 'myosdmap'
  pool 1 pg_num .* (re)
