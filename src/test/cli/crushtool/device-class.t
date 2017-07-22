  $ cp "$TESTDIR/device-class.crush" .
  $ crushtool -c device-class.crush -o device-class.compiled
  $ crushtool -d device-class.compiled -o device-class.conf
  $ crushtool -c device-class.conf -o device-class.recompiled
  $ cmp device-class.crush device-class.conf
  $ cmp device-class.compiled device-class.recompiled
