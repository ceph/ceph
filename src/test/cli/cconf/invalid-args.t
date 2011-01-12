  $ cat >test.conf <<EOF
  > [bar]
  > bar = green
  > EOF

# TODO output an error
  $ cconf -c test.conf broken
  [1]

# TODO output an error (missing key)
  $ cconf -c test.conf -s bar
  *** Caught signal (Segmentation fault) ***
  in thread [0-9a-f]{12} (re)
   ceph version .* (re)
   1: .* (re)
   2: .* (re)
   3: .* (re)
   4: .* (re)
   5: .* (re)
   6: .* (re)
  Segmentation fault
  [139]
