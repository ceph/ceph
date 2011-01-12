  $ cat >test.conf <<EOF
  > [bar]
  > bar = green
  > [foo]
  > bar = blue
  > [baz]
  > bar = yellow
  > [thud]
  > bar = yellow
  > EOF

  $ cconf -c test.conf -l bar
  bar

  $ cconf -c test.conf -l b
  bar
  baz

