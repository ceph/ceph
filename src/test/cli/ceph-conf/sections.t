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

  $ ceph-conf -c test.conf -l bar
  bar

  $ ceph-conf -c test.conf -l b
  bar
  baz

