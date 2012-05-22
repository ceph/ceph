  $ cat >test.conf <<EOF
  > [bar]
  > bar = green
  > [foo]
  > bar = blue
  > [baz]
  > bar = yellow
  > [thud]
  > bar = red
  > [nobar]
  > other = 42
  > EOF

  $ ceph-conf -c test.conf bar -s foo
  blue

# test the funny "equals sign" argument passing convention
  $ ceph-conf --conf=test.conf bar -s foo
  blue

  $ ceph-conf --conf=test.conf -L
  bar
  baz
  foo
  global
  nobar
  thud

  $ ceph-conf --conf=test.conf --list-all-sections
  bar
  baz
  foo
  global
  nobar
  thud

  $ ceph-conf --conf=test.conf --list_all_sections
  bar
  baz
  foo
  global
  nobar
  thud

# TODO man page stops in the middle of a sentence

  $ ceph-conf -c test.conf bar -s xyzzy
  [1]

  $ ceph-conf -c test.conf bar -s xyzzy
  [1]

  $ ceph-conf -c test.conf bar -s xyzzy -s thud
  red

  $ ceph-conf -c test.conf bar -s nobar -s thud
  red

  $ ceph-conf -c test.conf bar -s thud -s baz
  red

  $ ceph-conf -c test.conf bar -s baz -s thud
  yellow

  $ ceph-conf -c test.conf bar -s xyzzy -s nobar -s thud -s baz
  red

