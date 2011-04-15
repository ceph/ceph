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

  $ cconf -c test.conf bar -s foo
  blue

# test the funny "equals sign" argument passing convention
  $ cconf --conf=test.conf bar -s foo
  blue

  $ cconf --conf=test.conf -L
  bar
  baz
  foo
  global
  nobar
  thud

  $ cconf --conf=test.conf --list-all-sections
  bar
  baz
  foo
  global
  nobar
  thud

  $ cconf --conf=test.conf --list_all_sections
  bar
  baz
  foo
  global
  nobar
  thud

# TODO man page stops in the middle of a sentence

  $ cconf -c test.conf bar -s xyzzy
  [1]

  $ cconf -c test.conf bar -s xyzzy
  [1]

  $ cconf -c test.conf bar -s xyzzy -s thud
  red

  $ cconf -c test.conf bar -s nobar -s thud
  red

  $ cconf -c test.conf bar -s thud -s baz
  red

  $ cconf -c test.conf bar -s baz -s thud
  yellow

  $ cconf -c test.conf bar -s xyzzy -s nobar -s thud -s baz
  red

