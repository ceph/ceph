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
