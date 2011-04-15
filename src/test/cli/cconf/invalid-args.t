  $ cat >test.conf <<EOF
  > [bar]
  > bar = green
  > EOF

# TODO output an error
  $ cconf -c test.conf broken
  [1]

  $ cconf -c test.conf --name total.garbage
  You must pass a string of the form TYPE.ID to the --name option. Valid types are: auth, mon, osd, mds, client
  [1]

  $ cconf -c test.conf -s bar
  You must give an action, such as --lookup or --list-all-sections.
  Pass --help for more help.
  [1]
