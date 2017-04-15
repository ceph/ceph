  $ cp "$TESTDIR/choose-args.crush" .
  $ crushtool -c choose-args.crush -o choose-args.compiled
  $ crushtool -d choose-args.compiled -o choose-args.conf
  $ crushtool -c choose-args.conf -o choose-args.recompiled
  $ cmp choose-args.crush choose-args.conf
  $ cmp choose-args.compiled choose-args.recompiled
