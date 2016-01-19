  $ crushtool -c "$TESTDIR/check-overlapped-rules.crushmap.txt" -o "$TESTDIR/check-overlapped-rules.crushmap"
  $ crushtool -i "$TESTDIR/check-overlapped-rules.crushmap" --check
  overlapped rules in ruleset 0: rule-r0, rule-r1, rule-r2
  overlapped rules in ruleset 0: rule-r0, rule-r2, rule-r3
  overlapped rules in ruleset 0: rule-r0, rule-r3
  $ rm -f "$TESTDIR/check-overlapped-rules.crushmap"
