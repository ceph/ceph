  $ crushtool -i "$TESTDIR/check-overlapped-rules.crushmap" --check
  overlapped rules in ruleset 0: rule-r0, rule-r1, rule-r2
  overlapped rules in ruleset 0: rule-r0, rule-r2, rule-r3
  overlapped rules in ruleset 0: rule-r0, rule-r3
  $ crushtool -c "$TESTDIR/check-overlapped-rules.crushmap.txt" -o "$TESTDIR/check-overlapped-rules.crushmap.new"
  rule 0 already exists
  [1]
