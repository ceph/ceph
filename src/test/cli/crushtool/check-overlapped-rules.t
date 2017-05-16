  $ crushtool -c "$TESTDIR/check-overlapped-rules.crushmap.txt" -o "$TESTDIR/check-overlapped-rules.crushmap"
  ruleset '0' already defined in rule 'rule-r0'
  
  ruleset '0' already defined in rule 'rule-r0'
  
  ruleset '0' already defined in rule 'rule-r0'
  
  ruleset '0' already defined in rule 'rule-r0'
  
  ruleset '0' already defined in rule 'rule-r0'
  
  $ crushtool -i "$TESTDIR/check-overlapped-rules.crushmap" --check
  $ rm -f "$TESTDIR/check-overlapped-rules.crushmap"
