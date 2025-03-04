  $ crushtool -i "$TESTDIR/simple.template" --add-item 0 1.0 device0 --loc host host0 --loc cluster cluster0 -o check-names.crushmap > /dev/null
  $ crushtool -i check-names.crushmap       --add-item 1 1.0 device1 --loc host host0 --loc cluster cluster0 -o check-names.crushmap > /dev/null
  $ crushtool -i check-names.crushmap --check 2
  $ crushtool -i check-names.crushmap       --add-item 2 1.0 device2 --loc host host0 --loc cluster cluster0 -o check-names.crushmap > /dev/null
  $ crushtool -i check-names.crushmap --check 2
  item id too large: item#2
  [1]
  $ crushtool -i check-names.crushmap --check
