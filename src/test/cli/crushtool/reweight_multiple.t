  $ crushtool -c "$TESTDIR/simple.template.multitree" -o mt
  $ crushtool -i mt --reweight-item osd1 2.5 -o mt
  crushtool reweighting item osd1 to 2.5
  $ crushtool -d mt -o mt.txt
  $ diff mt.txt "$TESTDIR/simple.template.multitree.reweighted"
