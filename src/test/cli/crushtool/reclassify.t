  $ crushtool -i $TESTDIR/crush-classes/a --set-subtree-class default hdd --reclassify --reclassify-bucket %-ssd ssd default --reclassify-bucket ssd ssd default --reclassify-root default hdd -o foo
  classify_root default (-1) as hdd
    renumbering bucket -1 -> -5
    renumbering bucket -4 -> -6
    renumbering bucket -3 -> -7
    renumbering bucket -2 -> -8
  classify_bucket %-ssd as ssd default bucket default (root)
  match %-ssd to ttipod001-cephosd-2-ssd basename ttipod001-cephosd-2
    have base -8
  match %-ssd to ttipod001-cephosd-1-ssd basename ttipod001-cephosd-1
    have base -7
  match %-ssd to ttipod001-cephosd-3-ssd basename ttipod001-cephosd-3
    have base -6
  classify_bucket ssd as ssd default bucket default (root)
  match ssd to ssd basename default
    have base -5
  moving items from -24 (ttipod001-cephosd-3-ssd) to -6 (ttipod001-cephosd-3)
  moving items from -23 (ttipod001-cephosd-1-ssd) to -7 (ttipod001-cephosd-1)
  moving items from -22 (ttipod001-cephosd-2-ssd) to -8 (ttipod001-cephosd-2)
  moving items from -21 (ssd) to -5 (default)
  $ crushtool -i $TESTDIR/crush-classes/a --compare foo
  rule 0 had 0/10240 mismatched mappings (0)
  rule 1 had 0/10240 mismatched mappings (0)
  maps appear equivalent

  $ crushtool -i $TESTDIR/crush-classes/d --set-subtree-class default hdd --reclassify --reclassify-bucket %-ssd ssd default --reclassify-bucket ssd ssd default --reclassify-root default hdd -o foo
  classify_root default (-1) as hdd
    renumbering bucket -1 -> -13
    renumbering bucket -6 -> -14
    renumbering bucket -5 -> -15
    renumbering bucket -4 -> -16
    renumbering bucket -3 -> -17
    renumbering bucket -2 -> -18
  classify_bucket %-ssd as ssd default bucket default (root)
  match %-ssd to node-20-ssd basename node-20
    have base -18
  match %-ssd to node-21-ssd basename node-21
    created base -25
  match %-ssd to node-22-ssd basename node-22
    created base -26
  match %-ssd to node-23-ssd basename node-23
    created base -27
  match %-ssd to node-27-ssd basename node-27
    created base -28
  classify_bucket ssd as ssd default bucket default (root)
  match ssd to ssd basename default
    have base -13
  moving items from -12 (node-27-ssd) to -28 (node-27)
  moving items from -11 (node-23-ssd) to -27 (node-23)
  moving items from -10 (node-22-ssd) to -26 (node-22)
  moving items from -9 (node-21-ssd) to -25 (node-21)
  moving items from -8 (node-20-ssd) to -18 (node-20)
  moving items from -7 (ssd) to -13 (default)
  $ crushtool -i $TESTDIR/crush-classes/d --compare foo
  rule 0 had 0/10240 mismatched mappings (0)
  rule 1 had 0/10240 mismatched mappings (0)
  maps appear equivalent

  $ crushtool -i $TESTDIR/crush-classes/e --reclassify --reclassify-bucket ceph-osd-ssd-% ssd default --reclassify-bucket ssd-root ssd default --reclassify-root default hdd -o foo
  classify_root default (-1) as hdd
    renumbering bucket -1 -> -55
    renumbering bucket -34 -> -56
    renumbering bucket -20 -> -57
    renumbering bucket -14 -> -58
    renumbering bucket -15 -> -59
    renumbering bucket -16 -> -60
    renumbering bucket -52 -> -61
    renumbering bucket -46 -> -62
    renumbering bucket -40 -> -63
  classify_bucket ceph-osd-ssd-% as ssd default bucket default (root)
  match ceph-osd-ssd-% to ceph-osd-ssd-node4 basename node4
    have base -57
  match ceph-osd-ssd-% to ceph-osd-ssd-node3 basename node3
    have base -58
  match ceph-osd-ssd-% to ceph-osd-ssd-node1 basename node1
    have base -60
  match ceph-osd-ssd-% to ceph-osd-ssd-node2 basename node2
    have base -59
  match ceph-osd-ssd-% to ceph-osd-ssd-node5 basename node5
    have base -56
  match ceph-osd-ssd-% to ceph-osd-ssd-node6 basename node6
    have base -63
  match ceph-osd-ssd-% to ceph-osd-ssd-node7 basename node7
    have base -62
  match ceph-osd-ssd-% to ceph-osd-ssd-node8 basename node8
    have base -61
  classify_bucket ssd-root as ssd default bucket default (root)
  match ssd-root to ssd-root basename default
    have base -55
  moving items from -49 (ceph-osd-ssd-node8) to -61 (node8)
  moving items from -43 (ceph-osd-ssd-node7) to -62 (node7)
  moving items from -37 (ceph-osd-ssd-node6) to -63 (node6)
  moving items from -31 (ceph-osd-ssd-node5) to -56 (node5)
  moving items from -18 (ssd-root) to -55 (default)
  moving items from -9 (ceph-osd-ssd-node2) to -59 (node2)
  moving items from -7 (ceph-osd-ssd-node1) to -60 (node1)
  moving items from -5 (ceph-osd-ssd-node3) to -58 (node3)
  moving items from -3 (ceph-osd-ssd-node4) to -57 (node4)

this one has weird node weights, so *lots* of mappings change...

  $ crushtool -i $TESTDIR/crush-classes/e --compare foo
  rule 0 had 6540/10240 mismatched mappings (0.638672)
  rule 1 had 8417/10240 mismatched mappings (0.821973)
  warning: maps are NOT equivalent
  [1]

  $ crushtool -i $TESTDIR/crush-classes/c --reclassify --reclassify-bucket %-SSD ssd default --reclassify-bucket ssd ssd default --reclassify-root default hdd -o foo
  classify_root default (-1) as hdd
    renumbering bucket -1 -> -55
    renumbering bucket -9 -> -56
    renumbering bucket -8 -> -57
    renumbering bucket -7 -> -58
    renumbering bucket -6 -> -59
    renumbering bucket -5 -> -60
    renumbering bucket -4 -> -61
    renumbering bucket -3 -> -62
    renumbering bucket -2 -> -63
  classify_bucket %-SSD as ssd default bucket default (root)
  match %-SSD to Ceph-Stor1-SSD basename Ceph-Stor1
    have base -63
  match %-SSD to Ceph-Stor2-SSD basename Ceph-Stor2
    have base -62
  match %-SSD to Ceph-Stor3-SSD basename Ceph-Stor3
    have base -61
  match %-SSD to Ceph-Stor4-SSD basename Ceph-Stor4
    have base -60
  match %-SSD to Ceph-Stor5-SSD basename Ceph-Stor5
    have base -59
  match %-SSD to Ceph-Stor6-SSD basename Ceph-Stor6
    have base -58
  match %-SSD to Ceph-Stor7-SSD basename Ceph-Stor7
    have base -57
  match %-SSD to Ceph-Stor8-SSD basename Ceph-Stor8
    have base -56
  classify_bucket ssd as ssd default bucket default (root)
  match ssd to ssd basename default
    have base -55
  moving items from -18 (ssd) to -55 (default)
  moving items from -17 (Ceph-Stor8-SSD) to -56 (Ceph-Stor8)
  moving items from -16 (Ceph-Stor7-SSD) to -57 (Ceph-Stor7)
  moving items from -15 (Ceph-Stor6-SSD) to -58 (Ceph-Stor6)
  moving items from -14 (Ceph-Stor5-SSD) to -59 (Ceph-Stor5)
  moving items from -13 (Ceph-Stor4-SSD) to -60 (Ceph-Stor4)
  moving items from -12 (Ceph-Stor3-SSD) to -61 (Ceph-Stor3)
  moving items from -11 (Ceph-Stor2-SSD) to -62 (Ceph-Stor2)
  moving items from -10 (Ceph-Stor1-SSD) to -63 (Ceph-Stor1)

wonky crush weights on Ceph-Stor1, so a small number of mappings change
because the new map has a strictly summing hierarchy.

  $ crushtool -i $TESTDIR/crush-classes/c --compare foo
  rule 0 had 158/10240 mismatched mappings (0.0154297)
  rule 1 had 62/5120 mismatched mappings (0.0121094)
  rule 2 had 0/10240 mismatched mappings (0)
  warning: maps are NOT equivalent
  [1]

  $ crushtool -i $TESTDIR/crush-classes/beesly --set-subtree-class 0513-R-0060 hdd --set-subtree-class 0513-R-0050 hdd --reclassify --reclassify-root 0513-R-0050 hdd --reclassify-root 0513-R-0060 hdd -o foo
  classify_root 0513-R-0050 (-2) as hdd
    renumbering bucket -2 -> -131
    renumbering bucket -14 -> -132
    renumbering bucket -34 -> -133
    renumbering bucket -33 -> -134
    renumbering bucket -30 -> -135
    renumbering bucket -26 -> -136
    renumbering bucket -22 -> -137
    renumbering bucket -18 -> -138
    renumbering bucket -13 -> -139
    renumbering bucket -9 -> -140
    renumbering bucket -12 -> -141
    renumbering bucket -11 -> -142
    renumbering bucket -32 -> -143
    renumbering bucket -31 -> -144
    renumbering bucket -10 -> -145
    renumbering bucket -8 -> -146
    renumbering bucket -6 -> -147
    renumbering bucket -28 -> -148
    renumbering bucket -27 -> -149
    renumbering bucket -21 -> -150
    renumbering bucket -20 -> -151
    renumbering bucket -19 -> -152
    renumbering bucket -7 -> -153
    renumbering bucket -5 -> -154
    renumbering bucket -4 -> -155
    renumbering bucket -25 -> -156
    renumbering bucket -24 -> -157
    renumbering bucket -23 -> -158
    renumbering bucket -17 -> -159
    renumbering bucket -16 -> -160
    renumbering bucket -15 -> -161
    renumbering bucket -3 -> -162
    renumbering bucket -72 -> -163
    renumbering bucket -98 -> -164
    renumbering bucket -97 -> -165
    renumbering bucket -96 -> -166
    renumbering bucket -95 -> -167
    renumbering bucket -94 -> -168
    renumbering bucket -93 -> -169
    renumbering bucket -68 -> -170
  classify_root 0513-R-0060 (-65) as hdd
    renumbering bucket -65 -> -35
    renumbering bucket -76 -> -36
    renumbering bucket -78 -> -37
    renumbering bucket -87 -> -38
    renumbering bucket -82 -> -39
    renumbering bucket -81 -> -40
    renumbering bucket -77 -> -41
    renumbering bucket -75 -> -42
    renumbering bucket -89 -> -43
    renumbering bucket -85 -> -44
    renumbering bucket -84 -> -45
    renumbering bucket -74 -> -46
    renumbering bucket -71 -> -47
    renumbering bucket -80 -> -48
    renumbering bucket -91 -> -49
    renumbering bucket -90 -> -50
    renumbering bucket -88 -> -51
    renumbering bucket -79 -> -52
    renumbering bucket -70 -> -53
    renumbering bucket -86 -> -54
    renumbering bucket -83 -> -55
    renumbering bucket -73 -> -56
    renumbering bucket -69 -> -57
  $ crushtool -i $TESTDIR/crush-classes/beesly --compare foo
  rule 0 had 0/10240 mismatched mappings (0)
  rule 1 had 0/10240 mismatched mappings (0)
  rule 2 had 0/10240 mismatched mappings (0)
  rule 4 had 0/10240 mismatched mappings (0)
  maps appear equivalent

  $ crushtool -i $TESTDIR/crush-classes/flax --reclassify --reclassify-root default hdd -o foo
  classify_root default (-1) as hdd
    renumbering bucket -1 -> -5
    renumbering bucket -12 -> -7
    renumbering bucket -9 -> -8
    renumbering bucket -6 -> -10
    renumbering bucket -4 -> -11
    renumbering bucket -3 -> -13
    renumbering bucket -2 -> -14
  $ crushtool -i $TESTDIR/crush-classes/flax --compare foo
  rule 0 had 0/10240 mismatched mappings (0)
  maps appear equivalent

  $ crushtool -i $TESTDIR/crush-classes/gabe --reclassify --reclassify-root default hdd -o foo
  classify_root default (-1) as hdd
    rule 3 includes take on root default class 0
  failed to reclassify map
  [1]

above fails because of ec-rack-by-2-hdd also has take default class hdd.

below is an adjusted version of the same cluster's map

  $ crushtool -i $TESTDIR/crush-classes/gabe2 --reclassify --reclassify-root default hdd -o foo
  classify_root default (-1) as hdd
    renumbering bucket -1 -> -178
    renumbering bucket -4 -> -179
    renumbering bucket -25 -> -180
    renumbering bucket -16 -> -181
    renumbering bucket -21 -> -182
    renumbering bucket -19 -> -183
    renumbering bucket -15 -> -184
    renumbering bucket -7 -> -185
    renumbering bucket -47 -> -186
    renumbering bucket -18 -> -187
    renumbering bucket -8 -> -188
    renumbering bucket -6 -> -189
    renumbering bucket -12 -> -190
    renumbering bucket -23 -> -191
    renumbering bucket -22 -> -192
    renumbering bucket -20 -> -193
    renumbering bucket -11 -> -194
    renumbering bucket -10 -> -195
    renumbering bucket -17 -> -196
    renumbering bucket -13 -> -197
    renumbering bucket -9 -> -198
    renumbering bucket -3 -> -199
    renumbering bucket -14 -> -200
    renumbering bucket -5 -> -201
    renumbering bucket -2 -> -202
  $ crushtool -i $TESTDIR/crush-classes/gabe2 --compare foo
  rule 0 had 627/10240 mismatched mappings (0.0612305)
  rule 1 had 422/6144 mismatched mappings (0.0686849)
  warning: maps are NOT equivalent
  [1]



  $ crushtool -i $TESTDIR/crush-classes/b --reclassify --reclassify-bucket %-hdd hdd default --reclassify-bucket %-ssd ssd default --reclassify-bucket ssd ssd default --reclassify-bucket hdd hdd default -o foo
  classify_bucket %-hdd as hdd default bucket default (root)
  match %-hdd to berta-hdd basename berta
    have base -37
  match %-hdd to oelgard-hdd basename oelgard
    have base -36
  match %-hdd to leonhard-hdd basename leonhard
    have base -33
  match %-hdd to gottlieb-hdd basename gottlieb
    have base -30
  match %-hdd to hieronymus-hdd basename hieronymus
    have base -31
  match %-hdd to uhu-hdd basename uhu
    have base -34
  match %-hdd to euphrosyne-hdd basename euphrosyne
    have base -35
  match %-hdd to frauenhaus-hdd basename frauenhaus
    created base -145
  match %-hdd to herrenhaus-hdd basename herrenhaus
    created base -146
  match %-hdd to zoo-hdd basename zoo
    created base -147
  match %-hdd to borkenkaefer-hdd basename borkenkaefer
    have base -4
  match %-hdd to hirsch-hdd basename hirsch
    have base -41
  match %-hdd to cassowary-hdd basename cassowary
    created base -148
  match %-hdd to fuchs-hdd basename fuchs
    created base -149
  match %-hdd to analia-hdd basename analia
    created base -150
  match %-hdd to gundula-hdd basename gundula
    created base -151
  match %-hdd to achim-hdd basename achim
    created base -152
  match %-hdd to hugo-hdd basename hugo
    created base -153
  match %-hdd to carl-hdd basename carl
    have base -32
  classify_bucket %-ssd as ssd default bucket default (root)
  match %-ssd to frauenhaus-ssd basename frauenhaus
    already creating base -145
  match %-ssd to herrenhaus-ssd basename herrenhaus
    already creating base -146
  match %-ssd to zoo-ssd basename zoo
    already creating base -147
  match %-ssd to berta-ssd basename berta
    have base -37
  match %-ssd to euphrosyne-ssd basename euphrosyne
    have base -35
  match %-ssd to oelgard-ssd basename oelgard
    have base -36
  match %-ssd to leonhard-ssd basename leonhard
    have base -33
  match %-ssd to hieronymus-ssd basename hieronymus
    have base -31
  match %-ssd to gottlieb-ssd basename gottlieb
    have base -30
  match %-ssd to uhu-ssd basename uhu
    have base -34
  match %-ssd to borkenkaefer-ssd basename borkenkaefer
    have base -4
  match %-ssd to hirsch-ssd basename hirsch
    have base -41
  match %-ssd to phaidon-ssd basename phaidon
    created base -154
  match %-ssd to glykera-ssd basename glykera
    created base -155
  match %-ssd to bonobo-ssd basename bonobo
    created base -156
  classify_bucket hdd as hdd default bucket default (root)
  match hdd to hdd basename default
    have base -1
  classify_bucket ssd as ssd default bucket default (root)
  match ssd to ssd basename default
    have base -1
  moving items from -124 (bonobo-ssd) to -156 (bonobo)
  moving items from -123 (glykera-ssd) to -155 (glykera)
  moving items from -122 (phaidon-ssd) to -154 (phaidon)
  moving items from -121 (carl-hdd) to -32 (carl)
  moving items from -120 (hugo-hdd) to -153 (hugo)
  moving items from -119 (achim-hdd) to -152 (achim)
  moving items from -118 (gundula-hdd) to -151 (gundula)
  moving items from -117 (analia-hdd) to -150 (analia)
  moving items from -116 (fuchs-hdd) to -149 (fuchs)
  moving items from -115 (cassowary-hdd) to -148 (cassowary)
  moving items from -39 (hirsch-ssd) to -41 (hirsch)
  moving items from -38 (hirsch-hdd) to -41 (hirsch)
  moving items from -29 (borkenkaefer-ssd) to -4 (borkenkaefer)
  moving items from -28 (hdd) to -1 (default)
  moving items from -27 (ssd) to -1 (default)
  moving items from -26 (uhu-ssd) to -34 (uhu)
  moving items from -25 (gottlieb-ssd) to -30 (gottlieb)
  moving items from -24 (hieronymus-ssd) to -31 (hieronymus)
  moving items from -23 (leonhard-ssd) to -33 (leonhard)
  moving items from -22 (borkenkaefer-hdd) to -4 (borkenkaefer)
  moving items from -21 (oelgard-ssd) to -36 (oelgard)
  moving items from -20 (euphrosyne-ssd) to -35 (euphrosyne)
  moving items from -19 (berta-ssd) to -37 (berta)
  moving items from -17 (zoo-ssd) to -147 (zoo)
  moving items from -16 (herrenhaus-ssd) to -146 (herrenhaus)
  moving items from -15 (frauenhaus-ssd) to -145 (frauenhaus)
  moving items from -12 (zoo-hdd) to -147 (zoo)
  moving items from -11 (herrenhaus-hdd) to -146 (herrenhaus)
  moving items from -10 (frauenhaus-hdd) to -145 (frauenhaus)
  moving items from -9 (euphrosyne-hdd) to -35 (euphrosyne)
  moving items from -8 (uhu-hdd) to -34 (uhu)
  moving items from -7 (hieronymus-hdd) to -31 (hieronymus)
  moving items from -6 (gottlieb-hdd) to -30 (gottlieb)
  moving items from -5 (leonhard-hdd) to -33 (leonhard)
  moving items from -3 (oelgard-hdd) to -36 (oelgard)
  moving items from -2 (berta-hdd) to -37 (berta)
  new bucket -156 missing parent, adding at {root=default}
  new bucket -155 missing parent, adding at {root=default}
  new bucket -154 missing parent, adding at {root=default}

  $ crushtool -i $TESTDIR/crush-classes/b --compare foo
  rule 0 had 0/3072 mismatched mappings (0)
  rule 1 had 0/4096 mismatched mappings (0)
  maps appear equivalent

  $ crushtool -i $TESTDIR/crush-classes/f --reclassify --reclassify-root default hdd -o foo
  classify_root default (-1) as hdd
    renumbering bucket -1 -> -178
    renumbering bucket -4 -> -179
    renumbering bucket -25 -> -180
    renumbering bucket -16 -> -181
    renumbering bucket -21 -> -182
    renumbering bucket -19 -> -183
    renumbering bucket -15 -> -184
    renumbering bucket -7 -> -185
    renumbering bucket -47 -> -186
    renumbering bucket -18 -> -187
    renumbering bucket -8 -> -188
    renumbering bucket -6 -> -189
    renumbering bucket -12 -> -190
    renumbering bucket -23 -> -191
    renumbering bucket -22 -> -192
    renumbering bucket -20 -> -193
    renumbering bucket -11 -> -194
    renumbering bucket -10 -> -195
    renumbering bucket -17 -> -196
    renumbering bucket -13 -> -197
    renumbering bucket -9 -> -198
    renumbering bucket -3 -> -199
    renumbering bucket -14 -> -200
    renumbering bucket -5 -> -201
    renumbering bucket -2 -> -202

We expect some mismatches below because there are some ssd-labeled nodes under
default that we aren't changing the class on.

  $ crushtool -i $TESTDIR/crush-classes/f --compare foo
  rule 0 had 627/10240 mismatched mappings (0.0612305)
  rule 1 had 422/6144 mismatched mappings (0.0686849)
  warning: maps are NOT equivalent
  [1]

  $ crushtool -i $TESTDIR/crush-classes/g --reclassify --reclassify-bucket sata-% hdd-sata default --reclassify-bucket sas-% hdd-sas default --reclassify-bucket sas hdd-sas default --reclassify-bucket sata hdd-sata default -o foo
  classify_bucket sas as hdd-sas default bucket default (root)
  match sas to sas basename default
    have base -1
  classify_bucket sas-% as hdd-sas default bucket default (root)
  match sas-% to sas-osd01 basename osd01
    created base -73
  match sas-% to sas-osd02 basename osd02
    created base -74
  match sas-% to sas-osd03 basename osd03
    created base -75
  match sas-% to sas-osd04 basename osd04
    created base -76
  match sas-% to sas-osd05 basename osd05
    created base -77
  match sas-% to sas-osd06 basename osd06
    created base -78
  match sas-% to sas-osd07 basename osd07
    created base -79
  match sas-% to sas-osd08 basename osd08
    created base -80
  match sas-% to sas-osd09 basename osd09
    created base -81
  match sas-% to sas-rack1 basename rack1
    created base -82
  match sas-% to sas-rack2 basename rack2
    created base -83
  match sas-% to sas-rack3 basename rack3
    created base -84
  classify_bucket sata as hdd-sata default bucket default (root)
  match sata to sata basename default
    have base -1
  classify_bucket sata-% as hdd-sata default bucket default (root)
  match sata-% to sata-osd11 basename osd11
    created base -85
  match sata-% to sata-osd10 basename osd10
    created base -86
  match sata-% to sata-osd14 basename osd14
    created base -87
  match sata-% to sata-osd13 basename osd13
    created base -88
  match sata-% to sata-osd12 basename osd12
    created base -89
  match sata-% to sata-osd15 basename osd15
    created base -90
  match sata-% to sata-osd16 basename osd16
    created base -91
  match sata-% to sata-osd18 basename osd18
    created base -92
  match sata-% to sata-osd19 basename osd19
    created base -93
  match sata-% to sata-osd17 basename osd17
    created base -94
  match sata-% to sata-osd20 basename osd20
    created base -95
  match sata-% to sata-osd21 basename osd21
    created base -96
  match sata-% to sata-osd22 basename osd22
    created base -97
  match sata-% to sata-osd23 basename osd23
    created base -98
  match sata-% to sata-osd24 basename osd24
    created base -99
  match sata-% to sata-osd25 basename osd25
    created base -100
  match sata-% to sata-osd26 basename osd26
    created base -101
  match sata-% to sata-osd27 basename osd27
    created base -102
  match sata-% to sata-rack1 basename rack1
    already creating base -82
  match sata-% to sata-rack2 basename rack2
    already creating base -83
  match sata-% to sata-rack3 basename rack3
    already creating base -84
  moving items from -36 (sas) to -1 (default)
  moving items from -35 (sata) to -1 (default)
  moving items from -34 (sas-rack3) to -84 (rack3)
  moving items from -33 (sas-rack2) to -83 (rack2)
  moving items from -32 (sas-rack1) to -82 (rack1)
  moving items from -31 (sata-rack3) to -84 (rack3)
  moving items from -30 (sata-rack2) to -83 (rack2)
  moving items from -29 (sata-rack1) to -82 (rack1)
  moving items from -28 (sas-osd09) to -81 (osd09)
  moving items from -27 (sas-osd08) to -80 (osd08)
  moving items from -26 (sas-osd07) to -79 (osd07)
  moving items from -25 (sas-osd06) to -78 (osd06)
  moving items from -24 (sas-osd05) to -77 (osd05)
  moving items from -23 (sas-osd04) to -76 (osd04)
  moving items from -22 (sas-osd03) to -75 (osd03)
  moving items from -21 (sas-osd02) to -74 (osd02)
  moving items from -20 (sata-osd27) to -102 (osd27)
  moving items from -19 (sas-osd01) to -73 (osd01)
  moving items from -18 (sata-osd26) to -101 (osd26)
  moving items from -17 (sata-osd25) to -100 (osd25)
  moving items from -16 (sata-osd24) to -99 (osd24)
  moving items from -15 (sata-osd23) to -98 (osd23)
  moving items from -14 (sata-osd22) to -97 (osd22)
  moving items from -13 (sata-osd21) to -96 (osd21)
  moving items from -12 (sata-osd20) to -95 (osd20)
  moving items from -11 (sata-osd17) to -94 (osd17)
  moving items from -10 (sata-osd19) to -93 (osd19)
  moving items from -9 (sata-osd18) to -92 (osd18)
  moving items from -8 (sata-osd16) to -91 (osd16)
  moving items from -7 (sata-osd15) to -90 (osd15)
  moving items from -6 (sata-osd12) to -89 (osd12)
  moving items from -5 (sata-osd13) to -88 (osd13)
  moving items from -4 (sata-osd14) to -87 (osd14)
  moving items from -3 (sata-osd10) to -86 (osd10)
  moving items from -2 (sata-osd11) to -85 (osd11)
  $ crushtool -i $TESTDIR/crush-classes/g --compare foo
  rule 0 had 0/10240 mismatched mappings (0)
  rule 1 had 0/10240 mismatched mappings (0)
  maps appear equivalent
