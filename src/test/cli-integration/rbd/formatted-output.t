ls on empty pool never containing images
========================================
  $ ceph osd pool create rbd_other
  pool 'rbd_other' created
  $ rbd pool init rbd_other
  $ rados -p rbd rm rbd_directory >/dev/null 2>&1 || true
  $ rbd ls
  $ rbd ls --format json
  []
  $ rbd ls --format xml
  <images></images>

create
=======
  $ RBD_FORCE_ALLOW_V1=1 rbd create -s 1024 --image-format 1 foo --log-to-stderr=false
  rbd: image format 1 is deprecated
  $ rbd create -s 512 --image-format 2 bar
  $ rbd create -s 2048 --image-format 2 --image-feature layering baz
  $ RBD_FORCE_ALLOW_V1=1 rbd create -s 1 --image-format 1 quux --log-to-stderr=false
  rbd: image format 1 is deprecated
  $ rbd create -s 1G --image-format 2 quuy

snapshot
========
  $ rbd snap create bar@snap --no-progress
  $ rbd resize -s 1024 --no-progress bar
  $ rbd resize -s 2G --no-progress quuy
  $ rbd snap create bar@snap2 --no-progress
  $ rbd snap create foo@snap --no-progress

clone
=====
  $ rbd snap protect bar@snap
  $ rbd clone --image-feature layering,exclusive-lock,object-map,fast-diff bar@snap rbd_other/child
  $ rbd snap create rbd_other/child@snap --no-progress
  $ rbd flatten rbd_other/child 2> /dev/null
  $ rbd bench rbd_other/child --io-type write --io-pattern seq --io-total 1B > /dev/null 2>&1
  $ rbd clone bar@snap rbd_other/deep-flatten-child
  $ rbd snap create rbd_other/deep-flatten-child@snap --no-progress
  $ rbd flatten rbd_other/deep-flatten-child 2> /dev/null

lock
====
  $ rbd lock add quux id
  $ rbd lock add baz id1 --shared tag
  $ rbd lock add baz id2 --shared tag
  $ rbd lock add baz id3 --shared tag

test formatting
===============
  $ rbd children foo@snap
  $ rbd children bar@snap
  rbd_other/child
  $ rbd children bar@snap2
TODO: figure out why .* does not match the block_name_prefix line in rbd info.
For now, use a more inclusive regex.
  $ rbd info foo
  rbd image 'foo':
  \tsize 1 GiB in 256 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 1 (esc)
  [^^]+ (re)
  \tformat: 1 (esc)
  $ rbd info foo --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "id": "", 
      "name": "foo", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "size": 1073741824, 
      "snapshot_count": 1
  }
  $ rbd info foo --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>foo</name>
    <id/>
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>1</snapshot_count>
    <block_name_prefix>rb.0.*</block_name_prefix> (glob)
    <format>1</format>
  </image>
  $ rbd info foo@snap
  rbd image 'foo':
  \tsize 1 GiB in 256 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 1 (esc)
  [^^]+ (re)
  \tformat: 1 (esc)
  \tprotected: False (esc)
  $ rbd info foo@snap --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "id": "", 
      "name": "foo", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "protected": "false", 
      "size": 1073741824, 
      "snapshot_count": 1
  }
  $ rbd info foo@snap --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>foo</name>
    <id/>
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>1</snapshot_count>
    <block_name_prefix>rb.0*</block_name_prefix> (glob)
    <format>1</format>
    <protected>false</protected>
  </image>
  $ rbd info bar
  rbd image 'bar':
  \tsize 1 GiB in 256 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 2 (esc)
  \tid:* (glob)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \top_features:  (esc)
  \tflags:  (esc)
  \tcreate_timestamp:* (glob)
  \taccess_timestamp:* (glob)
  \tmodify_timestamp:* (glob)
  $ rbd info bar --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "access_timestamp": "*",  (glob)
      "block_name_prefix": "rbd_data.*",  (glob)
      "create_timestamp": "*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "id": "*",  (glob)
      "modify_timestamp": "*",  (glob)
      "name": "bar", 
      "object_size": 4194304, 
      "objects": 256, 
      "op_features": [], 
      "order": 22, 
      "size": 1073741824, 
      "snapshot_count": 2
  }
  $ rbd info bar --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>bar</name>
    <id>*</id> (glob)
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>2</snapshot_count>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <op_features/>
    <flags/>
    <create_timestamp>*</create_timestamp> (glob)
    <access_timestamp>*</access_timestamp> (glob)
    <modify_timestamp>*</modify_timestamp> (glob)
  </image>
  $ rbd info bar@snap
  rbd image 'bar':
  \tsize 512 MiB in 128 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 2 (esc)
  \tid:* (glob)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \top_features:  (esc)
  \tflags:  (esc)
  \tcreate_timestamp:* (glob)
  \taccess_timestamp:* (glob)
  \tmodify_timestamp:* (glob)
  \tprotected: True (esc)
  $ rbd info bar@snap --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "access_timestamp": "*",  (glob)
      "block_name_prefix": "rbd_data.*",  (glob)
      "create_timestamp": "*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "id": "*",  (glob)
      "modify_timestamp": "*",  (glob)
      "name": "bar", 
      "object_size": 4194304, 
      "objects": 128, 
      "op_features": [], 
      "order": 22, 
      "protected": "true", 
      "size": 536870912, 
      "snapshot_count": 2
  }
  $ rbd info bar@snap --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>bar</name>
    <id>*</id> (glob)
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>2</snapshot_count>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <op_features/>
    <flags/>
    <create_timestamp>*</create_timestamp> (glob)
    <access_timestamp>*</access_timestamp> (glob)
    <modify_timestamp>*</modify_timestamp> (glob)
    <protected>true</protected>
  </image>
  $ rbd info bar@snap2
  rbd image 'bar':
  \tsize 1 GiB in 256 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 2 (esc)
  \tid:* (glob)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \top_features:  (esc)
  \tflags:  (esc)
  \tcreate_timestamp:* (glob)
  \taccess_timestamp:* (glob)
  \tmodify_timestamp:* (glob)
  \tprotected: False (esc)
  $ rbd info bar@snap2 --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "access_timestamp": "*",  (glob)
      "block_name_prefix": "rbd_data.*",  (glob)
      "create_timestamp": "*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "id": "*",  (glob)
      "modify_timestamp": "*",  (glob)
      "name": "bar", 
      "object_size": 4194304, 
      "objects": 256, 
      "op_features": [], 
      "order": 22, 
      "protected": "false", 
      "size": 1073741824, 
      "snapshot_count": 2
  }
  $ rbd info bar@snap2 --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>bar</name>
    <id>*</id> (glob)
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>2</snapshot_count>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <op_features/>
    <flags/>
    <create_timestamp>*</create_timestamp> (glob)
    <access_timestamp>*</access_timestamp> (glob)
    <modify_timestamp>*</modify_timestamp> (glob)
    <protected>false</protected>
  </image>
  $ rbd info baz
  rbd image 'baz':
  \tsize 2 GiB in 512 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 0 (esc)
  \tid:* (glob)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering (esc)
  \top_features:  (esc)
  \tflags:  (esc)
  \tcreate_timestamp:* (glob)
  \taccess_timestamp:* (glob)
  \tmodify_timestamp:* (glob)
  $ rbd info baz --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "access_timestamp": "*",  (glob)
      "block_name_prefix": "rbd_data.*",  (glob)
      "create_timestamp": "*",  (glob)
      "features": [
          "layering"
      ], 
      "flags": [], 
      "format": 2, 
      "id": "*",  (glob)
      "modify_timestamp": "*",  (glob)
      "name": "baz", 
      "object_size": 4194304, 
      "objects": 512, 
      "op_features": [], 
      "order": 22, 
      "size": 2147483648, 
      "snapshot_count": 0
  }
  $ rbd info baz --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>baz</name>
    <id>*</id> (glob)
    <size>2147483648</size>
    <objects>512</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>0</snapshot_count>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
    </features>
    <op_features/>
    <flags/>
    <create_timestamp>*</create_timestamp> (glob)
    <access_timestamp>*</access_timestamp> (glob)
    <modify_timestamp>*</modify_timestamp> (glob)
  </image>
  $ rbd info quux
  rbd image 'quux':
  \tsize 1 MiB in 1 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 0 (esc)
  [^^]+ (re)
  \tformat: 1 (esc)
  $ rbd info quux --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "id": "", 
      "name": "quux", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576, 
      "snapshot_count": 0
  }
  $ rbd info quux --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>quux</name>
    <id/>
    <size>1048576</size>
    <objects>1</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>0</snapshot_count>
    <block_name_prefix>rb.0.*</block_name_prefix> (glob)
    <format>1</format>
  </image>
  $ rbd info rbd_other/child
  rbd image 'child':
  \tsize 512 MiB in 128 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 1 (esc)
  \tid:* (glob)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff (esc)
  \top_features:  (esc)
  \tflags:  (esc)
  \tcreate_timestamp:* (glob)
  \taccess_timestamp:* (glob)
  \tmodify_timestamp:* (glob)
  $ rbd info rbd_other/child --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "access_timestamp": "*",  (glob)
      "block_name_prefix": "rbd_data.*",  (glob)
      "create_timestamp": "*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff"
      ], 
      "flags": [], 
      "format": 2, 
      "id": "*",  (glob)
      "modify_timestamp": "*",  (glob)
      "name": "child", 
      "object_size": 4194304, 
      "objects": 128, 
      "op_features": [], 
      "order": 22, 
      "size": 536870912, 
      "snapshot_count": 1
  }
  $ rbd info rbd_other/child --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>child</name>
    <id>*</id> (glob)
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>1</snapshot_count>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
    </features>
    <op_features/>
    <flags/>
    <create_timestamp>*</create_timestamp> (glob)
    <access_timestamp>*</access_timestamp> (glob)
    <modify_timestamp>*</modify_timestamp> (glob)
  </image>
  $ rbd info rbd_other/child@snap
  rbd image 'child':
  \tsize 512 MiB in 128 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 1 (esc)
  \tid:* (glob)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff (esc)
  \top_features:  (esc)
  \tflags:  (esc)
  \tcreate_timestamp:* (glob)
  \taccess_timestamp:* (glob)
  \tmodify_timestamp:* (glob)
  \tprotected: False (esc)
  \tparent: rbd/bar@snap (esc)
  \toverlap: 512 MiB (esc)
  $ rbd info rbd_other/child@snap --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "access_timestamp": "*",  (glob)
      "block_name_prefix": "rbd_data.*",  (glob)
      "create_timestamp": "*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff"
      ], 
      "flags": [], 
      "format": 2, 
      "id": "*",  (glob)
      "modify_timestamp": "*",  (glob)
      "name": "child", 
      "object_size": 4194304, 
      "objects": 128, 
      "op_features": [], 
      "order": 22, 
      "parent": {
          "id": "*",  (glob)
          "image": "bar", 
          "overlap": 536870912, 
          "pool": "rbd", 
          "pool_namespace": "", 
          "snapshot": "snap", 
          "trash": false
      }, 
      "protected": "false", 
      "size": 536870912, 
      "snapshot_count": 1
  }
  $ rbd info rbd_other/child@snap --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>child</name>
    <id>*</id> (glob)
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>1</snapshot_count>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
    </features>
    <op_features/>
    <flags/>
    <create_timestamp>*</create_timestamp> (glob)
    <access_timestamp>*</access_timestamp> (glob)
    <modify_timestamp>*</modify_timestamp> (glob)
    <protected>false</protected>
    <parent>
      <pool>rbd</pool>
      <pool_namespace/>
      <image>bar</image>
      <id>*</id> (glob)
      <snapshot>snap</snapshot>
      <trash>false</trash>
      <overlap>536870912</overlap>
    </parent>
  </image>
  $ rbd info rbd_other/deep-flatten-child
  rbd image 'deep-flatten-child':
  \tsize 512 MiB in 128 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 1 (esc)
  \tid:* (glob)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \top_features:  (esc)
  \tflags:  (esc)
  \tcreate_timestamp:* (glob)
  \taccess_timestamp:* (glob)
  \tmodify_timestamp:* (glob)
  $ rbd info rbd_other/deep-flatten-child --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "access_timestamp": "*",  (glob)
      "block_name_prefix": "rbd_data.*",  (glob)
      "create_timestamp": "*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "id": "*",  (glob)
      "modify_timestamp": "*",  (glob)
      "name": "deep-flatten-child", 
      "object_size": 4194304, 
      "objects": 128, 
      "op_features": [], 
      "order": 22, 
      "size": 536870912, 
      "snapshot_count": 1
  }
  $ rbd info rbd_other/deep-flatten-child --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>deep-flatten-child</name>
    <id>*</id> (glob)
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>1</snapshot_count>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <op_features/>
    <flags/>
    <create_timestamp>*</create_timestamp> (glob)
    <access_timestamp>*</access_timestamp> (glob)
    <modify_timestamp>*</modify_timestamp> (glob)
  </image>
  $ rbd info rbd_other/deep-flatten-child@snap
  rbd image 'deep-flatten-child':
  \tsize 512 MiB in 128 objects (esc)
  \torder 22 (4 MiB objects) (esc)
  \tsnapshot_count: 1 (esc)
  \tid:* (glob)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \top_features:  (esc)
  \tflags:  (esc)
  \tcreate_timestamp:* (glob)
  \taccess_timestamp:* (glob)
  \tmodify_timestamp:* (glob)
  \tprotected: False (esc)
  $ rbd info rbd_other/deep-flatten-child@snap --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "access_timestamp": "*",  (glob)
      "block_name_prefix": "rbd_data.*",  (glob)
      "create_timestamp": "*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "id": "*",  (glob)
      "modify_timestamp": "*",  (glob)
      "name": "deep-flatten-child", 
      "object_size": 4194304, 
      "objects": 128, 
      "op_features": [], 
      "order": 22, 
      "protected": "false", 
      "size": 536870912, 
      "snapshot_count": 1
  }
  $ rbd info rbd_other/deep-flatten-child@snap --format xml | xmlstarlet format -s 2 -o || true
  <image>
    <name>deep-flatten-child</name>
    <id>*</id> (glob)
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <snapshot_count>1</snapshot_count>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <op_features/>
    <flags/>
    <create_timestamp>*</create_timestamp> (glob)
    <access_timestamp>*</access_timestamp> (glob)
    <modify_timestamp>*</modify_timestamp> (glob)
    <protected>false</protected>
  </image>
  $ rbd list
  foo
  quux
  bar
  baz
  quuy
  $ rbd list --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      "foo", 
      "quux", 
      "bar", 
      "baz", 
      "quuy"
  ]
  $ rbd list --format xml | xmlstarlet format -s 2 -o || true
  <images>
    <name>foo</name>
    <name>quux</name>
    <name>bar</name>
    <name>baz</name>
    <name>quuy</name>
  </images>
  $ rbd list -l
  NAME       SIZE     PARENT  FMT  PROT  LOCK
  foo          1 GiB            1            
  foo@snap     1 GiB            1            
  quux         1 MiB            1        excl
  bar          1 GiB            2            
  bar@snap   512 MiB            2  yes       
  bar@snap2    1 GiB            2            
  baz          2 GiB            2        shr 
  quuy         2 GiB            2            
  $ rbd list -l --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      {
          "format": 1, 
          "id": "", 
          "image": "foo", 
          "size": 1073741824
      }, 
      {
          "format": 1, 
          "id": "", 
          "image": "foo", 
          "protected": "false", 
          "size": 1073741824, 
          "snapshot": "snap", 
          "snapshot_id": * (glob)
      }, 
      {
          "format": 1, 
          "id": "", 
          "image": "quux", 
          "lock_type": "exclusive", 
          "size": 1048576
      }, 
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "bar", 
          "size": 1073741824
      }, 
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "bar", 
          "protected": "true", 
          "size": 536870912, 
          "snapshot": "snap", 
          "snapshot_id": * (glob)
      }, 
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "bar", 
          "protected": "false", 
          "size": 1073741824, 
          "snapshot": "snap2", 
          "snapshot_id": * (glob)
      }, 
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "baz", 
          "lock_type": "shared", 
          "size": 2147483648
      }, 
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "quuy", 
          "size": 2147483648
      }
  ]
  $ rbd list -l --format xml | xmlstarlet format -s 2 -o || true
  <images>
    <image>
      <image>foo</image>
      <id/>
      <size>1073741824</size>
      <format>1</format>
    </image>
    <snapshot>
      <image>foo</image>
      <id/>
      <snapshot>snap</snapshot>
      <snapshot_id>*</snapshot_id> (glob)
      <size>1073741824</size>
      <format>1</format>
      <protected>false</protected>
    </snapshot>
    <image>
      <image>quux</image>
      <id/>
      <size>1048576</size>
      <format>1</format>
      <lock_type>exclusive</lock_type>
    </image>
    <image>
      <image>bar</image>
      <id>*</id> (glob)
      <size>1073741824</size>
      <format>2</format>
    </image>
    <snapshot>
      <image>bar</image>
      <id>*</id> (glob)
      <snapshot>snap</snapshot>
      <snapshot_id>*</snapshot_id> (glob)
      <size>536870912</size>
      <format>2</format>
      <protected>true</protected>
    </snapshot>
    <snapshot>
      <image>bar</image>
      <id>*</id> (glob)
      <snapshot>snap2</snapshot>
      <snapshot_id>*</snapshot_id> (glob)
      <size>1073741824</size>
      <format>2</format>
      <protected>false</protected>
    </snapshot>
    <image>
      <image>baz</image>
      <id>*</id> (glob)
      <size>2147483648</size>
      <format>2</format>
      <lock_type>shared</lock_type>
    </image>
    <image>
      <image>quuy</image>
      <id>*</id> (glob)
      <size>2147483648</size>
      <format>2</format>
    </image>
  </images>
  $ rbd list rbd_other
  child
  deep-flatten-child
  $ rbd list rbd_other --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      "child", 
      "deep-flatten-child"
  ]
  $ rbd list rbd_other --format xml | xmlstarlet format -s 2 -o || true
  <images>
    <name>child</name>
    <name>deep-flatten-child</name>
  </images>
  $ rbd list rbd_other -l
  NAME                     SIZE     PARENT        FMT  PROT  LOCK
  child                    512 MiB                  2            
  child@snap               512 MiB  rbd/bar@snap    2            
  deep-flatten-child       512 MiB                  2            
  deep-flatten-child@snap  512 MiB                  2            
  $ rbd list rbd_other -l --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "child", 
          "size": 536870912
      }, 
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "child", 
          "parent": {
              "image": "bar", 
              "pool": "rbd", 
              "pool_namespace": "", 
              "snapshot": "snap"
          }, 
          "protected": "false", 
          "size": 536870912, 
          "snapshot": "snap", 
          "snapshot_id": * (glob)
      }, 
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "deep-flatten-child", 
          "size": 536870912
      }, 
      {
          "format": 2, 
          "id": "*",  (glob)
          "image": "deep-flatten-child", 
          "protected": "false", 
          "size": 536870912, 
          "snapshot": "snap", 
          "snapshot_id": * (glob)
      }
  ]
  $ rbd list rbd_other -l --format xml | xmlstarlet format -s 2 -o || true
  <images>
    <image>
      <image>child</image>
      <id>*</id> (glob)
      <size>536870912</size>
      <format>2</format>
    </image>
    <snapshot>
      <image>child</image>
      <id>*</id> (glob)
      <snapshot>snap</snapshot>
      <snapshot_id>*</snapshot_id> (glob)
      <size>536870912</size>
      <parent>
        <pool>rbd</pool>
        <pool_namespace/>
        <image>bar</image>
        <snapshot>snap</snapshot>
      </parent>
      <format>2</format>
      <protected>false</protected>
    </snapshot>
    <image>
      <image>deep-flatten-child</image>
      <id>*</id> (glob)
      <size>536870912</size>
      <format>2</format>
    </image>
    <snapshot>
      <image>deep-flatten-child</image>
      <id>*</id> (glob)
      <snapshot>snap</snapshot>
      <snapshot_id>*</snapshot_id> (glob)
      <size>536870912</size>
      <format>2</format>
      <protected>false</protected>
    </snapshot>
  </images>
  $ rbd lock list foo
  $ rbd lock list foo --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  []
  $ rbd lock list foo --format xml | xmlstarlet format -s 2 -o || true
  <locks/>
  $ rbd lock list quux
  There is 1 exclusive lock on this image.
  Locker*ID*Address* (glob)
  client.* id * (glob)
  $ rbd lock list quux --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      {
          "address": "*",  (glob)
          "id": "id", 
          "locker": "client.*" (glob)
      }
  ]
  $ rbd lock list quux --format xml | xmlstarlet format -s 2 -o || true
  <locks>
    <lock>
      <id>id</id>
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </lock>
  </locks>
  $ rbd lock list baz
  There are 3 shared locks on this image.
  Lock tag: tag
  Locker*ID*Address* (glob)
  client.*id[123].* (re)
  client.*id[123].* (re)
  client.*id[123].* (re)
  $ rbd lock list baz --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      {
          "address": "*",  (glob)
          "id": "id*",  (glob)
          "locker": "client.*" (glob)
      }, 
      {
          "address": "*",  (glob)
          "id": "id*",  (glob)
          "locker": "client.*" (glob)
      }, 
      {
          "address": "*",  (glob)
          "id": "id*",  (glob)
          "locker": "client.*" (glob)
      }
  ]
  $ rbd lock list baz --format xml | xmlstarlet format -s 2 -o || true
  <locks>
    <lock>
      <id>id*</id> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </lock>
    <lock>
      <id>id*</id> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </lock>
    <lock>
      <id>id*</id> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </lock>
  </locks>
  $ rbd snap list foo
  SNAPID*NAME*SIZE*PROTECTED*TIMESTAMP* (glob)
  *snap*1 GiB* (glob)
  $ rbd snap list foo --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      {
          "id": *,  (glob)
          "name": "snap", 
          "protected": "false", 
          "size": 1073741824, 
          "timestamp": ""
      }
  ]
  $ rbd snap list foo --format xml | xmlstarlet format -s 2 -o || true
  <snapshots>
    <snapshot>
      <id>*</id> (glob)
      <name>snap</name>
      <size>1073741824</size>
      <protected>false</protected>
      <timestamp/>
    </snapshot>
  </snapshots>
  $ rbd snap list bar
  SNAPID*NAME*SIZE*PROTECTED*TIMESTAMP* (glob)
  *snap*512 MiB*yes* (glob)
  *snap2*1 GiB* (glob)
  $ rbd snap list bar --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      {
          "id": *,  (glob)
          "name": "snap", 
          "protected": "true", 
          "size": 536870912, 
          "timestamp": * (glob)
      }, 
      {
          "id": *,  (glob)
          "name": "snap2", 
          "protected": "false", 
          "size": 1073741824, 
          "timestamp": * (glob)
      }
  ]
  $ rbd snap list bar --format xml | xmlstarlet format -s 2 -o || true
  <snapshots>
    <snapshot>
      <id>*</id> (glob)
      <name>snap</name>
      <size>536870912</size>
      <protected>true</protected>
      <timestamp>*</timestamp> (glob)
    </snapshot>
    <snapshot>
      <id>*</id> (glob)
      <name>snap2</name>
      <size>1073741824</size>
      <protected>false</protected>
      <timestamp>*</timestamp> (glob)
    </snapshot>
  </snapshots>
  $ rbd snap list baz
  $ rbd snap list baz --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  []
  $ rbd snap list baz --format xml | xmlstarlet format -s 2 -o || true
  <snapshots/>
  $ rbd snap list rbd_other/child
  SNAPID*NAME*SIZE*PROTECTED*TIMESTAMP* (glob)
  *snap*512 MiB* (glob)
  $ rbd snap list rbd_other/child --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      {
          "id": *,  (glob)
          "name": "snap", 
          "protected": "false", 
          "size": 536870912, 
          "timestamp": * (glob)
      }
  ]
  $ rbd snap list rbd_other/child --format xml | xmlstarlet format -s 2 -o || true
  <snapshots>
    <snapshot>
      <id>*</id> (glob)
      <name>snap</name>
      <size>536870912</size>
      <protected>false</protected>
      <timestamp>*</timestamp> (glob)
    </snapshot>
  </snapshots>
  $ rbd disk-usage --pool rbd_other 2>/dev/null
  NAME                     PROVISIONED  USED 
  child@snap                   512 MiB    0 B
  child                        512 MiB  4 MiB
  deep-flatten-child@snap      512 MiB    0 B
  deep-flatten-child           512 MiB    0 B
  <TOTAL>                        1 GiB  4 MiB
  $ rbd disk-usage --pool rbd_other --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  {
      "images": [
          {
              "id": "*",  (glob)
              "name": "child", 
              "provisioned_size": 536870912, 
              "snapshot": "snap", 
              "snapshot_id": *,  (glob)
              "used_size": 0
          }, 
          {
              "id": "*",  (glob)
              "name": "child", 
              "provisioned_size": 536870912, 
              "used_size": 4194304
          }, 
          {
              "id": "*",  (glob)
              "name": "deep-flatten-child", 
              "provisioned_size": 536870912, 
              "snapshot": "snap", 
              "snapshot_id": *,  (glob)
              "used_size": 0
          }, 
          {
              "id": "*",  (glob)
              "name": "deep-flatten-child", 
              "provisioned_size": 536870912, 
              "used_size": 0
          }
      ], 
      "total_provisioned_size": 1073741824, 
      "total_used_size": 4194304
  }
  $ rbd disk-usage --pool rbd_other --format xml | xmlstarlet format -s 2 -o || true
  <stats>
    <images>
      <image>
        <name>child</name>
        <id>*</id> (glob)
        <snapshot>snap</snapshot>
        <snapshot_id>*</snapshot_id> (glob)
        <provisioned_size>536870912</provisioned_size>
        <used_size>0</used_size>
      </image>
      <image>
        <name>child</name>
        <id>*</id> (glob)
        <provisioned_size>536870912</provisioned_size>
        <used_size>4194304</used_size>
      </image>
      <image>
        <name>deep-flatten-child</name>
        <id>*</id> (glob)
        <snapshot>snap</snapshot>
        <snapshot_id>*</snapshot_id> (glob)
        <provisioned_size>536870912</provisioned_size>
        <used_size>0</used_size>
      </image>
      <image>
        <name>deep-flatten-child</name>
        <id>*</id> (glob)
        <provisioned_size>536870912</provisioned_size>
        <used_size>0</used_size>
      </image>
    </images>
    <total_provisioned_size>1073741824</total_provisioned_size>
    <total_used_size>4194304</total_used_size>
  </stats>

# cleanup
  $ rbd snap remove --no-progress rbd_other/deep-flatten-child@snap
  $ rbd snap remove --no-progress rbd_other/child@snap
  $ rbd snap unprotect bar@snap
  $ rbd snap purge bar 2> /dev/null
  $ rbd snap purge foo 2> /dev/null
  $ rbd rm rbd_other/deep-flatten-child 2> /dev/null
  $ rbd rm rbd_other/child 2> /dev/null
  $ rbd rm foo 2> /dev/null
  $ rbd rm bar 2> /dev/null
  $ rbd rm quux 2> /dev/null
  $ rbd rm quuy 2> /dev/null
  $ rbd rm baz 2> /dev/null
  $ ceph osd pool delete rbd_other rbd_other --yes-i-really-really-mean-it
  pool 'rbd_other' removed
