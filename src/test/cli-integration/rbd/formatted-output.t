ls on empty pool never containing images
========================================
  $ ceph osd pool create rbd_other 8
  pool 'rbd_other' created
  $ rados -p rbd rm rbd_directory >/dev/null 2>&1 || true
  $ rbd ls
  $ rbd ls --format json
  [] (no-eol)
  $ rbd ls --format xml
  <images></images> (no-eol)

create
=======
  $ rbd create -s 1024 --image-format 1 foo
  rbd: image format 1 is deprecated
  $ rbd create -s 512 --image-format 2 bar
  $ rbd create -s 2048 --image-format 2 --image-feature layering baz
  $ rbd create -s 1 --image-format 1 quux
  rbd: image format 1 is deprecated
  $ rbd create -s 1G --image-format 2 quuy

snapshot
========
  $ rbd snap create bar@snap
  $ rbd resize -s 1024 --no-progress bar
  $ rbd resize -s 2G --no-progress quuy
  $ rbd snap create bar@snap2
  $ rbd snap create foo@snap

clone
=====
  $ rbd snap protect bar@snap
  $ rbd clone --image-feature layering,exclusive-lock,object-map,fast-diff bar@snap rbd_other/child
  $ rbd snap create rbd_other/child@snap
  $ rbd flatten rbd_other/child 2> /dev/null
  $ rbd bench-write rbd_other/child --io-pattern seq --io-total 1B > /dev/null 2>&1
  $ rbd clone bar@snap rbd_other/deep-flatten-child
  $ rbd snap create rbd_other/deep-flatten-child@snap
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
  \tsize 1024 MB in 256 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 1 (esc)
  $ rbd info foo --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "name": "foo", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "size": 1073741824
  }
The version of xml_pp included in ubuntu precise always prints a 'warning'
whenever it is run. grep -v to ignore it, but still work on other distros.
  $ rbd info foo --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>foo</name>
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rb.0.*</block_name_prefix> (glob)
    <format>1</format>
  </image>
  $ rbd info foo@snap
  rbd image 'foo':
  \tsize 1024 MB in 256 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 1 (esc)
  \tprotected: False (esc)
  $ rbd info foo@snap --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "name": "foo", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "protected": "false", 
      "size": 1073741824
  }
  $ rbd info foo@snap --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>foo</name>
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rb.0*</block_name_prefix> (glob)
    <format>1</format>
    <protected>false</protected>
  </image>
  $ rbd info bar
  rbd image 'bar':
  \tsize 1024 MB in 256 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \tflags:  (esc)
  $ rbd info bar --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "name": "bar", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "size": 1073741824
  }
  $ rbd info bar --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>bar</name>
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <flags></flags>
  </image>
  $ rbd info bar@snap
  rbd image 'bar':
  \tsize 512 MB in 128 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \tflags:  (esc)
  \tprotected: True (esc)
  $ rbd info bar@snap --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "name": "bar", 
      "object_size": 4194304, 
      "objects": 128, 
      "order": 22, 
      "protected": "true", 
      "size": 536870912
  }
  $ rbd info bar@snap --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>bar</name>
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <flags></flags>
    <protected>true</protected>
  </image>
  $ rbd info bar@snap2
  rbd image 'bar':
  \tsize 1024 MB in 256 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \tflags:  (esc)
  \tprotected: False (esc)
  $ rbd info bar@snap2 --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "name": "bar", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "protected": "false", 
      "size": 1073741824
  }
  $ rbd info bar@snap2 --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>bar</name>
    <size>1073741824</size>
    <objects>256</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <flags></flags>
    <protected>false</protected>
  </image>
  $ rbd info baz
  rbd image 'baz':
  \tsize 2048 MB in 512 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering (esc)
  \tflags:  (esc)
  $ rbd info baz --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering"
      ], 
      "flags": [], 
      "format": 2, 
      "name": "baz", 
      "object_size": 4194304, 
      "objects": 512, 
      "order": 22, 
      "size": 2147483648
  }
  $ rbd info baz --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>baz</name>
    <size>2147483648</size>
    <objects>512</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
    </features>
    <flags></flags>
  </image>
  $ rbd info quux
  rbd image 'quux':
  \tsize 1024 kB in 1 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 1 (esc)
  $ rbd info quux --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "name": "quux", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576
  }
  $ rbd info quux --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>quux</name>
    <size>1048576</size>
    <objects>1</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rb.0.*</block_name_prefix> (glob)
    <format>1</format>
  </image>
  $ rbd info rbd_other/child
  rbd image 'child':
  \tsize 512 MB in 128 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff (esc)
  \tflags:  (esc)
  $ rbd info rbd_other/child --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff"
      ], 
      "flags": [], 
      "format": 2, 
      "name": "child", 
      "object_size": 4194304, 
      "objects": 128, 
      "order": 22, 
      "size": 536870912
  }
  $ rbd info rbd_other/child --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>child</name>
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
    </features>
    <flags></flags>
  </image>
  $ rbd info rbd_other/child@snap
  rbd image 'child':
  \tsize 512 MB in 128 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff (esc)
  \tflags:  (esc)
  \tprotected: False (esc)
  \tparent: rbd/bar@snap (esc)
  \toverlap: 512 MB (esc)
  $ rbd info rbd_other/child@snap --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff"
      ], 
      "flags": [], 
      "format": 2, 
      "name": "child", 
      "object_size": 4194304, 
      "objects": 128, 
      "order": 22, 
      "parent": {
          "image": "bar", 
          "overlap": 536870912, 
          "pool": "rbd", 
          "snapshot": "snap"
      }, 
      "protected": "false", 
      "size": 536870912
  }
  $ rbd info rbd_other/child@snap --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>child</name>
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
    </features>
    <flags></flags>
    <protected>false</protected>
    <parent>
      <pool>rbd</pool>
      <image>bar</image>
      <snapshot>snap</snapshot>
      <overlap>536870912</overlap>
    </parent>
  </image>
  $ rbd info rbd_other/deep-flatten-child
  rbd image 'deep-flatten-child':
  \tsize 512 MB in 128 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \tflags:  (esc)
  $ rbd info rbd_other/deep-flatten-child --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "name": "deep-flatten-child", 
      "object_size": 4194304, 
      "objects": 128, 
      "order": 22, 
      "size": 536870912
  }
  $ rbd info rbd_other/deep-flatten-child --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>deep-flatten-child</name>
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <flags></flags>
  </image>
  $ rbd info rbd_other/deep-flatten-child@snap
  rbd image 'deep-flatten-child':
  \tsize 512 MB in 128 objects (esc)
  \torder 22 (4096 kB objects) (esc)
  [^^]+ (re)
  \tformat: 2 (esc)
  \tfeatures: layering, exclusive-lock, object-map, fast-diff, deep-flatten (esc)
  \tflags:  (esc)
  \tprotected: False (esc)
  $ rbd info rbd_other/deep-flatten-child@snap --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "exclusive-lock", 
          "object-map", 
          "fast-diff", 
          "deep-flatten"
      ], 
      "flags": [], 
      "format": 2, 
      "name": "deep-flatten-child", 
      "object_size": 4194304, 
      "objects": 128, 
      "order": 22, 
      "protected": "false", 
      "size": 536870912
  }
  $ rbd info rbd_other/deep-flatten-child@snap --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <image>
    <name>deep-flatten-child</name>
    <size>536870912</size>
    <objects>128</objects>
    <order>22</order>
    <object_size>4194304</object_size>
    <block_name_prefix>rbd_data.*</block_name_prefix> (glob)
    <format>2</format>
    <features>
      <feature>layering</feature>
      <feature>exclusive-lock</feature>
      <feature>object-map</feature>
      <feature>fast-diff</feature>
      <feature>deep-flatten</feature>
    </features>
    <flags></flags>
    <protected>false</protected>
  </image>
  $ rbd list
  foo
  quux
  bar
  baz
  quuy
  $ rbd list --format json | python -mjson.tool | sed 's/,$/, /'
  [
      "foo", 
      "quux", 
      "bar", 
      "baz", 
      "quuy"
  ]
  $ rbd list --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <images>
    <name>foo</name>
    <name>quux</name>
    <name>bar</name>
    <name>baz</name>
    <name>quuy</name>
  </images>
  $ rbd list -l
  NAME       SIZE PARENT FMT PROT LOCK 
  foo       1024M          1           
  foo@snap  1024M          1           
  quux      1024k          1      excl 
  bar       1024M          2           
  bar@snap   512M          2 yes       
  bar@snap2 1024M          2           
  baz       2048M          2      shr  
  quuy      2048M          2           
  $ rbd list -l --format json | python -mjson.tool | sed 's/,$/, /'
  [
      {
          "format": 1, 
          "image": "foo", 
          "size": 1073741824
      }, 
      {
          "format": 1, 
          "image": "foo", 
          "protected": "false", 
          "size": 1073741824, 
          "snapshot": "snap"
      }, 
      {
          "format": 1, 
          "image": "quux", 
          "lock_type": "exclusive", 
          "size": 1048576
      }, 
      {
          "format": 2, 
          "image": "bar", 
          "size": 1073741824
      }, 
      {
          "format": 2, 
          "image": "bar", 
          "protected": "true", 
          "size": 536870912, 
          "snapshot": "snap"
      }, 
      {
          "format": 2, 
          "image": "bar", 
          "protected": "false", 
          "size": 1073741824, 
          "snapshot": "snap2"
      }, 
      {
          "format": 2, 
          "image": "baz", 
          "lock_type": "shared", 
          "size": 2147483648
      }, 
      {
          "format": 2, 
          "image": "quuy", 
          "size": 2147483648
      }
  ]
  $ rbd list -l --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <images>
    <image>
      <image>foo</image>
      <size>1073741824</size>
      <format>1</format>
    </image>
    <snapshot>
      <image>foo</image>
      <snapshot>snap</snapshot>
      <size>1073741824</size>
      <format>1</format>
      <protected>false</protected>
    </snapshot>
    <image>
      <image>quux</image>
      <size>1048576</size>
      <format>1</format>
      <lock_type>exclusive</lock_type>
    </image>
    <image>
      <image>bar</image>
      <size>1073741824</size>
      <format>2</format>
    </image>
    <snapshot>
      <image>bar</image>
      <snapshot>snap</snapshot>
      <size>536870912</size>
      <format>2</format>
      <protected>true</protected>
    </snapshot>
    <snapshot>
      <image>bar</image>
      <snapshot>snap2</snapshot>
      <size>1073741824</size>
      <format>2</format>
      <protected>false</protected>
    </snapshot>
    <image>
      <image>baz</image>
      <size>2147483648</size>
      <format>2</format>
      <lock_type>shared</lock_type>
    </image>
    <image>
      <image>quuy</image>
      <size>2147483648</size>
      <format>2</format>
    </image>
  </images>
  $ rbd list rbd_other
  child
  deep-flatten-child
  $ rbd list rbd_other --format json | python -mjson.tool | sed 's/,$/, /'
  [
      "child", 
      "deep-flatten-child"
  ]
  $ rbd list rbd_other --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <images>
    <name>child</name>
    <name>deep-flatten-child</name>
  </images>
  $ rbd list rbd_other -l
  NAME                    SIZE PARENT       FMT PROT LOCK 
  child                   512M                2           
  child@snap              512M rbd/bar@snap   2           
  deep-flatten-child      512M                2           
  deep-flatten-child@snap 512M                2           
  $ rbd list rbd_other -l --format json | python -mjson.tool | sed 's/,$/, /'
  [
      {
          "format": 2, 
          "image": "child", 
          "size": 536870912
      }, 
      {
          "format": 2, 
          "image": "child", 
          "parent": {
              "image": "bar", 
              "pool": "rbd", 
              "snapshot": "snap"
          }, 
          "protected": "false", 
          "size": 536870912, 
          "snapshot": "snap"
      }, 
      {
          "format": 2, 
          "image": "deep-flatten-child", 
          "size": 536870912
      }, 
      {
          "format": 2, 
          "image": "deep-flatten-child", 
          "protected": "false", 
          "size": 536870912, 
          "snapshot": "snap"
      }
  ]
  $ rbd list rbd_other -l --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <images>
    <image>
      <image>child</image>
      <size>536870912</size>
      <format>2</format>
    </image>
    <snapshot>
      <image>child</image>
      <snapshot>snap</snapshot>
      <size>536870912</size>
      <parent>
        <pool>rbd</pool>
        <image>bar</image>
        <snapshot>snap</snapshot>
      </parent>
      <format>2</format>
      <protected>false</protected>
    </snapshot>
    <image>
      <image>deep-flatten-child</image>
      <size>536870912</size>
      <format>2</format>
    </image>
    <snapshot>
      <image>deep-flatten-child</image>
      <snapshot>snap</snapshot>
      <size>536870912</size>
      <format>2</format>
      <protected>false</protected>
    </snapshot>
  </images>
  $ rbd lock list foo
  $ rbd lock list foo --format json | python -mjson.tool | sed 's/,$/, /'
  {}
  $ rbd lock list foo --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <locks></locks>
  $ rbd lock list quux
  There is 1 exclusive lock on this image.
  Locker*ID*Address* (glob)
  client.* id * (glob)
  $ rbd lock list quux --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "id": {
          "address": "*",  (glob)
          "locker": "client.*" (glob)
      }
  }
  $ rbd lock list quux --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <locks>
    <id>
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </id>
  </locks>
  $ rbd lock list baz
  There are 3 shared locks on this image.
  Lock tag: tag
  Locker*ID*Address* (glob)
  client.*id[123].* (re)
  client.*id[123].* (re)
  client.*id[123].* (re)
  $ rbd lock list baz --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "id1": {
          "address": "*",  (glob)
          "locker": "client.*" (glob)
      }, 
      "id2": {
          "address": "*",  (glob)
          "locker": "client.*" (glob)
      }, 
      "id3": {
          "address": "*",  (glob)
          "locker": "client.*" (glob)
      }
  }
  $ rbd lock list baz --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <locks>
    <id*> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </id*> (glob)
    <id*> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </id*> (glob)
    <id*> (glob)
      <locker>client.*</locker> (glob)
      <address>*</address> (glob)
    </id*> (glob)
  </locks>
  $ rbd snap list foo
  SNAPID NAME    SIZE 
      *snap*1024*MB* (glob)
  $ rbd snap list foo --format json | python -mjson.tool | sed 's/,$/, /'
  [
      {
          "id": *,  (glob)
          "name": "snap", 
          "size": 1073741824
      }
  ]
  $ rbd snap list foo --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <snapshots>
    <snapshot>
      <id>*</id> (glob)
      <name>snap</name>
      <size>1073741824</size>
    </snapshot>
  </snapshots>
  $ rbd snap list bar
  SNAPID NAME     SIZE 
      *snap*512*MB* (glob)
      *snap2*1024*MB* (glob)
  $ rbd snap list bar --format json | python -mjson.tool | sed 's/,$/, /'
  [
      {
          "id": *,  (glob)
          "name": "snap", 
          "size": 536870912
      }, 
      {
          "id": *,  (glob)
          "name": "snap2", 
          "size": 1073741824
      }
  ]
  $ rbd snap list bar --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <snapshots>
    <snapshot>
      <id>*</id> (glob)
      <name>snap</name>
      <size>536870912</size>
    </snapshot>
    <snapshot>
      <id>*</id> (glob)
      <name>snap2</name>
      <size>1073741824</size>
    </snapshot>
  </snapshots>
  $ rbd snap list baz
  $ rbd snap list baz --format json | python -mjson.tool | sed 's/,$/, /'
  []
  $ rbd snap list baz --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <snapshots></snapshots>
  $ rbd snap list rbd_other/child
  SNAPID NAME   SIZE 
      *snap*512*MB* (glob)
  $ rbd snap list rbd_other/child --format json | python -mjson.tool | sed 's/,$/, /'
  [
      {
          "id": *,  (glob)
          "name": "snap", 
          "size": 536870912
      }
  ]
  $ rbd snap list rbd_other/child --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <snapshots>
    <snapshot>
      <id>*</id> (glob)
      <name>snap</name>
      <size>536870912</size>
    </snapshot>
  </snapshots>
  $ rbd disk-usage --pool rbd_other
  NAME                    PROVISIONED  USED 
  child@snap                     512M     0 
  child                          512M 4096k 
  deep-flatten-child@snap        512M     0 
  deep-flatten-child             512M     0 
  <TOTAL>                       1024M 4096k 
  $ rbd disk-usage --pool rbd_other --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "images": [
          {
              "name": "child", 
              "provisioned_size": 536870912, 
              "snapshot": "snap", 
              "used_size": 0
          }, 
          {
              "name": "child", 
              "provisioned_size": 536870912, 
              "used_size": 4194304
          }, 
          {
              "name": "deep-flatten-child", 
              "provisioned_size": 536870912, 
              "snapshot": "snap", 
              "used_size": 0
          }, 
          {
              "name": "deep-flatten-child", 
              "provisioned_size": 536870912, 
              "used_size": 0
          }
      ], 
      "total_provisioned_size": 1073741824, 
      "total_used_size": 4194304
  }
  $ rbd disk-usage --pool rbd_other --format xml | xml_pp 2>&1 | grep -v '^new version at /usr/bin/xml_pp'
  <stats>
    <images>
      <image>
        <name>child</name>
        <snapshot>snap</snapshot>
        <provisioned_size>536870912</provisioned_size>
        <used_size>0</used_size>
      </image>
      <image>
        <name>child</name>
        <provisioned_size>536870912</provisioned_size>
        <used_size>4194304</used_size>
      </image>
      <image>
        <name>deep-flatten-child</name>
        <snapshot>snap</snapshot>
        <provisioned_size>536870912</provisioned_size>
        <used_size>0</used_size>
      </image>
      <image>
        <name>deep-flatten-child</name>
        <provisioned_size>536870912</provisioned_size>
        <used_size>0</used_size>
      </image>
    </images>
    <total_provisioned_size>1073741824</total_provisioned_size>
    <total_used_size>4194304</total_used_size>
  </stats>

# cleanup
  $ rbd snap remove rbd_other/deep-flatten-child@snap
  $ rbd snap remove rbd_other/child@snap
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
