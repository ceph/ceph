Plain create with various options specified via usual cli arguments
===================================================================
  $ rbd create -s 1 test
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 --object-size 1M test
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "name": "test", 
      "object_size": 1048576, 
      "objects": 1, 
      "order": 20, 
      "size": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1G --object-size 4K test
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "name": "test", 
      "object_size": 4096, 
      "objects": 262144, 
      "order": 12, 
      "size": 1073741824
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --image-format 2
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1G test --image-format 2
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "size": 1073741824
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --image-format 2 --object-size 1M
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 1048576, 
      "objects": 1, 
      "order": 20, 
      "size": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --image-format 2 --stripe-unit 1048576 --stripe-count 8
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576, 
      "stripe_count": 8, 
      "stripe_unit": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --image-format 2 --stripe-unit 1048576B --stripe-count 8
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576, 
      "stripe_count": 8, 
      "stripe_unit": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1G test --image-format 2 --stripe-unit 4K --stripe-count 8
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "size": 1073741824, 
      "stripe_count": 8, 
      "stripe_unit": 4096
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1G test --image-format 2 --stripe-unit 1M --stripe-count 8
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 256, 
      "order": 22, 
      "size": 1073741824, 
      "stripe_count": 8, 
      "stripe_unit": 1048576
  }
  $ rbd rm test --no-progress

Format 2 Usual arguments with custom rbd_default_* params
=========================================================
  $ rbd create -s 1 test --image-format 2 --stripe-unit 1048576 --stripe-count 8 --rbd-default-order 21
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 2097152, 
      "objects": 1, 
      "order": 21, 
      "size": 1048576, 
      "stripe_count": 8, 
      "stripe_unit": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --image-format 2 --stripe-unit 1048576 --stripe-count 8 --object-size 8M --rbd-default-order 20
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 8388608, 
      "objects": 1, 
      "order": 23, 
      "size": 1048576, 
      "stripe_count": 8, 
      "stripe_unit": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --image-format 2 --rbd-default-stripe-unit 1048576 --rbd-default-stripe-count 8
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576, 
      "stripe_count": 8, 
      "stripe_unit": 1048576
  }
  $ rbd rm test --no-progress

Format 1 Usual arguments with custom rbd_default_* params
=========================================================
  $ rbd create -s 1 test --rbd-default-order 20
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rb.0.*",  (glob)
      "format": 1, 
      "name": "test", 
      "object_size": 1048576, 
      "objects": 1, 
      "order": 20, 
      "size": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --rbd-default-format 2
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --rbd-default-format 2 --rbd-default-order 20
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 1048576, 
      "objects": 1, 
      "order": 20, 
      "size": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --rbd-default-format 2 --rbd-default-order 20 --rbd-default-features 1
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 1048576, 
      "objects": 1, 
      "order": 20, 
      "size": 1048576
  }
  $ rbd rm test --no-progress
  $ rbd create -s 1 test --rbd-default-format 2 --stripe-unit 1048576 --stripe-count 8
  $ rbd info test --format json | python -mjson.tool | sed 's/,$/, /'
  {
      "block_name_prefix": "rbd_data.*",  (glob)
      "features": [
          "layering", 
          "striping", 
          "exclusive"
      ], 
      "format": 2, 
      "name": "test", 
      "object_size": 4194304, 
      "objects": 1, 
      "order": 22, 
      "size": 1048576, 
      "stripe_count": 8, 
      "stripe_unit": 1048576
  }
  $ rbd rm test --no-progress
