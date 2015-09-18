  $ rbd create foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd create [--pool <pool>] [--image <image>] 
                    [--image-format <image-format>] [--new-format] 
                    [--order <order>] [--image-features <image-features>] 
                    [--image-shared] [--stripe-unit <stripe-unit>] 
                    [--stripe-count <stripe-count>] --size <size> 
                    <image-spec> 
  
  Create an empty image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --image-format arg   image format [1 or 2]
    --new-format         use image format 2
                         (deprecated)
    --order arg          object order [12 <= order <= 25]
    --image-features arg image features
                         [layering(+), striping(+), exclusive-lock(*),
                         object-map(*), fast-diff(*), deep-flatten]
    --image-shared       shared image
    --stripe-unit arg    stripe unit
    --stripe-count arg   stripe count
    -s [ --size ] arg    image size (in M/G/T)
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (+) enabled by default for new images if features not specified
  
  [22]
  $ rbd flatten foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd flatten [--pool <pool>] [--image <image>] [--no-progress] 
                     <image-spec> 
  
  Fill clone with parent data (make it independent).
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --no-progress        disable progress output
  
  [22]
  $ rbd resize foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd resize [--pool <pool>] [--image <image>] --size <size> 
                    [--allow-shrink] [--no-progress] 
                    <image-spec> 
  
  Resize (expand or shrink) image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    -s [ --size ] arg    image size (in M/G/T)
    --allow-shrink       permit shrinking
    --no-progress        disable progress output
  
  [22]
  $ rbd rm foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd remove [--pool <pool>] [--image <image>] [--no-progress] 
                    <image-spec> 
  
  Delete an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --no-progress        disable progress output
  
  [22]
  $ rbd import-diff /tmp/diff foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd import-diff [--path <path>] [--pool <pool>] [--image <image>] 
                         [--no-progress] 
                         <path-name> <image-spec> 
  
  Import an incremental diff.
  
  Positional arguments
    <path-name>          import file (or '-' for stdin)
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    --path arg           import file (or '-' for stdin)
    -p [ --pool ] arg    pool name
    --image arg          image name
    --no-progress        disable progress output
  
  [22]
  $ rbd mv foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd rename [--pool <pool>] [--image <image>] [--dest-pool <dest-pool>] 
                    [--dest <dest>] 
                    <source-image-spec> <dest-image-spec> 
  
  Rename image within pool.
  
  Positional arguments
    <source-image-spec>  source image specification
                         (example: [<pool-name>/]<image-name>)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --image arg          source image name
    --dest-pool arg      destination pool name
    --dest arg           destination image name
  
  [22]
  $ rbd mv foo@snap bar
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd rename [--pool <pool>] [--image <image>] [--dest-pool <dest-pool>] 
                    [--dest <dest>] 
                    <source-image-spec> <dest-image-spec> 
  
  Rename image within pool.
  
  Positional arguments
    <source-image-spec>  source image specification
                         (example: [<pool-name>/]<image-name>)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --image arg          source image name
    --dest-pool arg      destination pool name
    --dest arg           destination image name
  
  [22]
  $ rbd mv foo@snap bar@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd rename [--pool <pool>] [--image <image>] [--dest-pool <dest-pool>] 
                    [--dest <dest>] 
                    <source-image-spec> <dest-image-spec> 
  
  Rename image within pool.
  
  Positional arguments
    <source-image-spec>  source image specification
                         (example: [<pool-name>/]<image-name>)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --image arg          source image name
    --dest-pool arg      destination pool name
    --dest arg           destination image name
  
  [22]
  $ rbd image-meta list foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd image-meta list [--pool <pool>] [--image <image>] 
                             [--format <format>] [--pretty-format] 
                             <image-spec> 
  
  Image metadata list keys with values.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  [22]
  $ rbd image-meta get foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd image-meta get [--pool <pool>] [--image <image>] 
                            <image-spec> <key> 
  
  Image metadata get the value associated with the key.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd image-meta get foo@snap key
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd image-meta get [--pool <pool>] [--image <image>] 
                            <image-spec> <key> 
  
  Image metadata get the value associated with the key.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd image-meta set foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd image-meta set [--pool <pool>] [--image <image>] 
                            <image-spec> <key> <value> 
  
  Image metadata set key with value.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
    <value>              image meta value
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd image-meta set foo@snap key
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd image-meta set [--pool <pool>] [--image <image>] 
                            <image-spec> <key> <value> 
  
  Image metadata set key with value.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
    <value>              image meta value
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd image-meta set foo@snap key val
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd image-meta set [--pool <pool>] [--image <image>] 
                            <image-spec> <key> <value> 
  
  Image metadata set key with value.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
    <value>              image meta value
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd image-meta remove foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd image-meta remove [--pool <pool>] [--image <image>] 
                               <image-spec> <key> 
  
  Image metadata remove the key and value associated.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd image-meta remove foo@snap key
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd image-meta remove [--pool <pool>] [--image <image>] 
                               <image-spec> <key> 
  
  Image metadata remove the key and value associated.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <key>                image meta key
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd snap ls foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd snap ls [--pool <pool>] [--image <image>] [--format <format>] 
                     [--pretty-format] 
                     <image-spec> 
  
  Dump list of image snapshots.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  [22]
  $ rbd snap purge foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd snap purge [--pool <pool>] [--image <image>] [--no-progress] 
                        <image-spec> 
  
  Deletes all snapshots.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --no-progress        disable progress output
  
  [22]
  $ rbd watch foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd watch [--pool <pool>] [--image <image>] 
                   <image-spec> 
  
  Watch events on image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd status foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd status [--pool <pool>] [--image <image>] [--format <format>] 
                    [--pretty-format] 
                    <image-spec> 
  
  Show the status of this image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  [22]
  $ rbd feature disable foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd feature disable [--pool <pool>] [--image <image>] 
                             <image-spec> <features> [<features> ...]
  
  Disable the specified image feature.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <features>           image features
                         [layering, striping, exclusive-lock, object-map,
                         fast-diff, deep-flatten]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd feature disable foo@snap layering
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd feature disable [--pool <pool>] [--image <image>] 
                             <image-spec> <features> [<features> ...]
  
  Disable the specified image feature.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <features>           image features
                         [layering, striping, exclusive-lock, object-map,
                         fast-diff, deep-flatten]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd feature enable foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd feature enable [--pool <pool>] [--image <image>] 
                            <image-spec> <features> [<features> ...]
  
  Enable the specified image feature.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <features>           image features
                         [layering, striping, exclusive-lock, object-map,
                         fast-diff, deep-flatten]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd feature enable foo@snap layering
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd feature enable [--pool <pool>] [--image <image>] 
                            <image-spec> <features> [<features> ...]
  
  Enable the specified image feature.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <features>           image features
                         [layering, striping, exclusive-lock, object-map,
                         fast-diff, deep-flatten]
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd lock list foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd lock list [--pool <pool>] [--image <image>] [--format <format>] 
                       [--pretty-format] 
                       <image-spec> 
  
  Show locks held on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  [22]
  $ rbd lock add foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd lock add [--pool <pool>] [--image <image>] [--shared <shared>] 
                      <image-spec> <lock-id> 
  
  Take a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <lock-id>            unique lock id
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --shared arg         shared lock tag
  
  [22]
  $ rbd lock add foo@snap id
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd lock add [--pool <pool>] [--image <image>] [--shared <shared>] 
                      <image-spec> <lock-id> 
  
  Take a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <lock-id>            unique lock id
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --shared arg         shared lock tag
  
  [22]
  $ rbd lock remove foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd lock remove [--pool <pool>] [--image <image>] 
                         <image-spec> <lock-id> <locker> 
  
  Release a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <lock-id>            unique lock id
    <locker>             locker client
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd lock remove foo@snap id
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd lock remove [--pool <pool>] [--image <image>] 
                         <image-spec> <lock-id> <locker> 
  
  Release a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <lock-id>            unique lock id
    <locker>             locker client
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd lock remove foo@snap id client.1234
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd lock remove [--pool <pool>] [--image <image>] 
                         <image-spec> <lock-id> <locker> 
  
  Release a lock on an image.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
    <lock-id>            unique lock id
    <locker>             locker client
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
  
  [22]
  $ rbd bench-write foo@snap
  rbd: snapname specified for a command that doesn't use it
  
  usage: rbd bench-write [--pool <pool>] [--image <image>] [--io-size <io-size>] 
                         [--io-threads <io-threads>] [--io-total <io-total>] 
                         [--io-pattern <io-pattern>] 
                         <image-spec> 
  
  Simple write benchmark.
  
  Positional arguments
    <image-spec>         image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --io-size arg        write size (in B/K/M/G/T)
    --io-threads arg     ios in flight
    --io-total arg       total size to write (in B/K/M/G/T)
    --io-pattern arg     write pattern (rand or seq)
  
  [22]

  $ rbd clone foo@snap bar@snap
  rbd: destination snapname specified for a command that doesn't use it
  
  usage: rbd clone [--pool <pool>] [--image <image>] [--snap <snap>] 
                   [--dest-pool <dest-pool>] [--dest <dest>] [--order <order>] 
                   [--image-features <image-features>] [--image-shared] 
                   [--stripe-unit <stripe-unit>] [--stripe-count <stripe-count>] 
                   <source-snap-spec> <dest-image-spec> 
  
  Clone a snapshot into a COW child image.
  
  Positional arguments
    <source-snap-spec>   source snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --image arg          source image name
    --snap arg           source snapshot name
    --dest-pool arg      destination pool name
    --dest arg           destination image name
    --order arg          object order [12 <= order <= 25]
    --image-features arg image features
                         [layering(+), striping(+), exclusive-lock(*),
                         object-map(*), fast-diff(*), deep-flatten]
    --image-shared       shared image
    --stripe-unit arg    stripe unit
    --stripe-count arg   stripe count
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (+) enabled by default for new images if features not specified
  
  [22]
  $ rbd import /bin/ls ls@snap
  rbd: destination snapname specified for a command that doesn't use it
  
  usage: rbd import [--path <path>] [--dest-pool <dest-pool>] [--dest <dest>] 
                    [--image-format <image-format>] [--new-format] 
                    [--order <order>] [--image-features <image-features>] 
                    [--image-shared] [--stripe-unit <stripe-unit>] 
                    [--stripe-count <stripe-count>] [--no-progress] 
                    [--pool <pool>] [--image <image>] 
                    <path-name> <dest-image-spec> 
  
  Import image from file.
  
  Positional arguments
    <path-name>          import file (or '-' for stdin)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    --path arg           import file (or '-' for stdin)
    --dest-pool arg      destination pool name
    --dest arg           destination image name
    --image-format arg   image format [1 or 2]
    --new-format         use image format 2
                         (deprecated)
    --order arg          object order [12 <= order <= 25]
    --image-features arg image features
                         [layering(+), striping(+), exclusive-lock(*),
                         object-map(*), fast-diff(*), deep-flatten]
    --image-shared       shared image
    --stripe-unit arg    stripe unit
    --stripe-count arg   stripe count
    --no-progress        disable progress output
    -p [ --pool ] arg    pool name (deprecated)
    --image arg          image name (deprecated)
  
  Image Features:
    (*) supports enabling/disabling on existing images
    (+) enabled by default for new images if features not specified
  
  [22]
  $ rbd cp foo bar@snap
  rbd: destination snapname specified for a command that doesn't use it
  
  usage: rbd copy [--pool <pool>] [--image <image>] [--snap <snap>] 
                  [--dest-pool <dest-pool>] [--dest <dest>] [--no-progress] 
                  <source-image-or-snap-spec> <dest-image-spec> 
  
  Copy src image to dest.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <dest-image-spec>            destination image specification
                                 (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --dest-pool arg              destination pool name
    --dest arg                   destination image name
    --no-progress                disable progress output
  
  [22]
  $ rbd cp foo@snap bar@snap
  rbd: destination snapname specified for a command that doesn't use it
  
  usage: rbd copy [--pool <pool>] [--image <image>] [--snap <snap>] 
                  [--dest-pool <dest-pool>] [--dest <dest>] [--no-progress] 
                  <source-image-or-snap-spec> <dest-image-spec> 
  
  Copy src image to dest.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <dest-image-spec>            destination image specification
                                 (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --dest-pool arg              destination pool name
    --dest arg                   destination image name
    --no-progress                disable progress output
  
  [22]
  $ rbd mv foo bar@snap
  rbd: destination snapname specified for a command that doesn't use it
  
  usage: rbd rename [--pool <pool>] [--image <image>] [--dest-pool <dest-pool>] 
                    [--dest <dest>] 
                    <source-image-spec> <dest-image-spec> 
  
  Rename image within pool.
  
  Positional arguments
    <source-image-spec>  source image specification
                         (example: [<pool-name>/]<image-name>)
    <dest-image-spec>    destination image specification
                         (example: [<pool-name>/]<image-name>)
  
  Optional arguments
    -p [ --pool ] arg    source pool name
    --image arg          source image name
    --dest-pool arg      destination pool name
    --dest arg           destination image name
  
  [22]
