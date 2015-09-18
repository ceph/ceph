  $ rbd info
  rbd: image name was not specified
  
  usage: rbd info [--pool <pool>] [--image <image>] [--snap <snap>] 
                  [--format <format>] [--pretty-format] 
                  <image-or-snap-spec> 
  
  Show information about image size, striping, etc.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    --format arg          output format [plain, json, or xml]
    --pretty-format       pretty formatting (json and xml)
  
  [22]
  $ rbd create
  rbd: image name was not specified
  
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
  $ rbd clone
  rbd: image name was not specified
  
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
  $ rbd clone foo
  rbd: snap name was not specified
  
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
  $ rbd clone foo@snap
  rbd: destination image name was not specified
  
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
  $ rbd clone foo bar
  rbd: snap name was not specified
  
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
  $ rbd clone foo bar@snap
  rbd: snap name was not specified
  
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
  $ rbd children
  rbd: image name was not specified
  
  usage: rbd children [--pool <pool>] [--image <image>] [--snap <snap>] 
                      [--format <format>] [--pretty-format] 
                      <snap-spec> 
  
  Display children of snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  [22]
  $ rbd children foo
  rbd: snap name was not specified
  
  usage: rbd children [--pool <pool>] [--image <image>] [--snap <snap>] 
                      [--format <format>] [--pretty-format] 
                      <snap-spec> 
  
  Display children of snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
    --format arg         output format [plain, json, or xml]
    --pretty-format      pretty formatting (json and xml)
  
  [22]
  $ rbd flatten
  rbd: image name was not specified
  
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
  $ rbd resize
  rbd: image name was not specified
  
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
  $ rbd rm
  rbd: image name was not specified
  
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
  $ rbd export
  rbd: image name was not specified
  
  usage: rbd export [--pool <pool>] [--image <image>] [--snap <snap>] 
                    [--path <path>] [--no-progress] 
                    <source-image-or-snap-spec> <path-name> 
  
  Export image to file.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <path-name>                  export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --path arg                   export file (or '-' for stdout)
    --no-progress                disable progress output
  
  [22]
  $ rbd import
  rbd: path was not specified
  
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
  $ rbd diff
  rbd: image name was not specified
  
  usage: rbd diff [--pool <pool>] [--image <image>] [--snap <snap>] 
                  [--from-snap <from-snap>] [--whole-object] [--format <format>] 
                  [--pretty-format] 
                  <image-or-snap-spec> 
  
  Print extents that differ since a previous snap, or image creation.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    --from-snap arg       snapshot starting point
    --whole-object        compare whole object
    --format arg          output format [plain, json, or xml]
    --pretty-format       pretty formatting (json and xml)
  
  [22]
  $ rbd export-diff
  rbd: image name was not specified
  
  usage: rbd export-diff [--pool <pool>] [--image <image>] [--snap <snap>] 
                         [--path <path>] [--from-snap <from-snap>] 
                         [--whole-object] [--no-progress] 
                         <source-image-or-snap-spec> <path-name> 
  
  Export incremental diff to file.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <path-name>                  export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --path arg                   export file (or '-' for stdout)
    --from-snap arg              snapshot starting point
    --whole-object               compare whole object
    --no-progress                disable progress output
  
  [22]
  $ rbd export-diff foo
  rbd: path was not specified
  
  usage: rbd export-diff [--pool <pool>] [--image <image>] [--snap <snap>] 
                         [--path <path>] [--from-snap <from-snap>] 
                         [--whole-object] [--no-progress] 
                         <source-image-or-snap-spec> <path-name> 
  
  Export incremental diff to file.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <path-name>                  export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --path arg                   export file (or '-' for stdout)
    --from-snap arg              snapshot starting point
    --whole-object               compare whole object
    --no-progress                disable progress output
  
  [22]
  $ rbd export-diff foo@snap
  rbd: path was not specified
  
  usage: rbd export-diff [--pool <pool>] [--image <image>] [--snap <snap>] 
                         [--path <path>] [--from-snap <from-snap>] 
                         [--whole-object] [--no-progress] 
                         <source-image-or-snap-spec> <path-name> 
  
  Export incremental diff to file.
  
  Positional arguments
    <source-image-or-snap-spec>  source image or snapshot specification
                                 (example:
                                 [<pool-name>/]<image-name>[@<snap-name>])
    <path-name>                  export file (or '-' for stdout)
  
  Optional arguments
    -p [ --pool ] arg            source pool name
    --image arg                  source image name
    --snap arg                   source snapshot name
    --path arg                   export file (or '-' for stdout)
    --from-snap arg              snapshot starting point
    --whole-object               compare whole object
    --no-progress                disable progress output
  
  [22]
  $ rbd merge-diff
  rbd: first diff was not specified
  
  usage: rbd merge-diff [--path <path>] [--no-progress] 
                        <diff1-path> <diff2-path> <path-name> 
  
  Merge two diff exports together.
  
  Positional arguments
    <diff1-path>         path to first diff (or '-' for stdin)
    <diff2-path>         path to second diff
    <path-name>          path to merged diff (or '-' for stdout)
  
  Optional arguments
    --path arg           path to merged diff (or '-' for stdout)
    --no-progress        disable progress output
  
  [22]
  $ rbd merge-diff /tmp/diff1
  rbd: second diff was not specified
  
  usage: rbd merge-diff [--path <path>] [--no-progress] 
                        <diff1-path> <diff2-path> <path-name> 
  
  Merge two diff exports together.
  
  Positional arguments
    <diff1-path>         path to first diff (or '-' for stdin)
    <diff2-path>         path to second diff
    <path-name>          path to merged diff (or '-' for stdout)
  
  Optional arguments
    --path arg           path to merged diff (or '-' for stdout)
    --no-progress        disable progress output
  
  [22]
  $ rbd merge-diff /tmp/diff1 /tmp/diff2
  rbd: path was not specified
  
  usage: rbd merge-diff [--path <path>] [--no-progress] 
                        <diff1-path> <diff2-path> <path-name> 
  
  Merge two diff exports together.
  
  Positional arguments
    <diff1-path>         path to first diff (or '-' for stdin)
    <diff2-path>         path to second diff
    <path-name>          path to merged diff (or '-' for stdout)
  
  Optional arguments
    --path arg           path to merged diff (or '-' for stdout)
    --no-progress        disable progress output
  
  [22]
  $ rbd import-diff
  rbd: path was not specified
  
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
  $ rbd import-diff /tmp/diff
  rbd: image name was not specified
  
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
  $ rbd cp
  rbd: image name was not specified
  
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
  $ rbd cp foo
  rbd: destination image name was not specified
  
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
  $ rbd cp foo@snap
  rbd: destination image name was not specified
  
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
  $ rbd mv
  rbd: image name was not specified
  
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
  $ rbd mv foo
  rbd: destination image name was not specified
  
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
  $ rbd image-meta list
  rbd: image name was not specified
  
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
  $ rbd image-meta get
  rbd: image name was not specified
  
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
  $ rbd image-meta get foo
  rbd: metadata key was not specified
  
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
  $ rbd image-meta set
  rbd: image name was not specified
  
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
  $ rbd image-meta set foo
  rbd: metadata key was not specified
  
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
  $ rbd image-meta set foo key
  rbd: metadata value was not specified
  
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
  $ rbd image-meta remove
  rbd: image name was not specified
  
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
  $ rbd image-meta remove foo
  rbd: metadata key was not specified
  
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
  $ rbd object-map rebuild
  rbd: image name was not specified
  
  usage: rbd object-map rebuild [--pool <pool>] [--image <image>] 
                                [--snap <snap>] [--no-progress] 
                                <image-or-snap-spec> 
  
  Rebuild an invalid object map.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    --no-progress         disable progress output
  
  [22]
  $ rbd snap ls
  rbd: image name was not specified
  
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
  $ rbd snap create
  rbd: image name was not specified
  
  usage: rbd snap create [--pool <pool>] [--image <image>] [--snap <snap>] 
                         <snap-spec> 
  
  Create a snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  [22]
  $ rbd snap create foo
  rbd: snap name was not specified
  
  usage: rbd snap create [--pool <pool>] [--image <image>] [--snap <snap>] 
                         <snap-spec> 
  
  Create a snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  [22]
  $ rbd snap rollback
  rbd: image name was not specified
  
  usage: rbd snap rollback [--pool <pool>] [--image <image>] [--snap <snap>] 
                           [--no-progress] 
                           <snap-spec> 
  
  Rollback image to snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
    --no-progress        disable progress output
  
  [22]
  $ rbd snap rollback foo
  rbd: snap name was not specified
  
  usage: rbd snap rollback [--pool <pool>] [--image <image>] [--snap <snap>] 
                           [--no-progress] 
                           <snap-spec> 
  
  Rollback image to snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
    --no-progress        disable progress output
  
  [22]
  $ rbd snap rm
  rbd: image name was not specified
  
  usage: rbd snap remove [--pool <pool>] [--image <image>] [--snap <snap>] 
                         <snap-spec> 
  
  Deletes a snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  [22]
  $ rbd snap rm foo
  rbd: snap name was not specified
  
  usage: rbd snap remove [--pool <pool>] [--image <image>] [--snap <snap>] 
                         <snap-spec> 
  
  Deletes a snapshot.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  [22]
  $ rbd snap purge
  rbd: image name was not specified
  
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
  $ rbd snap protect
  rbd: image name was not specified
  
  usage: rbd snap protect [--pool <pool>] [--image <image>] [--snap <snap>] 
                          <snap-spec> 
  
  Prevent a snapshot from being deleted.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  [22]
  $ rbd snap protect foo
  rbd: snap name was not specified
  
  usage: rbd snap protect [--pool <pool>] [--image <image>] [--snap <snap>] 
                          <snap-spec> 
  
  Prevent a snapshot from being deleted.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  [22]
  $ rbd snap unprotect
  rbd: image name was not specified
  
  usage: rbd snap unprotect [--pool <pool>] [--image <image>] [--snap <snap>] 
                            <snap-spec> 
  
  Allow a snapshot to be deleted.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  [22]
  $ rbd snap unprotect foo
  rbd: snap name was not specified
  
  usage: rbd snap unprotect [--pool <pool>] [--image <image>] [--snap <snap>] 
                            <snap-spec> 
  
  Allow a snapshot to be deleted.
  
  Positional arguments
    <snap-spec>          snapshot specification
                         (example: [<pool-name>/]<image-name>@<snapshot-name>)
  
  Optional arguments
    -p [ --pool ] arg    pool name
    --image arg          image name
    --snap arg           snapshot name
  
  [22]
  $ rbd watch
  rbd: image name was not specified
  
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
  $ rbd status
  rbd: image name was not specified
  
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
  $ rbd map
  rbd: image name was not specified
  
  usage: rbd map [--pool <pool>] [--image <image>] [--snap <snap>] 
                 [--options <options>] [--read-only] 
                 <image-or-snap-spec> 
  
  Map image to a block device using the kernel.
  
  Positional arguments
    <image-or-snap-spec>  image or snapshot specification
                          (example: [<pool-name>/]<image-name>[@<snap-name>])
  
  Optional arguments
    -p [ --pool ] arg     pool name
    --image arg           image name
    --snap arg            snapshot name
    -o [ --options ] arg  mapping options
    --read-only           mount read-only
  
  [22]
  $ rbd unmap
  rbd: unmap requires either image name or device path
  
  usage: rbd unmap [--pool <pool>] [--image <image>] [--snap <snap>] 
                   <image-or-snap-or-device-spec> 
  
  Unmap a rbd device that was used by the kernel.
  
  Positional arguments
    <image-or-snap-or-device-spec>  image, snapshot, or device specification
                                    [<pool-name>/]<image-name>[@<snapshot-name>]
                                    or <device-path>
  
  Optional arguments
    -p [ --pool ] arg               pool name
    --image arg                     image name
    --snap arg                      snapshot name
  
  [22]
  $ rbd feature disable
  rbd: image name was not specified
  
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
  $ rbd feature disable foo
  rbd: at least one feature name must be specified
  
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
  $ rbd feature enable
  rbd: image name was not specified
  
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
  $ rbd feature enable foo
  rbd: at least one feature name must be specified
  
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
  $ rbd lock list
  rbd: image name was not specified
  
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
  $ rbd lock add
  rbd: image name was not specified
  
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
  $ rbd lock add foo
  rbd: lock id was not specified
  
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
  $ rbd lock remove
  rbd: image name was not specified
  
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
  $ rbd lock remove foo
  rbd: lock id was not specified
  
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
  $ rbd lock remove foo id
  rbd: locker was not specified
  
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
  $ rbd bench-write
  rbd: image name was not specified
  
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
