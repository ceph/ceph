  $ rbd map
  rbd: image name was not specified
  [1]
  $ rbd unmap
  rbd: device path was not specified
  [1]
  $ rbd clone foo@snap bar@snap
  rbd: cannot clone to a snapshot
  [1]
  $ rbd cp foo
  rbd: destination image name was not specified
  [1]
  $ rbd cp foo@bar
  rbd: destination image name was not specified
  [1]
  $ rbd copy foo
  rbd: destination image name was not specified
  [1]
  $ rbd copy foo@bar
  rbd: destination image name was not specified
  [1]
  $ rbd mv foo
  rbd: destination image name was not specified
  [1]
  $ rbd rename foo
  rbd: destination image name was not specified
  [1]
  $ rbd clone foo@bar
  rbd: destination image name was not specified
  [1]
  $ rbd clone foo
  rbd: snap name was not specified
  [1]
