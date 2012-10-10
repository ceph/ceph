  $ rbd resize --snap=snap1 img
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd resize img@snap
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd import --snap=snap1 /bin/ls ls
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd create --snap=snap img
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd rm --snap=snap img
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd rename --snap=snap img
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd ls --snap=snap rbd
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd snap ls --snap=snap img
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd watch --snap=snap img
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd lock list --snap=snap img
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd lock add --snap=snap img id
  rbd: snapname specified for a command that doesn't use it
  [1]
  $ rbd lock remove --snap=snap img id client.1234
  rbd: snapname specified for a command that doesn't use it
  [1]
