A command taking no args:

  $ rbd showmapped junk
  rbd: too many arguments
  [1]

A command taking one arg:

  $ rbd info img1 junk
  rbd: too many arguments
  [1]

A command taking two args:

  $ rbd copy img1 img2 junk
  rbd: too many arguments
  [1]

A command taking three args:

  $ rbd lock remove img1 lock1 locker1 junk
  rbd: too many arguments
  [1]

A command taking unlimited args:

  $ rbd feature enable img1 layering striping exclusive-lock object-map fast-diff deep-flatten journaling junk
  rbd: the argument for option is invalid
  [1]

  $ rbd feature disable img1 layering striping exclusive-lock object-map fast-diff deep-flatten journaling junk
  rbd: the argument for option is invalid
  [1]
