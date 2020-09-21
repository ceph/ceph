
  $ rbd create --size 20M img

  $ DEV=$(sudo rbd map img)
  $ blockdev --getiomin $DEV
  65536
  $ blockdev --getioopt $DEV
  65536
  $ cat /sys/block/${DEV#/dev/}/queue/discard_granularity
  65536
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map -o alloc_size=512 img)
  $ blockdev --getiomin $DEV
  512
  $ blockdev --getioopt $DEV
  512
  $ cat /sys/block/${DEV#/dev/}/queue/discard_granularity
  512
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map -o alloc_size=4194304 img)
  $ blockdev --getiomin $DEV
  4194304
  $ blockdev --getioopt $DEV
  4194304
  $ cat /sys/block/${DEV#/dev/}/queue/discard_granularity
  4194304
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map -o alloc_size=8388608 img)
  $ blockdev --getiomin $DEV
  4194304
  $ blockdev --getioopt $DEV
  4194304
  $ cat /sys/block/${DEV#/dev/}/queue/discard_granularity
  4194304
  $ sudo rbd unmap $DEV

  $ rbd rm --no-progress img
