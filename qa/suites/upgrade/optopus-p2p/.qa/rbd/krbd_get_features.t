
journaling makes the image only unwritable, rather than both unreadable
and unwritable:

  $ rbd create --size 1 --image-feature layering,exclusive-lock,journaling img
  $ rbd snap create img@snap
  $ rbd snap protect img@snap
  $ rbd clone --image-feature layering,exclusive-lock,journaling img@snap cloneimg

  $ DEV=$(sudo rbd map img)
  rbd: sysfs write failed
  rbd: map failed: (6) No such device or address
  [6]
  $ DEV=$(sudo rbd map --read-only img)
  $ blockdev --getro $DEV
  1
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map cloneimg)
  rbd: sysfs write failed
  rbd: map failed: (6) No such device or address
  [6]
  $ DEV=$(sudo rbd map --read-only cloneimg)
  $ blockdev --getro $DEV
  1
  $ sudo rbd unmap $DEV

  $ rbd rm --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img
