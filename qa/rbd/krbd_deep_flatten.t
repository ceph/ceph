
Write:

  $ rbd create --size 12M --image-feature layering,deep-flatten img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite -w 0 12M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd snap create cloneimg@snap
  $ DEV=$(sudo rbd map cloneimg)
  $ xfs_io -c 'pwrite -S 0xab -w 6M 1k' $DEV >/dev/null
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0600000 abab abab abab abab abab abab abab abab
  *
  0600400 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd flatten --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0600000 abab abab abab abab abab abab abab abab
  *
  0600400 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd snap rm --no-progress cloneimg@snap
  $ rbd rm --no-progress cloneimg

Write, whole object:

  $ rbd create --size 12M --image-feature layering,deep-flatten img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite -w 0 12M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd snap create cloneimg@snap
  $ DEV=$(sudo rbd map cloneimg)
  $ xfs_io -d -c 'pwrite -b 4M -S 0xab 4M 4M' $DEV >/dev/null
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000 abab abab abab abab abab abab abab abab
  *
  0800000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd flatten --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000 abab abab abab abab abab abab abab abab
  *
  0800000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd snap rm --no-progress cloneimg@snap
  $ rbd rm --no-progress cloneimg

Zeroout:

  $ rbd create --size 12M --image-feature layering,deep-flatten img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite -w 0 12M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd snap create cloneimg@snap
  $ DEV=$(sudo rbd map cloneimg)
  $ fallocate -z -o 6M -l 1k $DEV
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0600000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0600400 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd flatten --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0600000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0600400 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd snap rm --no-progress cloneimg@snap
  $ rbd rm --no-progress cloneimg

Zeroout, whole object:

  $ rbd create --size 12M --image-feature layering,deep-flatten img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite -w 0 12M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd snap create cloneimg@snap
  $ DEV=$(sudo rbd map cloneimg)
  $ fallocate -z -o 4M -l 4M $DEV
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0800000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd flatten --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0400000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0800000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd snap rm --no-progress cloneimg@snap
  $ rbd rm --no-progress cloneimg

Discard, whole object, empty clone:

  $ rbd create --size 12M --image-feature layering,deep-flatten img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite -w 0 12M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd snap create cloneimg@snap
  $ DEV=$(sudo rbd map cloneimg)
  $ blkdiscard -o 4M -l 4M $DEV
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd flatten --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd snap rm --no-progress cloneimg@snap
  $ rbd rm --no-progress cloneimg

Discard, whole object, full clone:

  $ rbd create --size 12M --image-feature layering,deep-flatten img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite -w 0 12M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd snap create cloneimg@snap
  $ DEV=$(sudo rbd map cloneimg)
  $ xfs_io -c 'pwrite -S 0xab -w 0 12M' $DEV >/dev/null
  $ blkdiscard -o 4M -l 4M $DEV
  $ sudo rbd unmap $DEV

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 abab abab abab abab abab abab abab abab
  *
  0400000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0800000 abab abab abab abab abab abab abab abab
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd flatten --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img

  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 abab abab abab abab abab abab abab abab
  *
  0400000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0800000 abab abab abab abab abab abab abab abab
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg@snap)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0c00000
  $ sudo rbd unmap $DEV

  $ rbd snap rm --no-progress cloneimg@snap
  $ rbd rm --no-progress cloneimg
