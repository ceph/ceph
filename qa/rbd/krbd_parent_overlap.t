
For reads, only the object extent needs to be reverse mapped:

  $ rbd create --size 5M img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite 0 5M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create --no-progress img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd resize --no-progress --size 12M cloneimg
  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0500000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0c00000
  $ dd if=$DEV iflag=direct bs=4M status=none | hexdump
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0500000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0c00000
  $ sudo rbd unmap $DEV
  $ rbd rm --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img

For writes, the entire object needs to be reverse mapped:

  $ rbd create --size 2M img
  $ DEV=$(sudo rbd map img)
  $ xfs_io -c 'pwrite 0 1M' $DEV >/dev/null
  $ sudo rbd unmap $DEV
  $ rbd snap create --no-progress img@snap
  $ rbd snap protect img@snap
  $ rbd clone img@snap cloneimg
  $ rbd resize --no-progress --size 8M cloneimg
  $ DEV=$(sudo rbd map cloneimg)
  $ xfs_io -c 'pwrite -S 0xef 3M 1M' $DEV >/dev/null
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0100000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0300000 efef efef efef efef efef efef efef efef
  *
  0400000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0800000
  $ sudo rbd unmap $DEV
  $ DEV=$(sudo rbd map cloneimg)
  $ hexdump $DEV
  0000000 cdcd cdcd cdcd cdcd cdcd cdcd cdcd cdcd
  *
  0100000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0300000 efef efef efef efef efef efef efef efef
  *
  0400000 0000 0000 0000 0000 0000 0000 0000 0000
  *
  0800000
  $ sudo rbd unmap $DEV
  $ rbd rm --no-progress cloneimg
  $ rbd snap unprotect img@snap
  $ rbd snap rm --no-progress img@snap
  $ rbd rm --no-progress img
