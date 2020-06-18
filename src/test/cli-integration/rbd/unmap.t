
Setup
=====

  $ rbd create --size 1 img
  $ rbd snap create img@snap --no-progress
  $ rbd create --size 1 anotherimg
  $ ceph osd pool create custom >/dev/null 2>&1
  $ rbd pool init custom
  $ rbd create --size 1 custom/img
  $ rbd snap create custom/img@snap --no-progress
  $ rbd snap create custom/img@anothersnap --no-progress

Spell out device instead of using $DEV - sfdisk is not a joke.

  $ DEV=$(sudo rbd device map img)
  $ cat <<EOF | sudo sfdisk /dev/rbd[01] >/dev/null 2>&1
  > unit: sectors
  > /dev/rbd0p1 : start=        2, size=        2, Id=83
  > /dev/rbd0p2 : start=        5, size=     2043, Id= 5
  > /dev/rbd0p3 : start=        0, size=        0, Id= 0
  > /dev/rbd0p4 : start=        0, size=        0, Id= 0
  > /dev/rbd0p5 : start=        7, size=        2, Id=83
  > EOF


Unmap by device
===============

Unmap by device (img is already mapped):

  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap $DEV
  $ rbd device list

Unmap by device partition:

  $ DEV=$(sudo rbd device map img)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap ${DEV}p1
  $ rbd device list

  $ DEV=$(sudo rbd device map img)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap ${DEV}p5
  $ rbd device list

Not a block device - random junk prefixed with /dev/ (so it's not
interpreted as a spec):

  $ sudo rbd device unmap /dev/foobar
  rbd: '/dev/foobar' is not a block device
  rbd: unmap failed: (22) Invalid argument
  [22]

Not a block device - device that's just been unmapped:

  $ DEV=$(sudo rbd device map img)
  $ sudo rbd device unmap $DEV
  $ sudo rbd device unmap $DEV
  rbd: '/dev/rbd?' is not a block device (glob)
  rbd: unmap failed: (22) Invalid argument
  [22]

A block device, but not rbd:

  $ sudo rbd device unmap /dev/[sv]da
  rbd: '/dev/?da' is not an rbd device (glob)
  rbd: unmap failed: (22) Invalid argument
  [22]


Unmap by spec
=============

img:

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap img
  $ rbd device list

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd --image img device unmap
  $ rbd device list

img@snap:

  $ sudo rbd device map img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap img@snap
  $ rbd device list

  $ sudo rbd device map img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd --snap snap device unmap img
  $ rbd device list

  $ sudo rbd device map img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd --image img --snap snap device unmap
  $ rbd device list

pool/img@snap, default pool:

  $ sudo rbd device map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap rbd/img@snap
  $ rbd device list

  $ sudo rbd device map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd --pool rbd device unmap img@snap
  $ rbd device list

  $ sudo rbd device map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd --pool rbd --snap snap device unmap img
  $ rbd device list

  $ sudo rbd device map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd --pool rbd --image img --snap snap device unmap
  $ rbd device list

pool/img@snap, custom pool:

  $ sudo rbd device map custom/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   custom             img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img@snap
  $ rbd device list

  $ sudo rbd device map custom/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   custom             img    snap  /dev/rbd? (glob)
  $ sudo rbd --pool custom device unmap img@snap
  $ rbd device list

  $ sudo rbd device map custom/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   custom             img    snap  /dev/rbd? (glob)
  $ sudo rbd --pool custom --snap snap device unmap img
  $ rbd device list

  $ sudo rbd device map custom/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   custom             img    snap  /dev/rbd? (glob)
  $ sudo rbd --pool custom --image img --snap snap device unmap
  $ rbd device list

Not a mapped spec - random junk (which gets interpreted as a spec):

  $ sudo rbd device unmap foobar
  rbd: rbd/foobar: not a mapped image or snapshot
  rbd: unmap failed: (22) Invalid argument
  [22]

  $ sudo rbd --image foobar device unmap
  rbd: rbd/foobar: not a mapped image or snapshot
  rbd: unmap failed: (22) Invalid argument
  [22]

Not a mapped spec - spec that's just been unmapped:

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ sudo rbd device unmap img
  $ sudo rbd device unmap img
  rbd: rbd/img: not a mapped image or snapshot
  rbd: unmap failed: (22) Invalid argument
  [22]

  $ sudo rbd device map img@snap
  /dev/rbd? (glob)
  $ sudo rbd device unmap img@snap
  $ sudo rbd device unmap img@snap
  rbd: rbd/img@snap: not a mapped image or snapshot
  rbd: unmap failed: (22) Invalid argument
  [22]

Need an arg:

  $ sudo rbd device unmap
  rbd: unmap requires either image name or device path
  [22]


Two images
==========

Unmap img first:

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ sudo rbd device map anotherimg
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image       snap  device   
  ?   rbd              img         -     /dev/rbd? (glob)
  ?   rbd              anotherimg  -     /dev/rbd? (glob)
  $ sudo rbd device unmap img
  $ rbd device list
  id  pool  namespace  image       snap  device   
  ?   rbd              anotherimg  -     /dev/rbd? (glob)
  $ sudo rbd device unmap anotherimg
  $ rbd device list

Unmap anotherimg first:

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ sudo rbd device map anotherimg
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image       snap  device   
  ?   rbd              img         -     /dev/rbd? (glob)
  ?   rbd              anotherimg  -     /dev/rbd? (glob)
  $ sudo rbd device unmap anotherimg
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap img
  $ rbd device list


Image and its snap
==================

Unmap the image first:

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ sudo rbd device map img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap img
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap img@snap
  $ rbd device list

Unmap the snap first:

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ sudo rbd device map img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap img@snap
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap img
  $ rbd device list


Two snaps of the same image
===========================

Unmap snap first:

  $ sudo rbd device map custom/img@snap
  /dev/rbd? (glob)
  $ sudo rbd device map custom/img@anothersnap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap         device   
  ?   custom             img    snap         /dev/rbd? (glob)
  ?   custom             img    anothersnap  /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img@snap
  $ rbd device list
  id  pool    namespace  image  snap         device   
  ?   custom             img    anothersnap  /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img@anothersnap
  $ rbd device list

Unmap anothersnap first:

  $ sudo rbd device map custom/img@snap
  /dev/rbd? (glob)
  $ sudo rbd device map custom/img@anothersnap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap         device   
  ?   custom             img    snap         /dev/rbd? (glob)
  ?   custom             img    anothersnap  /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img@anothersnap
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   custom             img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img@snap
  $ rbd device list


Same img and snap in different pools
====================================

img:

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ sudo rbd device map custom/img
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   rbd                img    -     /dev/rbd? (glob)
  ?   custom             img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap img
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   custom             img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img
  $ rbd device list

img@snap:

  $ sudo rbd device map img@snap
  /dev/rbd? (glob)
  $ sudo rbd device map custom/img@snap
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   rbd                img    snap  /dev/rbd? (glob)
  ?   custom             img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img@snap
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap img@snap
  $ rbd device list


Same spec mapped twice
======================

img:

  $ sudo rbd device map img
  /dev/rbd? (glob)
  $ sudo rbd device map img
  rbd: warning: image already mapped as /dev/rbd? (glob)
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap img
  rbd: rbd/img: mapped more than once, unmapping /dev/rbd? only (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    -     /dev/rbd? (glob)
  $ sudo rbd device unmap img
  $ rbd device list

img@snap:

  $ sudo rbd device map img@snap
  /dev/rbd? (glob)
  $ sudo rbd device map img@snap
  rbd: warning: image already mapped as /dev/rbd? (glob)
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap img@snap
  rbd: rbd/img@snap: mapped more than once, unmapping /dev/rbd? only (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap img@snap
  $ rbd device list

pool/img@snap, default pool:

  $ sudo rbd device map rbd/img@snap
  /dev/rbd? (glob)
  $ sudo rbd device map rbd/img@snap
  rbd: warning: image already mapped as /dev/rbd? (glob)
  /dev/rbd? (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap rbd/img@snap
  rbd: rbd/img@snap: mapped more than once, unmapping /dev/rbd? only (glob)
  $ rbd device list
  id  pool  namespace  image  snap  device   
  ?   rbd              img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap rbd/img@snap
  $ rbd device list

pool/img@snap, custom pool:

  $ sudo rbd device map custom/img@snap
  /dev/rbd? (glob)
  $ sudo rbd device map custom/img@snap
  rbd: warning: image already mapped as /dev/rbd? (glob)
  /dev/rbd? (glob)
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   custom             img    snap  /dev/rbd? (glob)
  ?   custom             img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img@snap
  rbd: custom/img@snap: mapped more than once, unmapping /dev/rbd? only (glob)
  $ rbd device list
  id  pool    namespace  image  snap  device   
  ?   custom             img    snap  /dev/rbd? (glob)
  $ sudo rbd device unmap custom/img@snap
  $ rbd device list


Teardown
========

  $ ceph osd pool delete custom custom --yes-i-really-really-mean-it >/dev/null 2>&1
  $ rbd snap purge anotherimg >/dev/null 2>&1
  $ rbd rm anotherimg >/dev/null 2>&1
  $ rbd snap purge img >/dev/null 2>&1
  $ rbd rm img >/dev/null 2>&1

