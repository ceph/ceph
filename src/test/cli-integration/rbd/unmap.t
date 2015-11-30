
Setup
=====

  $ rbd create --size 1 img
  $ rbd snap create img@snap
  $ rbd create --size 1 anotherimg
  $ ceph osd pool create custom 8 >/dev/null 2>&1
  $ rbd create --size 1 custom/img
  $ rbd snap create custom/img@snap
  $ rbd snap create custom/img@anothersnap

Spell out device instead of using $DEV - sfdisk is not a joke.

  $ DEV=$(sudo rbd map img)
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

  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap $DEV
  $ rbd showmapped

Unmap by device partition:

  $ DEV=$(sudo rbd map img)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap ${DEV}p1
  $ rbd showmapped

  $ DEV=$(sudo rbd map img)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap ${DEV}p5
  $ rbd showmapped

Not a block device - random junk prefixed with /dev/ (so it's not
interpreted as a spec):

  $ sudo rbd unmap /dev/foobar
  rbd: '/dev/foobar' is not a block device
  rbd: unmap failed: (22) Invalid argument
  [22]

Not a block device - device that's just been unmapped:

  $ DEV=$(sudo rbd map img)
  $ sudo rbd unmap $DEV
  $ sudo rbd unmap $DEV
  rbd: '/dev/rbd?' is not a block device (glob)
  rbd: unmap failed: (22) Invalid argument
  [22]

A block device, but not rbd:

  $ sudo rbd unmap /dev/[sv]da
  rbd: '/dev/?da' is not an rbd device (glob)
  rbd: unmap failed: (22) Invalid argument
  [22]


Unmap by spec
=============

img:

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap img
  $ rbd showmapped

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd --image img unmap
  $ rbd showmapped

img@snap:

  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap img@snap
  $ rbd showmapped

  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd --snap snap unmap img
  $ rbd showmapped

  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd --image img --snap snap unmap
  $ rbd showmapped

pool/img@snap, default pool:

  $ sudo rbd map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap rbd/img@snap
  $ rbd showmapped

  $ sudo rbd map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd --pool rbd unmap img@snap
  $ rbd showmapped

  $ sudo rbd map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd --pool rbd --snap snap unmap img
  $ rbd showmapped

  $ sudo rbd map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd --pool rbd --image img --snap snap unmap
  $ rbd showmapped

pool/img@snap, custom pool:

  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap device    
  ?  custom img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img@snap
  $ rbd showmapped

  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap device    
  ?  custom img   snap /dev/rbd?  (glob)
  $ sudo rbd --pool custom unmap img@snap
  $ rbd showmapped

  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap device    
  ?  custom img   snap /dev/rbd?  (glob)
  $ sudo rbd --pool custom --snap snap unmap img
  $ rbd showmapped

  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap device    
  ?  custom img   snap /dev/rbd?  (glob)
  $ sudo rbd --pool custom --image img --snap snap unmap
  $ rbd showmapped

Not a mapped spec - random junk (which gets interpreted as a spec):

  $ sudo rbd unmap foobar
  rbd: rbd/foobar@-: not a mapped image or snapshot
  rbd: unmap failed: (22) Invalid argument
  [22]

  $ sudo rbd --image foobar unmap
  rbd: rbd/foobar@-: not a mapped image or snapshot
  rbd: unmap failed: (22) Invalid argument
  [22]

Not a mapped spec - spec that's just been unmapped:

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ sudo rbd unmap img
  $ sudo rbd unmap img
  rbd: rbd/img@-: not a mapped image or snapshot
  rbd: unmap failed: (22) Invalid argument
  [22]

  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ sudo rbd unmap img@snap
  $ sudo rbd unmap img@snap
  rbd: rbd/img@snap: not a mapped image or snapshot
  rbd: unmap failed: (22) Invalid argument
  [22]

Need an arg:

  $ sudo rbd unmap
  rbd: unmap requires either image name or device path
  [22]


Two images
==========

Unmap img first:

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ sudo rbd map anotherimg
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image      snap device    
  ?  rbd  img        -    /dev/rbd?  (glob)
  ?  rbd  anotherimg -    /dev/rbd?  (glob)
  $ sudo rbd unmap img
  $ rbd showmapped
  id pool image      snap device    
  ?  rbd  anotherimg -    /dev/rbd?  (glob)
  $ sudo rbd unmap anotherimg
  $ rbd showmapped

Unmap anotherimg first:

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ sudo rbd map anotherimg
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image      snap device    
  ?  rbd  img        -    /dev/rbd?  (glob)
  ?  rbd  anotherimg -    /dev/rbd?  (glob)
  $ sudo rbd unmap anotherimg
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap img
  $ rbd showmapped


Image and its snap
==================

Unmap the image first:

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap img
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap img@snap
  $ rbd showmapped

Unmap the snap first:

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap img@snap
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap img
  $ rbd showmapped


Two snaps of the same image
===========================

Unmap snap first:

  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ sudo rbd map custom/img@anothersnap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap        device    
  ?  custom img   snap        /dev/rbd?  (glob)
  ?  custom img   anothersnap /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img@snap
  $ rbd showmapped
  id pool   image snap        device    
  ?  custom img   anothersnap /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img@anothersnap
  $ rbd showmapped

Unmap anothersnap first:

  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ sudo rbd map custom/img@anothersnap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap        device    
  ?  custom img   snap        /dev/rbd?  (glob)
  ?  custom img   anothersnap /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img@anothersnap
  $ rbd showmapped
  id pool   image snap device    
  ?  custom img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img@snap
  $ rbd showmapped


Same img and snap in different pools
====================================

img:

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ sudo rbd map custom/img
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap device    
  ?  rbd    img   -    /dev/rbd?  (glob)
  ?  custom img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap img
  $ rbd showmapped
  id pool   image snap device    
  ?  custom img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img
  $ rbd showmapped

img@snap:

  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap device    
  ?  rbd    img   snap /dev/rbd?  (glob)
  ?  custom img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img@snap
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap img@snap
  $ rbd showmapped


Same spec mapped twice
======================

img:

  $ sudo rbd map img
  /dev/rbd? (glob)
  $ sudo rbd map img
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap img
  rbd: rbd/img@-: mapped more than once, unmapping /dev/rbd? only (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   -    /dev/rbd?  (glob)
  $ sudo rbd unmap img
  $ rbd showmapped

img@snap:

  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ sudo rbd map img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap img@snap
  rbd: rbd/img@snap: mapped more than once, unmapping /dev/rbd? only (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap img@snap
  $ rbd showmapped

pool/img@snap, default pool:

  $ sudo rbd map rbd/img@snap
  /dev/rbd? (glob)
  $ sudo rbd map rbd/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap rbd/img@snap
  rbd: rbd/img@snap: mapped more than once, unmapping /dev/rbd? only (glob)
  $ rbd showmapped
  id pool image snap device    
  ?  rbd  img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap rbd/img@snap
  $ rbd showmapped

pool/img@snap, custom pool:

  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ sudo rbd map custom/img@snap
  /dev/rbd? (glob)
  $ rbd showmapped
  id pool   image snap device    
  ?  custom img   snap /dev/rbd?  (glob)
  ?  custom img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img@snap
  rbd: custom/img@snap: mapped more than once, unmapping /dev/rbd? only (glob)
  $ rbd showmapped
  id pool   image snap device    
  ?  custom img   snap /dev/rbd?  (glob)
  $ sudo rbd unmap custom/img@snap
  $ rbd showmapped


Teardown
========

  $ ceph osd pool delete custom custom --yes-i-really-really-mean-it >/dev/null 2>&1
  $ rbd snap purge anotherimg >/dev/null 2>&1
  $ rbd rm anotherimg >/dev/null 2>&1
  $ rbd snap purge img >/dev/null 2>&1
  $ rbd rm img >/dev/null 2>&1

