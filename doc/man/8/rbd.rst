===============================================
 rbd -- manage rados block device (RBD) images
===============================================

.. program:: rbd

Synopsis
========

| **rbd** [ -c *ceph.conf* ] [ -m *monaddr* ] [ -p | --pool *pool* ] [
  --size *size* ] [ --order *bits* ] [ *command* ... ]


Description
===========

**rbd** is a utility for manipulating rados block device (RBD) images,
used by the Linux rbd driver and the rbd storage driver for Qemu/KVM.
RBD images are simple block devices that are striped over objects and
stored in a RADOS object store. The size of the objects the image is
striped over must be a power of two.


Options
=======

.. option:: -c ceph.conf, --conf ceph.conf

   Use ceph.conf configuration file instead of the default /etc/ceph/ceph.conf to
   determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).

.. option:: -p pool, --pool pool

   Interact with the given pool. Required by most commands.


Parameters
==========

.. option:: --size size-in-mb

   Specifies the size (in megabytes) of the new rbd image.

.. option:: --order bits

   Specifies the object size expressed as a number of bits, such that
   the object size is ``1 << order``. The default is 22 (4 MB).

.. option:: --snap snap

   Specifies the snapshot name for the specific operation.

.. option:: --user username

   Specifies the username to use with the map command.

.. option:: --secret filename

   Specifies a file containing the secret to use with the map command.


Commands
========

.. TODO rst "option" directive seems to require --foo style options, parsing breaks on subcommands.. the args show up as bold too

:command:`ls` [*pool-name*]
  Will list all rbd images listed in the rbd_directory object.

:command:`info` [*image-name*]
  Will dump information (such as size and order) about a specific rbd image.

:command:`create` [*image-name*]
  Will create a new rbd image. You must also specify the size via --size.

:command:`clone` [*parent-snapname*] [*image-name*]
  Will create a clone (copy-on-write child) of the parent snapshot.
  Size and object order will be identical to parent image unless specified.

:command:`resize` [*image-name*]
  Resizes rbd image. The size parameter also needs to be specified.

:command:`rm` [*image-name*]
  Deletes an rbd image (including all data blocks). If the image has
  snapshots, this fails and nothing is deleted.

:command:`export` [*image-name*] [*dest-path*]
  Exports image to dest path.

:command:`import` [*path*] [*dest-image*]
  Creates a new image and imports its data from path.

:command:`cp` [*src-image*] [*dest-image*]
  Copies the content of a src-image into the newly created dest-image.

:command:`mv` [*src-image*] [*dest-image*]
  Renames an image.  Note: rename across pools is not supported.

:command:`snap` ls [*image-name*]
  Dumps the list of snapshots inside a specific image.

:command:`snap` create [*image-name*]
  Creates a new snapshot. Requires the snapshot name parameter specified.

:command:`snap` rollback [*image-name*]
  Rollback image content to snapshot. This will iterate through the entire blocks
  array and update the data head content to the snapshotted version.

:command:`snap` rm [*image-name*]
  Removes the specified snapshot.

:command:`snap` purge [*image-name*]
  Removes all snapshots from an image.

:command:`map` [*image-name*]
  Maps the specified image to a block device via the rbd kernel module.

:command:`unmap` [*device-path*]
  Unmaps the block device that was mapped via the rbd kernel module.

:command:`showmapped`
  Show the rbd images that are mapped via the rbd kernel module.

Image name
==========

In addition to using the --pool and the --snap options, the image name can include both
the pool name and the snapshot name. The image name format is as follows::

       [pool/]image-name[@snap]

Thus an image name that contains a slash character ('/') requires specifying the pool
name explicitly.


Examples
========

To create a new rbd image that is 100 GB::

       rbd -p mypool create myimage --size 102400

or alternatively::

       rbd create mypool/myimage --size 102400

To use a non-default object size (8 MB)::

       rbd create mypool/myimage --size 102400 --order 23

To delete an rbd image (be careful!)::

       rbd rm mypool/myimage

To create a new snapshot::

       rbd snap create mypool/myimage@mysnap

To create a copy-on-write clone of a snapshot::

       rbd clone myimage@mysnap cloneimage

To delete a snapshot::

       rbd snap rm mypool/myimage@mysnap

To map an image via the kernel with cephx enabled::

       rbd map myimage --user admin --secret secretfile

To unmap an image::

       rbd unmap /dev/rbd0


Availability
============

**rbd** is part of the Ceph distributed file system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`rados <rados>`\(8)
