:orphan:

===============================================
 rbd -- manage rados block device (RBD) images
===============================================

.. program:: rbd

Synopsis
========

| **rbd** [ -c *ceph.conf* ] [ -m *monaddr* ] [--cluster *cluster name*]
  [ -p | --pool *pool* ] [--size *size* ] [ --object-size *B/K/M* ] [ *command* ... ] 


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

.. option:: --cluster cluster name

   Use different cluster name as compared to default cluster name *ceph*.

.. option:: -p pool-name, --pool pool-name

   Interact with the given pool. Required by most commands.

.. option:: --no-progress

   Do not output progress information (goes to standard error by
   default for some commands).


Parameters
==========

.. option:: --image-format format-id

   Specifies which object layout to use. The default is 1.

   * format 1 - (deprecated) Use the original format for a new rbd image. This
     format is understood by all versions of librbd and the kernel rbd module,
     but does not support newer features like cloning.

   * format 2 - Use the second rbd format, which is supported by
     librbd and kernel since version 3.11 (except for striping). This adds
     support for cloning and is more easily extensible to allow more
     features in the future.

.. option:: --size size-in-M/G/T

   Specifies the size (in M/G/T) of the new rbd image.

.. option:: --object-size B/K/M

   Specifies the object size in B/K/M, it will be rounded up the nearest power of two.
   The default object size is 4 MB, smallest is 4K and maximum is 32M.


.. option:: --stripe-unit size-in-B/K/M

   Specifies the stripe unit size in B/K/M.  See striping section (below) for more details.

.. option:: --stripe-count num

   Specifies the number of objects to stripe over before looping back
   to the first object.  See striping section (below) for more details.

.. option:: --snap snap

   Specifies the snapshot name for the specific operation.

.. option:: --id username

   Specifies the username (without the ``client.`` prefix) to use with the map command.

.. option:: --keyring filename

   Specifies a keyring file containing a secret for the specified user
   to use with the map command.  If not specified, the default keyring
   locations will be searched.

.. option:: --keyfile filename

   Specifies a file containing the secret key of ``--id user`` to use with the map command.
   This option is overridden by ``--keyring`` if the latter is also specified.

.. option:: --shared lock-tag

   Option for `lock add` that allows multiple clients to lock the
   same image if they use the same tag. The tag is an arbitrary
   string. This is useful for situations where an image must
   be open from more than one client at once, like during
   live migration of a virtual machine, or for use underneath
   a clustered filesystem.

.. option:: --format format

   Specifies output formatting (default: plain, json, xml)

.. option:: --pretty-format

   Make json or xml formatted output more human-readable.

.. option:: -o map-options, --options map-options

   Specifies which options to use when mapping an image.  map-options is
   a comma-separated string of options (similar to mount(8) mount options).
   See map options section below for more details.

.. option:: --read-only

   Map the image read-only.  Equivalent to -o ro.

.. option:: --image-feature feature-name

   Specifies which RBD format 2 feature should be enabled when creating
   an image. Multiple features can be enabled by repeating this option
   multiple times. The following features are supported:

   * layering: layering support
   * striping: striping v2 support
   * exclusive-lock: exclusive locking support
   * object-map: object map support (requires exclusive-lock)
   * fast-diff: fast diff calculations (requires object-map)
   * deep-flatten: snapshot flatten support
   * journaling: journaled IO support (requires exclusive-lock)

.. option:: --image-shared

   Specifies that the image will be used concurrently by multiple clients.
   This will disable features that are dependent upon exclusive ownership
   of the image.

.. option:: --whole-object

   Specifies that the diff should be limited to the extents of a full object
   instead of showing intra-object deltas. When the object map feature is
   enabled on an image, limiting the diff to the object extents will
   dramatically improve performance since the differences can be computed
   by examining the in-memory object map instead of querying RADOS for each
   object within the image.

Commands
========

.. TODO rst "option" directive seems to require --foo style options, parsing breaks on subcommands.. the args show up as bold too

:command:`ls` [-l | --long] [*pool-name*]
  Will list all rbd images listed in the rbd_directory object.  With
  -l, also show snapshots, and use longer-format output including
  size, parent (if clone), format, etc.

:command:`du` [-p | --pool *pool-name*] [*image-spec* | *snap-spec*]
  Will calculate the provisioned and actual disk usage of all images and
  associated snapshots within the specified pool.  It can also be used against
  individual images and snapshots.

  If the RBD fast-diff feature isn't enabled on images, this operation will
  require querying the OSDs for every potential object within the image.

:command:`info` *image-spec* | *snap-spec*
  Will dump information (such as size and object size) about a specific rbd image.
  If image is a clone, information about its parent is also displayed.
  If a snapshot is specified, whether it is protected is shown as well.

:command:`create` (-s | --size *size-in-M/G/T*) [--image-format *format-id*] [--object-size *B/K/M*] [--stripe-unit *size-in-B/K/M* --stripe-count *num*] [--image-feature *feature-name*]... [--image-shared] *image-spec*
  Will create a new rbd image. You must also specify the size via --size.  The
  --stripe-unit and --stripe-count arguments are optional, but must be used together.

:command:`clone` [--object-size *B/K/M*] [--stripe-unit *size-in-B/K/M* --stripe-count *num*] [--image-feature *feature-name*] [--image-shared] *parent-snap-spec* *child-image-spec*
  Will create a clone (copy-on-write child) of the parent snapshot.
  Object size will be identical to that of the parent image unless
  specified. Size will be the same as the parent snapshot. The --stripe-unit
  and --stripe-count arguments are optional, but must be used together.

  The parent snapshot must be protected (see `rbd snap protect`).
  This requires image format 2.

:command:`flatten` *image-spec*
  If image is a clone, copy all shared blocks from the parent snapshot and
  make the child independent of the parent, severing the link between
  parent snap and child.  The parent snapshot can be unprotected and
  deleted if it has no further dependent clones.

  This requires image format 2.

:command:`children` *snap-spec*
  List the clones of the image at the given snapshot. This checks
  every pool, and outputs the resulting poolname/imagename.

  This requires image format 2.

:command:`resize` (-s | --size *size-in-M/G/T*) [--allow-shrink] *image-spec*
  Resizes rbd image. The size parameter also needs to be specified.
  The --allow-shrink option lets the size be reduced.

:command:`rm` *image-spec*
  Deletes an rbd image (including all data blocks). If the image has
  snapshots, this fails and nothing is deleted.

:command:`export` (*image-spec* | *snap-spec*) [*dest-path*]
  Exports image to dest path (use - for stdout).

:command:`import` [--image-format *format-id*] [--object-size *B/K/M*] [--stripe-unit *size-in-B/K/M* --stripe-count *num*] [--image-feature *feature-name*]... [--image-shared] *src-path* [*image-spec*]
  Creates a new image and imports its data from path (use - for
  stdin).  The import operation will try to create sparse rbd images 
  if possible.  For import from stdin, the sparsification unit is
  the data block size of the destination image (object size).

  The --stripe-unit and --stripe-count arguments are optional, but must be
  used together.

:command:`export-diff` [--from-snap *snap-name*] [--whole-object] (*image-spec* | *snap-spec*) *dest-path*
  Exports an incremental diff for an image to dest path (use - for stdout).  If
  an initial snapshot is specified, only changes since that snapshot are included; otherwise,
  any regions of the image that contain data are included.  The end snapshot is specified
  using the standard --snap option or @snap syntax (see below).  The image diff format includes
  metadata about image size changes, and the start and end snapshots.  It efficiently represents
  discarded or 'zero' regions of the image.

:command:`merge-diff` *first-diff-path* *second-diff-path* *merged-diff-path*
  Merge two continuous incremental diffs of an image into one single diff. The
  first diff's end snapshot must be equal with the second diff's start snapshot.
  The first diff could be - for stdin, and merged diff could be - for stdout, which
  enables multiple diff files to be merged using something like
  'rbd merge-diff first second - | rbd merge-diff - third result'. Note this command
  currently only support the source incremental diff with stripe_count == 1

:command:`import-diff` *src-path* *image-spec*
  Imports an incremental diff of an image and applies it to the current image.  If the diff
  was generated relative to a start snapshot, we verify that snapshot already exists before
  continuing.  If there was an end snapshot we verify it does not already exist before
  applying the changes, and create the snapshot when we are done.

:command:`diff` [--from-snap *snap-name*] [--whole-object] *image-spec* | *snap-spec*
  Dump a list of byte extents in the image that have changed since the specified start
  snapshot, or since the image was created.  Each output line includes the starting offset
  (in bytes), the length of the region (in bytes), and either 'zero' or 'data' to indicate
  whether the region is known to be zeros or may contain other data.

:command:`cp` (*src-image-spec* | *src-snap-spec*) *dest-image-spec*
  Copies the content of a src-image into the newly created dest-image.
  dest-image will have the same size, object size, and image format as src-image.

:command:`mv` *src-image-spec* *dest-image-spec*
  Renames an image.  Note: rename across pools is not supported.

:command:`image-meta list` *image-spec*
  Show metadata held on the image. The first column is the key
  and the second column is the value.

:command:`image-meta get` *image-spec* *key*
  Get metadata value with the key.

:command:`image-meta set` *image-spec* *key* *value*
  Set metadata key with the value. They will displayed in `image-meta list`.

:command:`image-meta remove` *image-spec* *key*
  Remove metadata key with the value.

:command:`object-map rebuild` *image-spec* | *snap-spec*
  Rebuilds an invalid object map for the specified image. An image snapshot can be
  specified to rebuild an invalid object map for a snapshot.

:command:`snap ls` *image-spec*
  Dumps the list of snapshots inside a specific image.

:command:`snap create` *snap-spec*
  Creates a new snapshot. Requires the snapshot name parameter specified.

:command:`snap rollback` *snap-spec*
  Rollback image content to snapshot. This will iterate through the entire blocks
  array and update the data head content to the snapshotted version.

:command:`snap rm` *snap-spec*
  Removes the specified snapshot.

:command:`snap purge` *image-spec*
  Removes all snapshots from an image.

:command:`snap protect` *snap-spec*
  Protect a snapshot from deletion, so that clones can be made of it
  (see `rbd clone`).  Snapshots must be protected before clones are made;
  protection implies that there exist dependent cloned children that
  refer to this snapshot.  `rbd clone` will fail on a nonprotected
  snapshot.

  This requires image format 2.

:command:`snap unprotect` *snap-spec*
  Unprotect a snapshot from deletion (undo `snap protect`).  If cloned
  children remain, `snap unprotect` fails.  (Note that clones may exist
  in different pools than the parent snapshot.)

  This requires image format 2.

:command:`map` [-o | --options *map-options* ] [--read-only] *image-spec* | *snap-spec*
  Maps the specified image to a block device via the rbd kernel module.

:command:`unmap` *image-spec* | *snap-spec* | *device-path*
  Unmaps the block device that was mapped via the rbd kernel module.

:command:`showmapped`
  Show the rbd images that are mapped via the rbd kernel module.

:command:`nbd map` [--device *device-path*] [--read-only] *image-spec* | *snap-spec*
  Maps the specified image to a block device via the rbd-nbd tool.

:command:`nbd unmap` *device-path*
  Unmaps the block device that was mapped via the rbd-nbd tool.

:command:`nbd list`
  Show the list of used nbd devices via the rbd-nbd tool.

:command:`status` *image-spec*
  Show the status of the image, including which clients have it open.

:command:`feature disable` *image-spec* *feature-name*...
  Disables the specified feature on the specified image. Multiple features can
  be specified.

:command:`feature enable` *image-spec* *feature-name*...
  Enables the specified feature on the specified image. Multiple features can
  be specified.

:command:`lock list` *image-spec*
  Show locks held on the image. The first column is the locker
  to use with the `lock remove` command.

:command:`lock add` [--shared *lock-tag*] *image-spec* *lock-id*
  Lock an image. The lock-id is an arbitrary name for the user's
  convenience. By default, this is an exclusive lock, meaning it
  will fail if the image is already locked. The --shared option
  changes this behavior. Note that locking does not affect
  any operation other than adding a lock. It does not
  protect an image from being deleted.

:command:`lock remove` *image-spec* *lock-id* *locker*
  Release a lock on an image. The lock id and locker are
  as output by lock ls.

:command:`bench-write` [--io-size *size-in-B/K/M/G/T*] [--io-threads *num-ios-in-flight*] [--io-total *total-size-to-write-in-B/K/M/G/T*] [--io-pattern seq | rand] *image-spec*
  Generate a series of writes to the image and measure the write throughput and
  latency.  Defaults are: --io-size 4096, --io-threads 16, --io-total 1G,
  --io-pattern seq.

Image and snap specs
====================

| *image-spec* is [*pool-name*]/*image-name*
| *snap-spec*  is [*pool-name*]/*image-name*\ @\ *snap-name*

The default for *pool-name* is "rbd".  If an image name contains a slash
character ('/'), *pool-name* is required.

You may specify each name individually, using --pool, --image and --snap
options, but this is discouraged in favor of the above spec syntax.

Striping
========

RBD images are striped over many objects, which are then stored by the
Ceph distributed object store (RADOS).  As a result, read and write
requests for the image are distributed across many nodes in the
cluster, generally preventing any single node from becoming a
bottleneck when individual images get large or busy.

The striping is controlled by three parameters:

.. option:: object-size

  The size of objects we stripe over is a power of two. It will be rounded up the nearest power of two.
  The default object size is 4 MB, smallest is 4K and maximum is 32M.

.. option:: stripe_unit

  Each [*stripe_unit*] contiguous bytes are stored adjacently in the same object, before we move on
  to the next object.

.. option:: stripe_count

  After we write [*stripe_unit*] bytes to [*stripe_count*] objects, we loop back to the initial object
  and write another stripe, until the object reaches its maximum size.  At that point,
  we move on to the next [*stripe_count*] objects.

By default, [*stripe_unit*] is the same as the object size and [*stripe_count*] is 1.  Specifying a different
[*stripe_unit*] requires that the STRIPINGV2 feature be supported (added in Ceph v0.53) and format 2 images be
used.


Map options
===========

Most of these options are useful mainly for debugging and benchmarking.  The
default values are set in the kernel and may therefore depend on the version of
the running kernel.

libceph (per client instance) options:

* fsid=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee - FSID that should be assumed by
  the client.

* ip=a.b.c.d[:p] - IP and, optionally, port the client should use.

* share - Enable sharing of client instances with other mappings (default).

* noshare - Disable sharing of client instances with other mappings.

* crc - Enable CRC32C checksumming for data writes (default).

* nocrc - Disable CRC32C checksumming for data writes.

* cephx_require_signatures - Require cephx message signing (since 3.19,
  default).

* nocephx_require_signatures - Don't require cephx message signing (since
  3.19).

* tcp_nodelay - Disable Nagle's algorithm on client sockets (since 4.0,
  default).

* notcp_nodelay - Enable Nagle's algorithm on client sockets (since 4.0).

* cephx_sign_messages - Enable message signing (since 4.4, default).

* nocephx_sign_messages - Disable message signing (since 4.4).

* mount_timeout=x - A timeout on various steps in `rbd map` and `rbd unmap`
  sequences (default is 60 seconds).  In particular, since 4.2 this can be used
  to ensure that `rbd unmap` eventually times out when there is no network
  connection to a cluster.

* osdkeepalive=x - OSD keepalive timeout (default is 5 seconds).

* osd_idle_ttl=x - OSD idle TTL (default is 60 seconds).

Mapping (per block device) options:

* rw - Map the image read-write (default).

* ro - Map the image read-only.  Equivalent to --read-only.

* queue_depth=x - queue depth (since 4.2, default is 128 requests).


Examples
========

To create a new rbd image that is 100 GB::

       rbd create mypool/myimage --size 102400

To use a non-default object size (8 MB)::

       rbd create mypool/myimage --size 102400 --object-size 8M

To delete an rbd image (be careful!)::

       rbd rm mypool/myimage

To create a new snapshot::

       rbd snap create mypool/myimage@mysnap

To create a copy-on-write clone of a protected snapshot::

       rbd clone mypool/myimage@mysnap otherpool/cloneimage

To see which clones of a snapshot exist::

       rbd children mypool/myimage@mysnap

To delete a snapshot::

       rbd snap rm mypool/myimage@mysnap

To map an image via the kernel with cephx enabled::

       rbd map mypool/myimage --id admin --keyfile secretfile

To map an image via the kernel with different cluster name other than default *ceph*.

       rbd map mypool/myimage --cluster *cluster name*

To unmap an image::

       rbd unmap /dev/rbd0

To create an image and a clone from it::

       rbd import --image-format 2 image mypool/parent
       rbd snap create mypool/parent@snap
       rbd snap protect mypool/parent@snap
       rbd clone mypool/parent@snap otherpool/child

To create an image with a smaller stripe_unit (to better distribute small writes in some workloads)::

       rbd create mypool/myimage --size 102400 --stripe-unit 65536B --stripe-count 16

To change an image from one image format to another, export it and then
import it as the desired image format::

       rbd export mypool/myimage@snap /tmp/img
       rbd import --image-format 2 /tmp/img mypool/myimage2

To lock an image for exclusive use::

       rbd lock add mypool/myimage mylockid

To release a lock::

       rbd lock remove mypool/myimage mylockid client.2485


Availability
============

**rbd** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`rados <rados>`\(8)
