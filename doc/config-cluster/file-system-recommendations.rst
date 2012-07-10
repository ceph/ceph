===========================================
 Hard Disk and File System Recommendations
===========================================

Ceph aims for data safety, which means that when the application receives notice
that data was written to the disk, that data was actually written to the disk.
For old kernels (<2.6.33), disable the write cache if the journal is on a raw
disk. Newer kernels should work fine.

Use ``hdparm`` to disable write caching on the hard disk::

	hdparm -W 0 /dev/hda 0

In production environments, we recommend running OSDs with an operating system
disk, and a separate disk(s) for data. If you run data and an operating system
on a single disk, create a separate partition for your data before configuring
your OSD cluster.

Ceph OSDs depend on the Extended Attributes (XATTRs) of the underlying file
system for:

- Internal object state
- Snapshot metadata
- RADOS Gateway Access Control Lists (ACLs).

Ceph OSDs rely heavily upon the stability and performance of the underlying file
system. The underlying file system must provide sufficient capacity for XATTRs.
File system candidates for Ceph include B tree and B+ tree file systems such as:

- ``btrfs``
- ``XFS``

If you are using ``ext4``, mount your file system to enable XATTRs. You must also
add the following line to the ``[osd]`` section of your ``ceph.conf`` file. ::

	filestore xattr use omap = true

.. warning:: XATTR limits.

   The RADOS Gateway's ACL and Ceph snapshots easily surpass the 4-kilobyte limit
   for XATTRs in ``ext4``, causing the ``ceph-osd`` process to crash. Version 0.45
   or newer uses ``leveldb`` to bypass this limitation. ``ext4`` is a poor file
   system choice if you intend to deploy the RADOS Gateway or use snapshots on
   versions earlier than 0.45.

.. tip:: Use ``xfs`` initially and ``btrfs`` when it is ready for production.

   The Ceph team believes that the best performance and stability will come from
   ``btrfs.`` The ``btrfs`` file system has internal transactions that keep the
   local data set in a consistent state. This makes OSDs based on ``btrfs`` simple
   to deploy, while providing scalability not currently available from block-based
   file systems. The 64-kb XATTR limit for ``xfs`` XATTRS is enough to accommodate
   RDB snapshot metadata and RADOS Gateway ACLs. So ``xfs`` is the second-choice
   file system of the Ceph team in the long run, but ``xfs`` is currently more
   stable than ``btrfs``.  If you only plan to use RADOS and ``rbd`` without
   snapshots and without ``radosgw``, the ``ext4`` file system should work just fine.
