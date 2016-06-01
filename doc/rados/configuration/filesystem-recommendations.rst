===========================================
 Hard Disk and File System Recommendations
===========================================

.. index:: hard drive preparation

Hard Drive Prep
===============

Ceph aims for data safety, which means that when the :term:`Ceph Client`
receives notice that data was written to a storage drive, that data was actually
written to the storage drive. For old kernels (<2.6.33), disable the write cache
if the journal is on a raw drive. Newer kernels should work fine.

Use ``hdparm`` to disable write caching on the hard disk::

	sudo hdparm -W 0 /dev/hda 0

In production environments, we recommend running a :term:`Ceph OSD Daemon` with
separate drives for the operating system and the data. If you run data and an
operating system on a single disk, we recommend creating a separate partition
for your data.

.. index:: filesystems

Filesystems
===========

Ceph OSD Daemons rely heavily upon the stability and performance of the
underlying filesystem.

Recommended
-----------

We currently recommend ``XFS`` for production deployments.

We used to recommend ``btrfs`` for testing, development, and any non-critical
deployments becuase it has the most promising set of features.  However, we
now plan to avoid using a kernel file system entirely with the new BlueStore
backend.  ``btrfs`` is still supported and has a comparatively compelling
set of features, but be mindful of its stability and support status in your
Linux distribution.

Not recommended
---------------

We recommend *against* using ``ext4`` due to limitations in the size
of xattrs it can store, and the problems this causes with the way Ceph
handles long RADOS object names.  Although these issues will generally
not surface with Ceph clusters using only short object names (e.g., an
RBD workload that does not include long RBD image names), other users
like RGW make extensive use of long object names and can break.

Starting with the Jewel release, the ``ceph-osd`` daemon will refuse
to start if the configured max object name cannot be safely stored on
``ext4``.  If the cluster is only being used with short object names
(e.g., RBD only), you can continue using ``ext4`` by setting the
following configuration option::

  osd max object name len = 256
  osd max object namespace len = 64

.. note:: This may result in difficult-to-diagnose errors if you try
          to use RGW or other librados clients that do not properly
          handle or politely surface any resulting ENAMETOOLONG
          errors.


Filesystem Background Info
==========================

The ``XFS``, ``btrfs`` and ``ext4`` file systems provide numerous
advantages in highly scaled data storage environments when `compared`_
to ``ext3``.

``XFS``, ``btrfs`` and ``ext4`` are `journaling file systems`_, which means that
they are more robust when recovering from crashes, power outages, etc. These
filesystems journal all of the changes they will make before performing writes.

``XFS`` was developed for Silicon Graphics, and is a mature and stable
filesystem. By contrast, ``btrfs`` is a relatively new file system that aims
to address the long-standing wishes of system administrators working with 
large scale data storage environments. ``btrfs`` has some unique features
and advantages compared to other Linux filesystems. 

``btrfs`` is a `copy-on-write`_ filesystem. It supports file creation
timestamps and checksums that verify metadata integrity, so it can detect
bad copies of data and fix them with the good copies. The copy-on-write 
capability means that ``btrfs`` can support snapshots that are writable.
``btrfs`` supports transparent compression and other features.

``btrfs`` also incorporates multi-device management into the file system,
which enables you to support heterogeneous disk storage infrastructure,
data allocation policies. The community also aims to provide ``fsck``, 
deduplication, and data encryption support in the future. This compelling 
list of features makes ``btrfs`` the ideal choice for Ceph clusters.

.. _copy-on-write: http://en.wikipedia.org/wiki/Copy-on-write
.. _compared: http://en.wikipedia.org/wiki/Comparison_of_file_systems
.. _journaling file systems: http://en.wikipedia.org/wiki/Journaling_file_system
