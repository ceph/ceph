============
 Filesystem
============
For details on file systems when configuring a cluster See 
`Hard Disk and File System Recommendations`_ .

.. tip:: We recommend configuring Ceph to use the ``XFS`` file system in 
         the near term, and ``btrfs`` in the long term once it is stable 
         enough for production.
 
Before ``ext3``, ``ReiserFS`` was the only journaling file system available for
Linux. However, ``ext3`` doesn't provide Extended Attribute (XATTR) support.
While ``ext4`` provides XATTR support, it only allows XATTRs up to 4kb. The 
4kb limit is not enough for RADOS GW ACLs, snapshots, and other features. As of
version 0.45, Ceph provides a ``leveldb`` feature for ``ext4`` file systems 
that stores XATTRs in excess of 4kb in a ``leveldb`` database.  

The ``XFS`` and ``btrfs`` file systems provide numerous advantages in highly 
scaled data storage environments when `compared`_ to ``ext3`` and ``ext4``.
Both ``XFS`` and ``btrfs`` are `journaling file systems`_, which means that
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
.. _Hard Disk and File System Recommendations: ../../config-cluster/file-system-recommendations
.. _compared: http://en.wikipedia.org/wiki/Comparison_of_file_systems
.. _journaling file systems: http://en.wikipedia.org/wiki/Journaling_file_system
