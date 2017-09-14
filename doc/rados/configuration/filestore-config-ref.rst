============================
 Filestore Config Reference
============================


``filestore debug omap check``

:Description: Debugging check on synchronization. Expensive. For debugging only.
:Type: Boolean
:Required: No
:Default: ``false``


.. index:: filestore; extended attributes

Extended Attributes
===================

Extended Attributes (XATTRs) are an important aspect in your configuration. 
Some file systems have limits on the number of bytes stored in XATTRS. 
Additionally, in some cases, the filesystem may not be as fast as an alternative
method of storing XATTRs. The following settings may help improve performance
by using a method of storing XATTRs that is extrinsic to the underlying filesystem.

Ceph XATTRs are stored as ``inline xattr``, using the XATTRs provided
by the underlying file system, if it does not impose a size limit. If
there is a size limit (4KB total on ext4, for instance), some Ceph
XATTRs will be stored in an key/value database when either the
``filestore max inline xattr size`` or ``filestore max inline
xattrs`` threshold is reached.


``filestore max inline xattr size``

:Description: The maximimum size of an XATTR stored in the filesystem (i.e., XFS,
              btrfs, ext4, etc.) per object. Should not be larger than the
              filesytem can handle. Default value of 0 means to use the value
              specific to the underlying filesystem.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``0``


``filestore max inline xattr size xfs``

:Description: The maximimum size of an XATTR stored in the XFS filesystem.
              Only used if ``filestore max inline xattr size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``65536``


``filestore max inline xattr size btrfs``

:Description: The maximimum size of an XATTR stored in the btrfs filesystem.
              Only used if ``filestore max inline xattr size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``2048``


``filestore max inline xattr size other``

:Description: The maximimum size of an XATTR stored in other filesystems.
              Only used if ``filestore max inline xattr size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``512``


``filestore max inline xattrs``

:Description: The maximum number of XATTRs stored in the filesystem per object.
              Default value of 0 means to use the value specific to the
              underlying filesystem.
:Type: 32-bit Integer
:Required: No
:Default: ``0``


``filestore max inline xattrs xfs``

:Description: The maximum number of XATTRs stored in the XFS filesystem per object.
              Only used if ``filestore max inline xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``10``


``filestore max inline xattrs btrfs``

:Description: The maximum number of XATTRs stored in the btrfs filesystem per object.
              Only used if ``filestore max inline xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``10``


``filestore max inline xattrs other``

:Description: The maximum number of XATTRs stored in other filesystems per object.
              Only used if ``filestore max inline xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``2``

.. index:: filestore; synchronization

Synchronization Intervals
=========================

Periodically, the filestore needs to quiesce writes and synchronize the
filesystem, which creates a consistent commit point. It can then free journal
entries up to the commit point. Synchronizing more frequently tends to reduce
the time required to perform synchronization, and reduces the amount of data
that needs to remain in the  journal. Less frequent synchronization allows the
backing filesystem to coalesce  small writes and metadata updates more
optimally--potentially resulting in more efficient synchronization.


``filestore max sync interval``

:Description: The maximum interval in seconds for synchronizing the filestore.
:Type: Double
:Required: No
:Default: ``5``


``filestore min sync interval``

:Description: The minimum interval in seconds for synchronizing the filestore.
:Type: Double
:Required: No
:Default: ``.01``


.. index:: filestore; flusher

Flusher
=======

The filestore flusher forces data from large writes to be written out using
``sync file range`` before the sync in order to (hopefully) reduce the cost of
the eventual sync. In practice, disabling 'filestore flusher' seems to improve
performance in some cases.


``filestore flusher``

:Description: Enables the filestore flusher.
:Type: Boolean
:Required: No
:Default: ``false``

.. deprecated:: v.65

``filestore flusher max fds``

:Description: Sets the maximum number of file descriptors for the flusher.
:Type: Integer
:Required: No
:Default: ``512``

.. deprecated:: v.65

``filestore sync flush``

:Description: Enables the synchronization flusher. 
:Type: Boolean
:Required: No
:Default: ``false``

.. deprecated:: v.65

``filestore fsync flushes journal data``

:Description: Flush journal data during filesystem synchronization.
:Type: Boolean
:Required: No
:Default: ``false``


.. index:: filestore; queue

Queue
=====

The following settings provide limits on the size of filestore queue.

``filestore queue max ops``

:Description: Defines the maximum number of in progress operations the file store accepts before blocking on queuing new operations. 
:Type: Integer
:Required: No. Minimal impact on performance.
:Default: ``50``


``filestore queue max bytes``

:Description: The maximum number of bytes for an operation. 
:Type: Integer
:Required: No
:Default: ``100 << 20``




.. index:: filestore; timeouts

Timeouts
========


``filestore op threads``

:Description: The number of filesystem operation threads that execute in parallel. 
:Type: Integer
:Required: No
:Default: ``2``


``filestore op thread timeout``

:Description: The timeout for a filesystem operation thread (in seconds).
:Type: Integer
:Required: No
:Default: ``60``


``filestore op thread suicide timeout``

:Description: The timeout for a commit operation before cancelling the commit (in seconds). 
:Type: Integer
:Required: No
:Default: ``180``


.. index:: filestore; btrfs

B-Tree Filesystem
=================


``filestore btrfs snap``

:Description: Enable snapshots for a ``btrfs`` filestore.
:Type: Boolean
:Required: No. Only used for ``btrfs``.
:Default: ``true``


``filestore btrfs clone range``

:Description: Enable cloning ranges for a ``btrfs`` filestore.
:Type: Boolean
:Required: No. Only used for ``btrfs``.
:Default: ``true``


.. index:: filestore; journal

Journal
=======


``filestore journal parallel``

:Description: Enables parallel journaling, default for btrfs.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore journal writeahead``

:Description: Enables writeahead journaling, default for xfs.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore journal trailing``

:Description: Deprecated, never use.
:Type: Boolean
:Required: No
:Default: ``false``


Misc
====


``filestore merge threshold``

:Description: Min number of files in a subdir before merging into parent
              NOTE: A negative value means to disable subdir merging
:Type: Integer
:Required: No
:Default: ``10``


``filestore split multiple``

:Description:  ``(filestore_split_multiple * abs(filestore_merge_threshold) + (rand() % filestore_split_rand_factor)) * 16``
               is the maximum number of files in a subdirectory before 
               splitting into child directories.

:Type: Integer
:Required: No
:Default: ``2``


``filestore split rand factor``

:Description:  A random factor added to the split threshold to avoid
               too many filestore splits occurring at once. See
               ``filestore split multiple`` for details.
               This can only be changed for an existing osd offline,
               via ceph-objectstore-tool's apply-layout-settings command.

:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``20``


``filestore update to``

:Description: Limits filestore auto upgrade to specified version.
:Type: Integer
:Required: No
:Default: ``1000``


``filestore blackhole``

:Description: Drop any new transactions on the floor.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore dump file``

:Description: File onto which store transaction dumps.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore kill at``

:Description: inject a failure at the n'th opportunity
:Type: String
:Required: No
:Default: ``false``


``filestore fail eio``

:Description: Fail/Crash on eio.
:Type: Boolean
:Required: No
:Default: ``true``

