============================
 Filestore Config Reference
============================
.. warning:: Filestore has been deprecated in the Reef release and is no longer supported.

The Filestore back end is no longer the default when creating new OSDs,
though Filestore OSDs are still supported.

``filestore_debug_omap_check``

:Description: Debugging check on synchronization. Expensive. For debugging only.
:Type: Boolean
:Required: No
:Default: ``false``


.. index:: filestore; extended attributes

Extended Attributes
===================

Extended Attributes (XATTRs) are important for Filestore OSDs.
Some file systems have limits on the number of bytes that can be stored in XATTRs. 
Additionally, in some cases, the file system may not be as fast as an alternative
method of storing XATTRs. The following settings may help improve performance
by using a method of storing XATTRs that is extrinsic to the underlying file system.

Ceph XATTRs are stored as ``inline xattr``, using the XATTRs provided
by the underlying file system, if it does not impose a size limit. If
there is a size limit (4KB total on ext4, for instance), some Ceph
XATTRs will be stored in a key/value database when either the
``filestore_max_inline_xattr_size`` or ``filestore_max_inline_xattrs``
threshold is reached.


``filestore_max_inline_xattr_size``

:Description: The maximum size of an XATTR stored in the file system (i.e., XFS,
              Btrfs, EXT4, etc.) per object. Should not be larger than the
              file system can handle. Default value of 0 means to use the value
              specific to the underlying file system.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``0``


``filestore_max_inline_xattr_size_xfs``

:Description: The maximum size of an XATTR stored in the XFS file system.
              Only used if ``filestore_max_inline_xattr_size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``65536``


``filestore_max_inline_xattr_size_btrfs``

:Description: The maximum size of an XATTR stored in the Btrfs file system.
              Only used if ``filestore_max_inline_xattr_size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``2048``


``filestore_max_inline_xattr_size_other``

:Description: The maximum size of an XATTR stored in other file systems.
              Only used if ``filestore_max_inline_xattr_size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``512``


``filestore_max_inline_xattrs``

:Description: The maximum number of XATTRs stored in the file system per object.
              Default value of 0 means to use the value specific to the
              underlying file system.
:Type: 32-bit Integer
:Required: No
:Default: ``0``


``filestore_max_inline_xattrs_xfs``

:Description: The maximum number of XATTRs stored in the XFS file system per object.
              Only used if ``filestore_max_inline_xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``10``


``filestore_max_inline_xattrs_btrfs``

:Description: The maximum number of XATTRs stored in the Btrfs file system per object.
              Only used if ``filestore_max_inline_xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``10``


``filestore_max_inline_xattrs_other``

:Description: The maximum number of XATTRs stored in other file systems per object.
              Only used if ``filestore_max_inline_xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``2``

.. index:: filestore; synchronization

Synchronization Intervals
=========================

Filestore needs to periodically quiesce writes and synchronize the
file system, which creates a consistent commit point. It can then free journal
entries up to the commit point. Synchronizing more frequently tends to reduce
the time required to perform synchronization, and reduces the amount of data
that needs to remain in the  journal. Less frequent synchronization allows the
backing file system to coalesce small writes and metadata updates more
optimally, potentially resulting in more efficient synchronization at the
expense of potentially increasing tail latency.

``filestore_max_sync_interval``

:Description: The maximum interval in seconds for synchronizing Filestore.
:Type: Double
:Required: No
:Default: ``5``


``filestore_min_sync_interval``

:Description: The minimum interval in seconds for synchronizing Filestore.
:Type: Double
:Required: No
:Default: ``.01``


.. index:: filestore; flusher

Flusher
=======

The Filestore flusher forces data from large writes to be written out using
``sync_file_range`` before the sync in order to (hopefully) reduce the cost of
the eventual sync. In practice, disabling 'filestore_flusher' seems to improve
performance in some cases.


``filestore_flusher``

:Description: Enables the filestore flusher.
:Type: Boolean
:Required: No
:Default: ``false``

.. deprecated:: v.65

``filestore_flusher_max_fds``

:Description: Sets the maximum number of file descriptors for the flusher.
:Type: Integer
:Required: No
:Default: ``512``

.. deprecated:: v.65

``filestore_sync_flush``

:Description: Enables the synchronization flusher. 
:Type: Boolean
:Required: No
:Default: ``false``

.. deprecated:: v.65

``filestore_fsync_flushes_journal_data``

:Description: Flush journal data during file system synchronization.
:Type: Boolean
:Required: No
:Default: ``false``


.. index:: filestore; queue

Queue
=====

The following settings provide limits on the size of the Filestore queue.

``filestore_queue_max_ops``

:Description: Defines the maximum number of in progress operations the file store accepts before blocking on queuing new operations. 
:Type: Integer
:Required: No. Minimal impact on performance.
:Default: ``50``


``filestore_queue_max_bytes``

:Description: The maximum number of bytes for an operation. 
:Type: Integer
:Required: No
:Default: ``100 << 20``




.. index:: filestore; timeouts

Timeouts
========


``filestore_op_threads``

:Description: The number of file system operation threads that execute in parallel. 
:Type: Integer
:Required: No
:Default: ``2``


``filestore_op_thread_timeout``

:Description: The timeout for a file system operation thread (in seconds).
:Type: Integer
:Required: No
:Default: ``60``


``filestore_op_thread_suicide_timeout``

:Description: The timeout for a commit operation before cancelling the commit (in seconds). 
:Type: Integer
:Required: No
:Default: ``180``


.. index:: filestore; btrfs

B-Tree Filesystem
=================


``filestore_btrfs_snap``

:Description: Enable snapshots for a ``btrfs`` filestore.
:Type: Boolean
:Required: No. Only used for ``btrfs``.
:Default: ``true``


``filestore_btrfs_clone_range``

:Description: Enable cloning ranges for a ``btrfs`` filestore.
:Type: Boolean
:Required: No. Only used for ``btrfs``.
:Default: ``true``


.. index:: filestore; journal

Journal
=======


``filestore_journal_parallel``

:Description: Enables parallel journaling, default for Btrfs.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore_journal_writeahead``

:Description: Enables writeahead journaling, default for XFS.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore_journal_trailing``

:Description: Deprecated, never use.
:Type: Boolean
:Required: No
:Default: ``false``


Misc
====


``filestore_merge_threshold``

:Description: Min number of files in a subdir before merging into parent
              NOTE: A negative value means to disable subdir merging
:Type: Integer
:Required: No
:Default: ``-10``


``filestore_split_multiple``

:Description:  ``(filestore_split_multiple * abs(filestore_merge_threshold) + (rand() % filestore_split_rand_factor)) * 16``
               is the maximum number of files in a subdirectory before 
               splitting into child directories.

:Type: Integer
:Required: No
:Default: ``2``


``filestore_split_rand_factor``

:Description:  A random factor added to the split threshold to avoid
               too many (expensive) Filestore splits occurring at once. See
               ``filestore_split_multiple`` for details.
               This can only be changed offline for an existing OSD,
               via the ``ceph-objectstore-tool apply-layout-settings`` command.

:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``20``


``filestore_update_to``

:Description: Limits Filestore auto upgrade to specified version.
:Type: Integer
:Required: No
:Default: ``1000``


``filestore_blackhole``

:Description: Drop any new transactions on the floor.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore_dump_file``

:Description: File onto which store transaction dumps.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore_kill_at``

:Description: inject a failure at the n'th opportunity
:Type: String
:Required: No
:Default: ``false``


``filestore_fail_eio``

:Description: Fail/Crash on eio.
:Type: Boolean
:Required: No
:Default: ``true``

