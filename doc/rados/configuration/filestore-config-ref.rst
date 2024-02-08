============================
 Filestore Config Reference
============================

.. note:: Since the Luminous release of Ceph, Filestore has not been Ceph's
   default storage back end. Since the Luminous release of Ceph, BlueStore has
   been Ceph's default storage back end. However, Filestore OSDs are still
   supported up to Quincy. Filestore OSDs are not supported in Reef. See
   :ref:`OSD Back Ends <rados_config_storage_devices_osd_backends>`. See
   :ref:`BlueStore Migration <rados_operations_bluestore_migration>` for
   instructions explaining how to replace an existing Filestore back end with a
   BlueStore back end.


``filestore_debug_omap_check``

:Description: Debugging check on synchronization. Expensive. For debugging only.
:Type: Boolean
:Required: No
:Default: ``false``


.. index:: filestore; extended attributes

Extended Attributes
===================

Extended Attributes (XATTRs) are important for Filestore OSDs. However, Certain
disadvantages can occur when the underlying file system is used for the storage
of XATTRs: some file systems have limits on the number of bytes that can be
stored in XATTRs, and your file system might in some cases therefore run slower
than would an alternative method of storing XATTRs. For this reason, a method
of storing XATTRs extrinsic to the underlying file system might improve
performance. To implement such an extrinsic method, refer to the following
settings.

If the underlying file system has no size limit, then Ceph XATTRs are stored as
``inline xattr``, using the XATTRs provided by the file system. But if there is
a size limit (for example, ext4 imposes a limit of 4 KB total), then some Ceph
XATTRs will be stored in a key/value database when the limit is reached. More
precisely, this begins to occur when either the
``filestore_max_inline_xattr_size`` or ``filestore_max_inline_xattrs``
threshold is reached.


``filestore_max_inline_xattr_size``

:Description: Defines the maximum size per object of an XATTR that can be
              stored in the file system (for example, XFS, Btrfs, ext4). The
              specified size should not be larger than the file system can
              handle. Using the default value of 0 instructs Filestore to use
              the value specific to the file system.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``0``


``filestore_max_inline_xattr_size_xfs``

:Description: Defines the maximum size of an XATTR that can be stored in the
              XFS file system.  This setting is used only if
              ``filestore_max_inline_xattr_size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``65536``


``filestore_max_inline_xattr_size_btrfs``

:Description: Defines the maximum size of an XATTR that can be stored in the
              Btrfs file system.  This setting is used only if
              ``filestore_max_inline_xattr_size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``2048``


``filestore_max_inline_xattr_size_other``

:Description: Defines the maximum size of an XATTR that can be stored in other file systems.
              This setting is used only if ``filestore_max_inline_xattr_size`` == 0.
:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``512``


``filestore_max_inline_xattrs``

:Description: Defines the maximum number of XATTRs per object that can be stored in the file system.
              Using the default value of 0 instructs Filestore to use the value specific to the file system.
:Type: 32-bit Integer
:Required: No
:Default: ``0``


``filestore_max_inline_xattrs_xfs``

:Description: Defines the maximum number of XATTRs per object that can be stored in the XFS file system.
              This setting is used only if ``filestore_max_inline_xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``10``


``filestore_max_inline_xattrs_btrfs``

:Description: Defines the maximum number of XATTRs per object that can be stored in the Btrfs file system.
              This setting is used only if ``filestore_max_inline_xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``10``


``filestore_max_inline_xattrs_other``

:Description: Defines the maximum number of XATTRs per object that can be stored in other file systems.
              This setting is used only if ``filestore_max_inline_xattrs`` == 0.
:Type: 32-bit Integer
:Required: No
:Default: ``2``

.. index:: filestore; synchronization

Synchronization Intervals
=========================

Filestore must periodically quiesce writes and synchronize the file system.
Each synchronization creates a consistent commit point. When the commit point
is created, Filestore is able to free all journal entries up to that point.
More-frequent synchronization tends to reduce both synchronization time and
the amount of data that needs to remain in the journal. Less-frequent
synchronization allows the backing file system to coalesce small writes and
metadata updates, potentially increasing synchronization
efficiency but also potentially increasing tail latency.


``filestore_max_sync_interval``

:Description: Defines the maximum interval (in seconds) for synchronizing Filestore.
:Type: Double
:Required: No
:Default: ``5``


``filestore_min_sync_interval``

:Description: Defines the minimum interval (in seconds) for synchronizing Filestore.
:Type: Double
:Required: No
:Default: ``.01``


.. index:: filestore; flusher

Flusher
=======

The Filestore flusher forces data from large writes to be written out using
``sync_file_range`` prior to the synchronization.
Ideally, this action reduces the cost of the eventual synchronization. In practice, however, disabling
'filestore_flusher' seems in some cases to improve performance.


``filestore_flusher``

:Description: Enables the Filestore flusher.
:Type: Boolean
:Required: No
:Default: ``false``

.. deprecated:: v.65

``filestore_flusher_max_fds``

:Description: Defines the maximum number of file descriptors for the flusher.
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

:Description: Flushes journal data during file-system synchronization.
:Type: Boolean
:Required: No
:Default: ``false``


.. index:: filestore; queue

Queue
=====

The following settings define limits on the size of the Filestore queue:

``filestore_queue_max_ops``

:Description: Defines the maximum number of in-progress operations that Filestore accepts before it blocks the queueing of any new operations. 
:Type: Integer
:Required: No. Minimal impact on performance.
:Default: ``50``


``filestore_queue_max_bytes``

:Description: Defines the maximum number of bytes permitted per operation.
:Type: Integer
:Required: No
:Default: ``100 << 20``


.. index:: filestore; timeouts

Timeouts
========

``filestore_op_threads``

:Description: Defines the number of file-system operation threads that execute in parallel. 
:Type: Integer
:Required: No
:Default: ``2``


``filestore_op_thread_timeout``

:Description: Defines the timeout (in seconds) for a file-system operation thread.
:Type: Integer
:Required: No
:Default: ``60``


``filestore_op_thread_suicide_timeout``

:Description: Defines the timeout (in seconds) for a commit operation before the commit is cancelled.
:Type: Integer
:Required: No
:Default: ``180``


.. index:: filestore; btrfs

B-Tree Filesystem
=================


``filestore_btrfs_snap``

:Description: Enables snapshots for a ``btrfs`` Filestore.
:Type: Boolean
:Required: No. Used only for ``btrfs``.
:Default: ``true``


``filestore_btrfs_clone_range``

:Description: Enables cloning ranges for a ``btrfs`` Filestore.
:Type: Boolean
:Required: No. Used only for ``btrfs``.
:Default: ``true``


.. index:: filestore; journal

Journal
=======


``filestore_journal_parallel``

:Description: Enables parallel journaling, default for ``btrfs``.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore_journal_writeahead``

:Description: Enables write-ahead journaling, default for XFS.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore_journal_trailing``

:Description: Deprecated. **Never use.**
:Type: Boolean
:Required: No
:Default: ``false``


Misc
====


``filestore_merge_threshold``

:Description: Defines the minimum number of files permitted in a subdirectory before the subdirectory is merged into its parent directory.
              NOTE: A negative value means that subdirectory merging is disabled.
:Type: Integer
:Required: No
:Default: ``-10``


``filestore_split_multiple``

:Description:  ``(filestore_split_multiple * abs(filestore_merge_threshold) + (rand() % filestore_split_rand_factor)) * 16``
               is the maximum number of files permitted in a subdirectory
               before the subdirectory is split into child directories.

:Type: Integer
:Required: No
:Default: ``2``


``filestore_split_rand_factor``

:Description:  A random factor added to the split threshold to avoid
               too many (expensive) Filestore splits occurring at the same time.
               For details, see ``filestore_split_multiple``.
               To change this setting for an existing OSD, it is necessary to take the OSD
               offline before running the ``ceph-objectstore-tool apply-layout-settings`` command.

:Type: Unsigned 32-bit Integer
:Required: No
:Default: ``20``


``filestore_update_to``

:Description: Limits automatic upgrades to a specified version of Filestore. Useful in cases in which you want to avoid upgrading to a specific version.
:Type: Integer
:Required: No
:Default: ``1000``


``filestore_blackhole``

:Description: Drops any new transactions on the floor, similar to redirecting to NULL. 
:Type: Boolean
:Required: No
:Default: ``false``


``filestore_dump_file``

:Description: Defines the file that transaction dumps are stored on.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore_kill_at``

:Description: Injects a failure at the *n*\th opportunity.
:Type: String
:Required: No
:Default: ``false``


``filestore_fail_eio``

:Description: Fail/Crash on EIO.
:Type: Boolean
:Required: No
:Default: ``true``
