============================
 Filestore Config Reference
============================


Extended Attributes
===================


``filestore debug omap check``

:Description: Debugging check on synchronization. Expensive. For debugging only.
:Type: Boolean
:Required: No
:Default: ``0``


``filestore xattr use omap``

:Description: Use object map for XATTRS. Set to ``true`` for ``ext4`` file systems. 
:Type: Boolean
:Required: No
:Default: ``false``


``filestore max inline xattr size``

:Description: The maximum size of an inlined XATTR in bytes. 
:Type: Unsigned 32-bit Integer
:Required: No
:Default: 512


``filestore max inline xattrs``

:Description: 
:Type: 32-bit Integer
:Required: No
:Default: ``2``


Synchronization Intervals
=========================

Periodically, the filestore needs to quiesce writes and synchronize the filesystem,
which creates a consistent commit point. It can then free journal entries up to
the commit point. Synchronizing more frequently tends to reduce the time required 
perform synchronization, and reduces the amount of data that needs to remain in the 
journal. Less frequent synchronization allows the backing filesystem to coalesce 
small writes and metadata updates more optimally--potentially resulting in more
efficient synchronization.


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


Flusher
=======

The filestore flusher forces data from large writes to be written out
using ``sync file range``
before the sync in order to (hopefully) reduce the cost of the
eventual sync. In practice,
disabling 'filestore flusher' seems to improve performance in some cases.


``filestore flusher``

:Description: Enables the filestore flusher.
:Type: Boolean
:Required: No
:Default: ``false``


``filestore flusher max fds``

:Description: Sets the maximum number of file descriptors for the flusher.
:Type: Integer
:Required: No
:Default: ``512``

``filestore sync flush``

:Description: Enables the synchronization flusher. 
:Type: Boolean
:Required: No
:Default: ``false``


``filestore fsync flushes journal data``

:Description: Flush journal data during filesystem synchronization.
:Type: Boolean
:Required: No
:Default: ``false``


Queue
=====


``filestore queue max ops``

:Description: Defines the maximum number of in progress operations the file store accepts before blocking on queing new operations. 
:Type: Integer
:Required: No. Minimal impact on performance.
:Default: ``500``


``filestore queue max bytes``

:Description: The maximum number of bytes for an operation. 
:Type: Integer
:Required: No
:Default: ``100 << 20``


``filestore queue committing max ops``

:Description: The maximum number of operations the filestore can commit. 
:Type: Integer
:Required: No
:Default: ``500``


``filestore queue committing max bytes``

:Description: The maximum number of bytes the filestore can commit.
:Type: Integer
:Required: No
:Default: ``100 << 20``



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


Extent Mapping
==============


``filestore fiemap``

:Description: Allows an OSD to determine which bits of a file have been written. For efficient sparse reads. 
:Type: Boolean
:Required: No
:Default: ``false``


OPTION(filestore_fiemap_threshold, OPT_INT, 4096)





//Todo:


``filestore``

:Description: IGNORE FOR NOW
:Type: Boolean
:Required: No
:Default: ``false``


OPTION(filestore_journal_parallel, OPT_BOOL, false)
OPTION(filestore_journal_writeahead, OPT_BOOL, false)
OPTION(filestore_journal_trailing, OPT_BOOL, false)
OPTION(filestore_merge_threshold, OPT_INT, 10)
OPTION(filestore_split_multiple, OPT_INT, 2)
OPTION(filestore_update_to, OPT_INT, 1000)
OPTION(filestore_blackhole, OPT_BOOL, false)     // drop any new transactions on the floor
OPTION(filestore_dump_file, OPT_STR, "")         // file onto which store transaction dumps
OPTION(filestore_kill_at, OPT_INT, 0)            // inject a failure at the n'th opportunity
OPTION(filestore_fail_eio, OPT_BOOL, true)       // fail/crash on EIO
