MDS Journaling
==============

CephFS Metadata Pool
--------------------

CephFS uses a separate (metadata) pool for managing file metadata (inodes and
dentries) in a Ceph File System. The metadata pool has all the information about
files in a Ceph File System including the File System hierarchy. Additionally,
CephFS maintains meta information related to other entities in a file system
such as file system journals, open file table, session map, etc.

This document describes how Ceph Metadata Servers use and rely on journaling.

CephFS MDS Journaling
---------------------

CephFS metadata servers stream a journal of metadata events into RADOS in the metadata
pool prior to executing a file system operation. Active MDS daemon(s) manage metadata
for files and directories in CephFS.

CephFS uses journaling for couple of reasons:

#. Consistency: On an MDS failover, the journal events can be replayed to reach a
   consistent file system state. Also, metadata operations that require multiple
   updates to the backing store need to be journaled for crash consistency (along
   with other consistency mechanisms such as locking, etc..).

#. Performance: Journal updates are (mostly) sequential, hence updates to journals
   are fast. Furthermore, updates can be batched into single write, thereby saving
   disk seek time involved in updates to different parts of a file. Having a large
   journal also helps a standby MDS to warm its cache which helps indirectly during
   MDS failover.

Each active metadata server maintains its own journal in the metadata pool. Journals
are striped over multiple objects. Journal entries which are not required (deemed as
old) are trimmed by the metadata server.

Journal Events
--------------

Apart from journaling file system metadata updates, CephFS journals various other events
such as client session info and directory import/export state to name a few. These events
are used by the metadata sever to reestablish correct state as required, e.g., Ceph MDS
tries to reconnect clients on restart when journal events get replayed and a specific
event type in the journal specifies that a client entity type has a session with the MDS
before it was restarted.

To examine the list of such events recorded in the journal, CephFS provides a command
line utility `cephfs-journal-tool` which can be used as follows:

::

   cephfs-journal-tool --rank=<fs>:<rank> event get list

`cephfs-journal-tool` is also used to discover and repair a damaged Ceph File System.
(See :doc:`/cephfs/cephfs-journal-tool` for more details)

Journal Event Types
-------------------

Following are various event types that are journaled by the MDS.

#. `EVENT_COMMITTED`: Mark a request (id) as committed.

#. `EVENT_EXPORT`: Maps directories to an MDS rank.

#. `EVENT_FRAGMENT`: Tracks various stages of directory fragmentation (split/merge).

#. `EVENT_IMPORTSTART`: Logged when an MDS rank starts importing directory fragments.

#. `EVENT_IMPORTFINISH`: Logged when an MDS rank finishes importing directory fragments.

#. `EVENT_NOOP`: No operation event type for skipping over a journal region.

#. `EVENT_OPEN`: Tracks which inodes have open file handles.

#. `EVENT_RESETJOURNAL`: Used to mark a journal as `reset` post truncation.

#. `EVENT_SESSION`: Tracks open client sessions.

#. `EVENT_SLAVEUPDATE`: Logs various stages of an operation that has been forwarded to a (slave) mds.

#. `EVENT_SUBTREEMAP`: Map of directory inodes to directory contents (subtree partition).

#. `EVENT_TABLECLIENT`: Log transition states of MDSs view of client tables (snap/anchor).

#. `EVENT_TABLESERVER`: Log transition states of MDSs view of server tables (snap/anchor).

#. `EVENT_UPDATE`: Log file operations on an inode.
