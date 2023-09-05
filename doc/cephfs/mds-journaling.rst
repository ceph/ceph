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

#. `EVENT_SEGMENT`: Log a new journal segment boundary.

#. `EVENT_LID`: Mark the beginning of a journal without a logical subtree map.

Journal Segments
----------------

The MDS journal is composed of logical segments, called LogSegments in the
code. These segments are used to collect metadata updates by multiple events
into one logical unit for the purposes of trimming. Whenever the journal tries
to commit metadata operations (e.g. flush a file create out as an omap update
to a dirfrag object), it does so in a replayable batch of updates from the
LogSegment. The updates must be replayable in case the MDS fails during the
series of updates to various metadata objects. The reason the updates are
performed in batch is to group updates to the same metadata object (a dirfrag)
where multiple omap entries are probably updated in the same time period.

Once a segment is trimmed, it is considered "expired". An expired segment is
eligible for deletion by the journaler as all of its updates are flushed to the
backing RADOS objects. This is done by updating the "expire position" of the
journaler to advance past the end of the expired segment. Some expired segments
may be kept in the journal to improve cache locality when the MDS restarts.

For most of CephFS's history (up to 2023), the journal segments were delineated
by subtree maps, the ``ESubtreeMap`` event. The major reason for this is that
journal recovery must start with a copy of the subtree map before replaying any
other events.

Now, log segments can be delineated by events which are a ``SegmentBoundary``.
These include, ``ESubtreeMap``, ``EResetJournal``, ``ESegment`` (2023), or
``ELid`` (2023).  For ``ESegment``, this light-weight segment boundary allows
the MDS to journal the subtree map less frequently while also keeping the
journal segments small to keep trimming events short.  In order to maintain the
constraint that the first event journal replay sees is the ``ESubtreeMap``,
those segments beginning with that event are considered "major segments" and a
new constraint was added to the deletion of expired segments: the first segment
of the journal must always be a major segment.

The ``ELid`` event exists to mark the MDS journal as "new" where a logical
``LogSegment`` and log sequence number is required for other operations to
proceed, in particular the MDSTable operations. The MDS uses this event when
creating a rank or shutting it down. No subtree map is required when replaying
the rank from this initial state.


Configurations
--------------

The targetted size of a log segment in terms of number of events is controlled by:

.. confval:: mds_log_events_per_segment

The frequency of major segments (noted by the journaling of the latest ``ESubtreeMap``) is controlled by:

.. confval:: mds_log_major_segment_event_ratio

When ``mds_log_events_per_segment * mds_log_major_segment_event_ratio``
non-``ESubtreeMap`` events are logged, the MDS will journal a new
``ESubtreeMap``. This is necessary to allow the journal to shrink in size
during the trimming of expired segments.

The target maximum number of segments is controlled by:

.. confval:: mds_log_max_segments

The MDS will often sit a little above this number due to non-major segments
awaiting trimming up to the next major segment.
