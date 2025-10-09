CephFS Snapshots
================

CephFS supports snapshots, generally created by invoking mkdir within the
``.snap`` directory. Note this is a hidden, special directory, not visible
during a directory listing.

Overview
-----------

Generally, snapshots do what they sound like: they create an immutable view
of the file system at the point in time they're taken. There are some headline
features that make CephFS snapshots different from what you might expect:

* Arbitrary subtrees. Snapshots are created within any directory you choose,
  and cover all data in the file system under that directory.
* Asynchronous. If you create a snapshot, buffered data is flushed out lazily,
  including from other clients. As a result, "creating" the snapshot is
  very fast.

Important Data Structures
-------------------------
* SnapRealm: A `SnapRealm` is created whenever you create a snapshot at a new
  point in the hierarchy (or, when a snapshotted inode is move outside of its
  parent snapshot). SnapRealms contain an `sr_t srnode`, and `inodes_with_caps`
  that are part of the snapshot. Clients also have a SnapRealm concept that
  maintains less data but is used to associate a `SnapContext` with each open
  file for writing.
* sr_t: An `sr_t` is the on-disk snapshot metadata. It is part of the containing
  directory and contains sequence counters, timestamps, the list of associated
  snapshot IDs, and `past_parent_snaps`.
* SnapServer: SnapServer manages snapshot ID allocation, snapshot deletion and
  tracks list of effective snapshots in the file system. A file system only has
  one instance of snapserver.
* SnapClient: SnapClient is used to communicate with snapserver, each MDS rank
  has its own snapclient instance. SnapClient also caches effective snapshots
  locally.

Creating a snapshot
-------------------
CephFS snapshot feature is enabled by default on new file system. To enable it
on existing file systems, use command below.

.. code::

       $ ceph fs set <fs_name> allow_new_snaps true

When snapshots are enabled, all directories in CephFS will have a special
``.snap`` directory. (You may configure a different name with the ``client
snapdir`` setting if you wish.)

To create a CephFS snapshot, create a subdirectory under
``.snap`` with a name of your choice. For example, to create a snapshot on
directory "/1/2/3/", invoke ``mkdir /1/2/3/.snap/my-snapshot-name`` .

.. note::
   Snapshot names can not start with an underscore ('_'), as these names are
   reserved for internal usage.

.. note::
   Snapshot names can not exceed 240 characters.  This is because the MDS makes
   use of long snapshot names internally, which follow the format:
   `_<SNAPSHOT-NAME>_<INODE-NUMBER>`.  Since filenames in general can't have
   more than 255 characters, and `<node-id>` takes 13 characters, the long
   snapshot names can take as much as 255 - 1 - 1 - 13 = 240.

This is transmitted to the MDS Server as a
CEPH_MDS_OP_MKSNAP-tagged `MClientRequest`, and initially handled in
Server::handle_client_mksnap(). It allocates a `snapid` from the `SnapServer`,
projects a new inode with the new SnapRealm, and commits it to the MDLog as
usual. When committed, it invokes
`MDCache::do_realm_invalidate_and_update_notify()`, which notifies all clients
with caps on files under "/1/2/3/", about the new SnapRealm. When clients get
the notifications, they update client-side SnapRealm hierarchy, link files
under "/1/2/3/" to the new SnapRealm and generate a `SnapContext` for the
new SnapRealm.

Note that this *is not* a synchronous part of the snapshot creation!

Updating a snapshot
-------------------
If you delete a snapshot, a similar process is followed. If you remove an inode
out of its parent SnapRealm, the rename code creates a new SnapRealm for the
renamed inode (if SnapRealm does not already exist), saves IDs of snapshots that
are effective on the original parent SnapRealm into `past_parent_snaps` of the
new SnapRealm, then follows a process similar to creating snapshot.

Generating a SnapContext
------------------------
A RADOS `SnapContext` consists of a snapshot sequence ID (`snapid`) and all
the snapshot IDs that an object is already part of. To generate that list, we
combine `snapids` associated with the SnapRealm and all valid `snapids` in
`past_parent_snaps`. Stale `snapids` are filtered out by SnapClient's cached
effective snapshots.

Storing snapshot data
---------------------
File data is stored in RADOS "self-managed" snapshots. Clients are careful to
use the correct `SnapContext` when writing file data to the OSDs.

Storing snapshot metadata
-------------------------
Snapshotted dentries (and their inodes) are stored in-line as part of the
directory they were in at the time of the snapshot. *All dentries* include a
`first` and `last` snapid for which they are valid. (Non-snapshotted dentries
will have their `last` set to CEPH_NOSNAP).

Snapshot writeback
------------------
There is a great deal of code to handle writeback efficiently. When a Client
receives an `MClientSnap` message, it updates the local `SnapRealm`
representation and its links to specific `Inodes`, and generates a `CapSnap`
for the `Inode`. The `CapSnap` is flushed out as part of capability writeback,
and if there is dirty data the `CapSnap` is used to block fresh data writes
until the snapshot is completely flushed to the OSDs.

In the MDS, we generate snapshot-representing dentries as part of the regular
process for flushing them. Dentries with outstanding `CapSnap` data is kept
pinned and in the journal.

Deleting snapshots
------------------
Snapshots are deleted by invoking "rmdir" on the ".snap" directory they are
rooted in. (Attempts to delete a directory which roots snapshots *will fail*;
you must delete the snapshots first.) Once deleted, they are entered into the
`OSDMap` list of deleted snapshots and the file data is removed by the OSDs.
Metadata is cleaned up as the directory objects are read in and written back
out again.

Hard links
----------
Inode with multiple hard links is moved to a dummy global SnapRealm. The
dummy SnapRealm covers all snapshots in the file system. The inode's data
will be preserved for any new snapshot. These preserved data will cover
snapshots on any linkage of the inode.

Multi-FS
---------
Snapshots and multiple file systems don't interact well. Specifically, each
MDS cluster allocates `snapids` independently; if you have multiple file systems
sharing a single pool (via namespaces), their snapshots *will* collide and
deleting one will result in missing file data for others. (This may even be
invisible, not throwing errors to the user.) If each FS gets its own
pool things probably work, but this isn't tested and may not be true.

.. Note:: To avoid snap id collision between mon-managed snapshots and file system
   snapshots, pools with mon-managed snapshots are not allowed to be attached
   to a file system. Also, mon-managed snapshots can't be created in pools
   already attached to a file system either.
