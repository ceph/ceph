MDS Quiesce Protocol
====================

The MDS quiesce protocol is a mechanism for "quiescing" (quieting) a tree in a
file system, stopping all write (and sometimes incidentally read) I/O.

The purpose of this API is to prevent multiple clients from interleaving reads
and writes across an eventually consistent snapshot barrier where out-of-band
communication exists between clients. This communication can lead to clients
wrongly believing they've reached a checkpoint that is mutually recoverable to
via a snapshot.

.. note:: This is documentation for the low-level mechanism in the MDS for
          quiescing a tree of files. The higher-level QuiesceDb is the
          intended API for clients to effect a quiesce.


Mechanism
---------

The MDS quiesces I/O using a new ``quiesce_path`` internal request that obtains
appropriate locks on the root of a tree and then launches a series of
sub-requests for locking other inodes in the tree. The locks obtained will
force clients to release caps and in-progress client/MDS requests to complete.

The sub-requests launched are ``quiesce_inode`` internal requests. These will
obtain "cap-related" locks which control capability state, including the
``filelock``, ``authlock``, ``linklock``, and ``xattrlock``. Additionally, the
new local lock ``quiescelock`` is acquired. More information on that lock in
the next section.

Locks that are not cap-related are skipped because they do not control typical
and durable metadata state. Additionally, only Capabilities can give a client
local control of a file's metadata or data.

Once all locks have been acquired, the cap-related locks are released and the
``quiescelock`` is relied on to prevent issuing Capabilities to clients for the
cap-related locks. This is controlled primarily by ``CInode:get_caps_*``
methods. Releasing these locks is necessary to allow other ranks with the
replicated inode to quiesce without lock state transitions resulting in
deadlock. For example, a client wanting ``Xx`` on an inode will trigger a
``xattrlock`` in ``LOCK_SYNC`` state to transition to ``LOCK_SYNC_EXCL``.  That
state would not allow another rank to acquire ``xattrlock`` for reading,
thereby creating deadlock, subject to quiesce timeout/expiration. (Quiesce
cannot complete until all ranks quiesce the tree.)

Finally, if the inode is a directory, the ``quiesce_inode`` operation traverses
all directory fragments and issues new ``quiesce_inode`` requests for any child
inodes.


Inode Quiescelock
-----------------

The ``quiescelock`` is a new local lock for inodes which supports quiescing
I/O.  It is a type of superlock where every client or MDS operation which
requires a wrlock or xlock on a "cap-related" inode lock will also implicitly
acquire a wrlock on the ``quiescelock``.

.. note:: A local lock supports multiple writers and only one exclusive locker. No read locks.

During normal operation in the MDS, the ``quiescelock`` is never held except
for writing. However, when a subtree is quiesced, the ``quiesce_inode``
internal operation will hold ``quiescelock`` exclusively for the entire
lifetime of the ``quiesce_inode`` operation. This will deny the **new**
acquisition of any other cap-related inode lock.  The ``quiescelock`` must be ordered
before all other locks (see ``src/include/ceph_fs.h`` for ordering) in order to
act as this superlock.

One primary reason for this ``quiescelock`` is to prevent a client request from
blocking on acquiring locks held by ``quiesce_inode`` (e.g. ``filelock`` or
``quiescelock``) while still holding locks obtained during normal path
traversal. Notably, the important locks are the ``snaplock`` and ``policylock``
obtained via ``Locker::try_rdlock_snap_layout`` on all parents of the root
inode of the request (the ``ino`` in the ``filepath`` struct). If that
operation waits with those locks held, then a future ``mksnap`` on the root
inode will be impossible.

.. note:: The ``mksnap`` RPC only acquires a wrlock (write lock) on the
          ``snaplock`` for the inode to be snapshotted.

The way ``quiescelock`` helps prevent this is by being the first **mandatory**
lock acquired when acquiring a wrlock or xlock on a cap-related lock.
Additionally, there is also special handling when it cannot be acquired: all
locks held by the operation are dropped and the operation waits for the
``quiescelock`` to be available. The lock is mandatory in that a call to
``Locker::acquire_locks`` with a wrlock/xlock on a cap-related lock  will
automatically include (add) the ``quiescelock``.

So, the expected normal flow is that an operation like ``mkdir`` will perform
its path traversal, acquiring parent and dentry locks, then attempt to acquire
locks on the parent inode necessary for the creation of a dentry. The operation
will fail to acquire a wrlock on the automatically included ``quiescelock``,
add itself to the ``quiescelock`` wait list, and then drop all held locks.


Lookups and Exports
-------------------

Quiescing a tree results in a number of ``quiesce_inode`` operations for each
inode under the tree. Those operations have a shared lifetime tied to the
parent ``quiesce_path`` operation. So, once operations complete quiesce (but do
not finish and release locks), the operations sit with locks held and do not
monitor the state of the tree. This means we need to handle cases where new
metadata is imported.

If an inode is fetched via a directory ``lookup`` or ``readdir``, the MDS will
check if its parent is quiesced (i.e. is the parent directory ``quiescelock``
xlocked?). If so, the MDS will immediately issue an dispatch a
``quiesce_inode`` operation for that inode. Because it's a fresh inode, the
operation will immediately succeed and prevent the client from being issued
inappropriate capabailities.

The second case is handling subtree imports from another rank. This is
problematic since the subtree import may have inodes with inappropriate state
that would invalidate the guarantees of the reportedly "quiesced" tree. To
avoid this, importer MDS will skip discovery of the root inode for an import if
it encounters a directory inode that is quiesced. If skipped, the rank
will send a NAK message back to the exporter which will abort the export.
