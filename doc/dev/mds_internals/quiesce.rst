MDS Quiesce Protocol
====================

The MDS quiesce protocol is a mechanism for "quiescing" (quieting) a subvolume
in a file system, stopping all write (and most read) I/O.  A subvolume is a
subtree in the file system marked with the ``ceph.dir.subvolume`` flag (like
the ``ceph-mgr`` volumes plugin subvolumes).

.. note:: The limitation that the quiesce operation may only be applied to a
          subvolume is for ease-of-implementation. Each subvolume implicitly creates a
          ``SnapRealm`` which (a) will not have any child ``SnapRealm`` and (b) tracks
          all inodes in cache which are part of that subvolume. A future implementation
          may work for any file system path with approporiate adjustments to track
          child inodes.

The purpose of this API is to prevent multiple clients from interleaving reads
and writes across an eventually consistent snapshot barrier where out-of-band
communication exists between clients. This communication can lead to clients
wrongly believing they've reached a checkpoint that is mutually recoverable to
via a snapshot.

Mechanism
---------

The MDS quiesces I/O using a new ``quiesce_subvolume`` internal request that
obtains appropriate locks on the root of a subvolume and then launches a series
of sub-requests for locking other inodes in the subvolume. The locks obtained
will force clients to release caps and in-progress client/MDS requests to
complete.

The sub-requests launched are ``quiesce_subvolume_inode`` internal requests
which simply lock the inode, if the MDS is authoritative for the inode.
Generally, these are rdlocks (read locks) on each inode metadata lock but the
``filelock`` is xlocked (exclusively locked) because its type allows for
multiple readers and writers. Additionally, a new ``quiescelock`` is exclusively
locked (more on that next).

The inodes in a subvolume are known because subvolumes are a special kind of
``SnapRealm``. Before the quiesce protocol was written, the MDS already kept
track of inodes with issued capabilities in the ``SnapRealm``. It is further
enhanced to keep a list of all inodes in cache.

Because the ``quiesce_subvolume_inode`` request will xlock the ``filelock`` and
``quiescelock``, it is necessary to run this operation only on the
authoritative MDS. It is expected that the glue layer on top of the quiesce
protocol will execute the same ``quiesce_subvolume`` operation on each MDS
rank. This allows each rank which may be authoritative for part of the
subvolume subtree to lock all inodes it is authoritative for in the subvolume
``SnapRealm``.


Inode Quiescelock
-----------------

The ``quiescelock`` is a new lock for inodes which supports quiescing I/O.  It
is a type of superlock where every client or MDS operation which accesses an
inode lock will also implicitly acquire the ``quiescelock`` (readonly). In
general, this lock is never held except for reading. When a subtree is
quiesced, the ``quiesce_subvolume_inode`` internal operation will hold
``quiescelock`` exclusively, thereby denying the **new** acquisition of any
other inode lock.  The ``quiescelock`` must be ordered before all other locks
(see ``src/include/ceph_fs.h`` for ordering) in order to act as this superlock.

The reason for this lock is to prevent an operation from blocking on acquiring
locks held by ``quiesce_subvolume_inode`` while still holding locks obtained
during path traversal. Notably, the important locks are the ``snaplock`` and
``policylock`` obtained via ``Locker::try_rdlock_snap_layout`` on all parents
of the root inode of the request (the ``ino`` in the ``filepath`` struct). If
that operation waits with those locks held, then a future ``mksnap`` on the
subvolume inode will be impossible.

.. note:: The ``mksnap`` RPC only acquires a wrlock (write lock) on the
          ``snaplock`` for the inode to be snapshotted.

The way ``quiescelock`` helps prevent this is by being the first **mandatory**
lock acquired and special handling when it cannot be acquired: all locks held
by the operation are dropped and the operation waits for the ``quiescelock`` to
be available.  The lock is mandatory in that all inode locks automatically
include (add) the ``quiescelock`` when calling ``Locker::acquire_locks``. So
the expected normal flow is that an operation like ``getattr`` will perform its
path traversal, acquiring parent and dentry locks, then attempt to acquire
locks on the inode necessary for the requested client caps. The operation will
fail to acquire the automatically included ``quiescelock``, add itself to the
``quiescelock`` wait list, and then drop all held locks.

There is a divergence in locking behavior for the root of the subvolume. The
``quiescelock`` is only locked read-only. This allows the inode to be accessed
by operations like ``mksnap`` which will implicitly acquire the ``quiescelock``
read-only when locking the ``snaplock`` for writing. Additionally, if
``Locker::acquire_locks`` will only acquire read locks without waiting, then it
will skip the read-only lock on ``quiescelock``. This is to allow some forms of
``lookup`` nececessary for subvolume management at higher layers.


Readable quiesced subvolume
---------------------------

It may be desirable to allow readers to continue accessing a quiesced
subvolume. One way to do that is to have a separate superlock (yuck) for read
access, say ``quiescerlock``. If a "readable" quiesce is performed, then
``quiescerlock`` is not xlocked by ``quiesce_subvolume_inode``. Read locks on
other (non-quiesce) locks will acquire a read lock only on ``quiescerlock`` and
no longer on ``quiescelock``.  Write locks would try to acquire both
``quiescelock`` and ``quiescerlock`` (since writes may also read).

Ideally, it may be a new lock type could be used to handle both cases but no
such lock type yet exists.
