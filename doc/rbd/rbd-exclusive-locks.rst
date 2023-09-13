.. _rbd-exclusive-locks:

====================
 RBD Exclusive Locks
====================

.. index:: Ceph Block Device; RBD exclusive locks; exclusive-lock

Exclusive locks are mechanisms designed to prevent multiple processes from
accessing the same Rados Block Device (RBD) in an uncoordinated fashion.
Exclusive locks are used heavily in virtualization (where they prevent VMs from
clobbering each other's writes) and in `RBD mirroring`_ (where they are a
prerequisite for journaling in journal-based mirroring and fast generation of
incremental diffs in snapshot-based mirroring).

The ``exclusive-lock`` feature is enabled on newly created images. This default
can be overridden via the ``rbd_default_features`` configuration option or the
``--image-feature`` and ``--image-shared`` options for ``rbd create`` command.

.. note::
   Many image features, including ``object-map`` and ``fast-diff``, depend upon
   exclusive locking. Disabling the ``exclusive-lock`` feature will negatively
   affect the performance of some operations.

To maintain multi-client access, the ``exclusive-lock`` feature implements
automatic cooperative lock transitions between clients. It ensures that only
a single client can write to an RBD image at any given time and thus protects
internal image structures such as the object map, the journal or the `PWL
cache`_ from concurrent modification.

Exclusive locking is mostly transparent to the user:

* Whenever a client (a ``librbd`` process or, in case of a ``krbd`` client,
  a client node's kernel) needs to handle a write to an RBD image on which
  exclusive locking has been enabled, it first acquires an exclusive lock on
  the image. If the lock is already held by some other client, that client is
  requested to release it.

* Whenever a client that holds an exclusive lock on an RBD image gets
  a request to release the lock, it stops handling writes, flushes its caches
  and releases the lock.

* Whenever a client that holds an exclusive lock on an RBD image terminates
  gracefully, the lock is also released gracefully.

* A graceful release of an exclusive lock on an RBD image (whether by request
  or due to client termination) enables another, subsequent, client to acquire
  the lock and start handling writes.

.. warning::
   By default, the ``exclusive-lock`` feature does not prevent two or more
   concurrently running clients from opening the same RBD image and writing to
   it in turns (whether on the same node or not). In effect, their writes just
   get linearized as the lock is automatically transitioned back and forth in
   a cooperative fashion.

.. note::
   To disable automatic lock transitions between clients, the
   ``RBD_LOCK_MODE_EXCLUSIVE`` flag may be specified when acquiring the
   exclusive lock. This is exposed by the ``--exclusive`` option for ``rbd
   device map`` command.


Blocklisting
============

Sometimes a client that previously held an exclusive lock on an RBD image does
not terminate gracefully, but dies abruptly. This may be because the client
process received a ``KILL`` or ``ABRT`` signal, or because the client node
underwent a hard reboot or suffered a power failure. In cases like this, the
lock is never gracefully released. This means that any new client that comes up
and attempts to write to the image must break the previously held exclusive
lock.

However, a process (or kernel thread) may hang or merely lose network
connectivity to the Ceph cluster for some amount of time. In that case,
breaking the lock would be potentially catastrophic: the hung process or
connectivity issue could resolve itself and the original process might then
compete with one that started in the interim, thus accessing RBD data in an
uncoordinated and destructive manner.

In the event that a lock cannot be acquired in the standard graceful manner,
the overtaking process not only breaks the lock but also blocklists the
previous lock holder. This is negotiated between the new client process and the
Ceph Monitor. 

* Upon receiving the blocklist request, the monitor instructs the relevant OSDs
  to no longer serve requests from the old client process;
* after the associated OSD map update is complete, the new client can break the
  previously held lock;
* after the new client has acquired the lock, it can commence writing
  to the image.

Blocklisting is thus a form of storage-level resource `fencing`_.

.. note::
   In order for blocklisting to work, the client must have the ``osd
   blocklist`` capability. This capability is included in the ``profile
   rbd`` capability profile, which should be set generally on all Ceph
   :ref:`client identities <user-management>` using RBD.

.. _RBD mirroring: ../rbd-mirroring
.. _PWL cache: ../rbd-persistent-write-log-cache
.. _fencing: https://en.wikipedia.org/wiki/Fencing_(computing)
