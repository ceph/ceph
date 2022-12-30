.. _rbd-exclusive-locks:

====================
 RBD Exclusive Locks
====================

.. index:: Ceph Block Device; RBD exclusive locks; exclusive-lock

Exclusive locks are mechanisms designed to prevent multiple processes from
accessing the same Rados Block Device (RBD) in an uncoordinated fashion.
Exclusive locks are used heavily in virtualization (where they prevent VMs from
clobbering each other's writes) and in RBD mirroring (where they are a
prerequisite for journaling).

By default, exclusive locks are enabled on newly created images. This default
can be overridden via the ``rbd_default_features`` configuration option or the
``--image-feature`` option for ``rbd create``.

.. note::
   Many image features, including ``object-map`` and ``fast-diff``, depend upon
   exclusive locking. Disabling the ``exclusive-lock`` feature will negatively
   affect the performance of some operations.

In order to ensure proper exclusive locking operations, any client using an RBD
image whose ``exclusive-lock`` feature is enabled must have a CephX identity
whose capabilities include ``profile rbd``.

Exclusive locking is mostly transparent to the user.

#. Whenever any ``librbd`` client process or kernel RBD client
   starts using an RBD image on which exclusive locking has been
   enabled, it obtains an exclusive lock on the image before the first
   write.

#. Whenever any such client process terminates gracefully, the process
   relinquishes the lock automatically.

#. This graceful termination enables another, subsequent, process to acquire
   the lock and to write to the image.

.. note::
   It is possible for two or more concurrently running processes to open the
   image and to read from it. The client acquires the exclusive lock only when
   attempting to write to the image. To disable transparent lock transitions
   between multiple clients, the client must acquire the lock by using the
   ``RBD_LOCK_MODE_EXCLUSIVE`` flag.


Blacklisting
============

Sometimes a client process (or, in case of a krbd client, a client node's
kernel) that previously held an exclusive lock on an image does not terminate
gracefully, but dies abruptly. This may be because the client process received
a ``KILL`` or ``ABRT`` signal, or because the client node underwent a hard
reboot or suffered a power failure. In cases like this, the exclusive lock is
never gracefully released. This means that any new process that starts and
attempts to use the device must break the previously held exclusive lock.

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

In order for blocklisting to work, the client must have the ``osd
blocklist`` capability. This capability is included in the ``profile
rbd`` capability profile, which should be set generally on all Ceph
:ref:`client identities <user-management>` using RBD.

.. _fencing: https://en.wikipedia.org/wiki/Fencing_(computing)
