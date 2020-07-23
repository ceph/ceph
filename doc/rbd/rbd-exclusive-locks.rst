.. _rbd-exclusive-locks:

====================
 RBD Exclusive Locks
====================

.. index:: Ceph Block Device; RBD exclusive locks; exclusive-lock

Exclusive locks are a mechanism designed to prevent multiple processes
from accessing the same Rados Block Device (RBD) in an uncoordinated
fashion. Exclusive locks are heavily used in virtualization (where
they prevent VMs from clobbering each others' writes), and also in RBD
mirroring (where they are a prerequisite for journaling).

Exclusive locks are enabled on newly created images by default, unless
overridden via the ``rbd_default_features`` configuration option or
the ``--image-feature`` flag for ``rbd create``.

In order to ensure proper exclusive locking operations, any client
using an RBD image whose ``exclusive-lock`` feature is enabled should
be using a CephX identity whose capabilities include ``profile rbd``.

Exclusive locking is mostly transparent to the user.

#. Whenever any ``librbd`` client process or kernel RBD client
   starts using an RBD image on which exclusive locking has been
   enabled, it obtains an exclusive lock on the image before the first
   write.

#. Whenever any such client process gracefully terminates, it
   automatically relinquishes the lock.

#. This subsequently enables another process to acquire the lock, and
   write to the image.

Note that it is perfectly possible for two or more concurrently
running processes to merely open the image, and also to read from
it. The client acquires the exclusive lock only when attempting to
write to the image. To disable transparent lock transitions between
multiple clients, it needs to acquire the lock specifically with
``RBD_LOCK_MODE_EXCLUSIVE``.


Blacklisting
============

Sometimes, a client process (or, in case of a krbd client, a client
node's kernel thread) that previously held an exclusive lock on an
image does not terminate gracefully, but dies abruptly. This may be
due to having received a ``KILL`` or ``ABRT`` signal, for example, or
a hard reboot or power failure of the client node. In that case, the
exclusive lock is never gracefully released. Thus, when a new process
starts and attempts to use the device, it needs a way to break the
previously held exclusive lock.

However, a process (or kernel thread) may also hang, or merely lose
network connectivity to the Ceph cluster for some amount of time. In
that case, simply breaking the lock would be potentially catastrophic:
the hung process or connectivity issue may resolve itself, and the old
process may then compete with one that has started in the interim,
accessing RBD data in an uncoordinated and destructive manner.

Thus, in the event that a lock cannot be acquired in the standard
graceful manner, the overtaking process not only breaks the lock, but
also blacklists the previous lock holder. This is negotiated between
the new client process and the Ceph Mon: upon receiving the blacklist
request,

* the Mon instructs the relevant OSDs to no longer serve requests from
  the old client process;
* once the associated OSD map update is complete, the Mon grants the
  lock to the new client;
* once the new client has acquired the lock, it can commence writing
  to the image.

Blacklisting is thus a form of storage-level resource `fencing`_.

In order for blacklisting to work, the client must have the ``osd
blacklist`` capability. This capability is included in the ``profile
rbd`` capability profile, which should generally be set on all Ceph
:ref:`client identities <user-management>` using RBD.

.. _fencing: https://en.wikipedia.org/wiki/Fencing_(computing)
