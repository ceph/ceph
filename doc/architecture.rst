======================
 Architecture of Ceph
======================

Ceph is a distributed network storage and file system with distributed
metadata management and POSIX semantics.

RADOS is a reliable object store, used by Ceph, but also directly
accessible.

``radosgw`` is an S3-compatible RESTful HTTP service for object
storage, using RADOS storage.

RBD is a Linux kernel feature that exposes RADOS storage as a block
device. Qemu/KVM also has a direct RBD client, that avoids the kernel
overhead.


.. index:: monitor, ceph-mon
.. _monitor:

Monitor cluster
===============

``ceph-mon`` is a lightweight daemon that provides a consensus for
distributed decisionmaking in a Ceph/RADOS cluster.

It also is the initial point of contact for new clients, and will hand
out information about the topology of the cluster, such as the
``osdmap``.

You normally run 3 ``ceph-mon`` daemons, on 3 separate physical machines,
isolated from each other; for example, in different racks or rows.

You could run just 1 instance, but that means giving up on high
availability.

You may use the same hosts for ``ceph-mon`` and other purposes.

``ceph-mon`` processes talk to each other using a Paxos_\-style
protocol. They discover each other via the ``[mon.X] mon addr`` fields
in ``ceph.conf``.

.. todo:: What about ``monmap``? Fact check.

Any decision requires the majority of the ``ceph-mon`` processes to be
healthy and communicating with each other. For this reason, you never
want an even number of ``ceph-mon``\s; there is no unambiguous majority
subgroup for an even number.

.. _Paxos: http://en.wikipedia.org/wiki/Paxos_algorithm

.. todo:: explain monmap


.. index:: RADOS, OSD, ceph-osd, object
.. _rados:

RADOS
=====

``ceph-osd`` is the storage daemon that provides the RADOS service. It
uses ``ceph-mon`` for cluster membership, services object read/write/etc
request from clients, and peers with other ``ceph-osd``\s for data
replication.

The data model is fairly simple on this level. There are multiple
named pools, and within each pool there are named objects, in a flat
namespace (no directories). Each object has both data and metadata.

The data for an object is a single, potentially big, series of
bytes. Additionally, the series may be sparse, it may have holes that
contain binary zeros, and take up no actual storage.

The metadata is an unordered set of key-value pairs. It's semantics
are completely up to the client; for example, the Ceph filesystem uses
metadata to store file owner etc.

.. todo:: Verify that metadata is unordered.

Underneath, ``ceph-osd`` stores the data on a local filesystem. We
recommend using Btrfs_, but any POSIX filesystem that has extended
attributes should work.

.. _Btrfs: http://en.wikipedia.org/wiki/Btrfs

.. todo:: write about access control

.. todo:: explain osdmap

.. todo:: explain plugins ("classes")


.. index:: Ceph filesystem, Ceph Distributed File System, MDS, ceph-mds
.. _cephfs:

Ceph filesystem
===============

The Ceph filesystem service is provided by a daemon called
``ceph-mds``. It uses RADOS to store all the filesystem metadata
(directories, file ownership, access modes, etc), and directs clients
to access RADOS directly for the file contents.

The Ceph filesystem aims for POSIX compatibility, except for a few
chosen differences. See :doc:`/appendix/differences-from-posix`.

``ceph-mds`` can run as a single process, or it can be distributed out to
multiple physical machines, either for high availability or for
scalability.

For high availability, the extra ``ceph-mds`` instances can be `standby`,
ready to take over the duties of any failed ``ceph-mds`` that was
`active`. This is easy because all the data, including the journal, is
stored on RADOS. The transition is triggered automatically by
``ceph-mon``.

For scalability, multiple ``ceph-mds`` instances can be `active`, and they
will split the directory tree into subtrees (and shards of a single
busy directory), effectively balancing the load amongst all `active`
servers.

Combinations of `standby` and `active` etc are possible, for example
running 3 `active` ``ceph-mds`` instances for scaling, and one `standby`.

To control the number of `active` ``ceph-mds``\es, see
:doc:`/ops/manage/grow/mds`.

.. topic:: Status as of 2011-09:

   Multiple `active` ``ceph-mds`` operation is stable under normal
   circumstances, but some failure scenarios may still cause
   operational issues.

.. todo:: document `standby-replay`

.. todo:: mds.0 vs mds.alpha etc details


.. index:: RADOS Gateway, radosgw
.. _radosgw:

``radosgw``
===========

``radosgw`` is a FastCGI service that provides a RESTful_ HTTP API to
store objects and metadata. It layers on top of RADOS with its own
data formats, and maintains it's own user database, authentication,
access control, and so on.

.. _RESTful: http://en.wikipedia.org/wiki/RESTful


.. index:: RBD, Rados Block Device
.. _rbd:

Rados Block Device (RBD)
========================

In virtual machine scenarios, RBD is typically used via the ``rbd``
network storage driver in Qemu/KVM, where the host machine uses
``librbd`` to provide a block device service to the guest.

Alternatively, as no direct ``librbd`` support is available in Xen,
the Linux kernel can act as the RBD client and provide a real block
device on the host machine, that can then be accessed by the
virtualization. This is done with the command-line tool ``rbd`` (see
:doc:`/ops/rbd`).

The latter is also useful in non-virtualized scenarios.

Internally, RBD stripes the device image over multiple RADOS objects,
each typically located on a separate ``ceph-osd``, allowing it to perform
better than a single server could.


Client
======

.. todo:: cephfs, ceph-fuse, librados, libcephfs, librbd


.. todo:: Summarize how much Ceph trusts the client, for what parts (security vs reliability).


TODO
====

.. todo:: Example scenarios Ceph projects are/not suitable for
