=================
 Welcome to Ceph
=================

Ceph is a unified, distributed storage system that operates on a large
number of hosts connected by a TCP/IP network. Ceph has been designed
to accommodate multiple petabytes of storage with ease.

Ceph Distributed File System provides POSIX filesystem semantics with
distributed metadata management.

RADOS is a reliable object store, used by Ceph, but also directly
accessible by client applications.

``radosgw`` is an S3-compatible RESTful HTTP service for object
storage, using RADOS storage.

RBD is a Linux kernel feature that exposes RADOS storage as a block
device. Qemu/KVM also has a direct RBD client, that avoids the kernel
overhead.

.. ditaa::

   /---------+-----------+-----------\/----------+------\/---------\/-----------\
   | ceph.ko | ceph-fuse | libcephfs ||  kernel  | Qemu ||         ||librados   |
   |c9EE     |c3EA       |c6F6       || /dev/rbd | /KVM ||         ||c6F6       |
   +---------+-----------+-----------+|c9EE      |c3EA  ||         |+-----------+
   |       Ceph DFS (protocol)       |+----------+------+| radosgw ||           |
   |               +-----------------+|                 ||         ||           |
   |               |     ceph-mds    || RBD (protocol)  ||         ||           |
   |               |cFA2             ||                 ||cFB5     ||           |
   +---------------+-----------------++-----------------++---------++           |
   |                                                                            |
   |                                          +=------+     +=------+           |
   |                                          |cls_rbd|     |cls_rgw|           |
   |                                          +-------+     +-------+           |
   |                                                                            |
   |                              ceph-osd                                      |
   |cFB3                                                                        |
   \----------------------------------------------------------------------------/



Mailing lists, bug tracker, IRC channel
=======================================

- `Ceph Blog <http://ceph.newdream.net/news/>`__: news and status info
- The development mailing list is at ceph-devel@vger.kernel.org, and
  archived at Gmane_. Send email to subscribe_ or unsubscribe_.
- `Bug/feature tracker <http://tracker.newdream.net/projects/ceph>`__:
  for filing bugs and feature requests.
- IRC channel ``#ceph`` on ``irc.oftc.net``: Many of the core
  developers are on IRC, especially daytime in the US/Pacific
  timezone. You are welcome to join and ask questions. You can find
  logs of the channel `here <http://irclogs.ceph.widodh.nl/>`__.
- `Commercial support <http://ceph.newdream.net/support/>`__

.. _subscribe: mailto:majordomo@vger.kernel.org?body=subscribe+ceph-devel
.. _unsubscribe: mailto:majordomo@vger.kernel.org?body=unsubscribe+ceph-devel
.. _Gmane: http://news.gmane.org/gmane.comp.file-systems.ceph.devel


Status
======

The Ceph project is currently focused on stability.  The object store
(RADOS), radosgw, and RBD are considered reasonably stable.  However,
we do not yet recommend storing valuable data with it yet without
proper precautions.

The OSD component of RADOS relies heavily on the stability and
performance of the underlying filesystem.  In the long-term we believe
that the best performance and stability will come from ``btrfs``.
Currently, you need to run the latest ``btrfs`` kernel to get the
latest stability fixes, and there are several performance fixes that
have not yet hit the mainline kernel.  In the short term you may wish
to carefully consider the tradeoffs between ``xfs``, ``ext4`` and
``btrfs``.  In particular:

* ``btrfs`` can efficiently clone objects, which improves performance
  and space utilization when using snapshots with RBD and the
  distributed filesystem.  ``xfs`` and ``ext4`` will have to copy
  snapshotted objects the first time they are touched.

* ``xfs`` has a 64 KB limit on extended attributes (xattrs).

* ``ext4`` has a 4 KB limit on xattrs.

Ceph uses xattrs for internal object state, snapshot metadata, and
``radosgw`` ACLs.  For most purposes, the 64 KB provided by ``xfs`` is
plenty, making that our second choice if ``btrfs`` is not an option
for you.  The 4 KB limit in ``ext4`` is easily hit by ``radosgw``, and
will cause ``ceph-osd`` to crash, making that a poor choice for
``radosgw`` users.  On the other hand, if you are using RADOS or RBD
without snapshots and without ``radosgw``, ``ext4`` will be just
fine.  We will have a workaround for xattr size limitations shortly,
making these problems largely go away.

.. _cfuse-kernel-tradeoff:

The Ceph filesystem is functionally fairly complete, but has not been
tested well enough at scale and under load yet. Multi-master MDS is
still problematic and we recommend running just one active MDS
(standbys are ok). If you have problems with ``kclient`` or
``ceph-fuse``, you may wish to try the other option; in general,
``kclient`` is expected to be faster (but be sure to use the latest
Linux kernel!)  while ``ceph-fuse`` provides better stability by not
triggering kernel crashes.

Ceph is developed on Linux. Other platforms may work, but are not the
focus of the project. Filesystem access from other operating systems
can be done via NFS or Samba re-exports.


Table of Contents
=================

.. toctree::
   :maxdepth: 3

   start/index
   architecture
   ops/index
   rec/index
   config
   control
   api/index
   Internals <dev/index>
   man/index
   papers
   appendix/index


Indices and tables
==================

- :ref:`genindex`
