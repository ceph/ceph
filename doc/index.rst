=================
 Welcome to Ceph
=================

Ceph is a distributed network storage and file system with distributed
metadata management and POSIX semantics.

RADOS is a reliable object store, used by Ceph, but also directly
accessible.

``radosgw`` is an S3-compatible RESTful HTTP service for object
storage, using RADOS storage.

RBD is a Linux kernel feature that exposes RADOS storage as a block
device. Qemu/KVM also has a direct RBD client, that avoids the kernel
overhead.

.. image:: overview.png



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

The Ceph project is currently focusing on stability. Users are
welcome, but we do not recommend storing valuable data with it yet
without proper precautions.

As of this writing, RADOS is the most stable component, and RBD block
devices are fairly reliable, if not performance tuned yet. The OSD
component of RADOS relies heavily on the stability and performance of
the underlying filesystem, and we keep hearing reports of ``btrfs``
issues; while on the long term we believe in ``btrfs``, in the short
term you may wish to carefully consider the tradeoffs between ``ext4``
and ``btrfs``, and make sure you are running the latest Linux kernel.

Radosgw is still going through heavy development, but it will likely
mature next.

.. _cfuse-kernel-tradeoff:

The Ceph filesystem is functionally fairly complete, but has not been
tested well enough at scale and under load yet. Multi-master MDS is
still problematic and we recommend running just one active MDS
(standbys are ok). If you have problems with ``kclient`` or ``cfuse``,
you may wish to try the other option; in general, ``kclient`` is
expected to be faster (but be sure to use the latest Linux kernel!)
while ``cfuse`` provides better stability by not triggering kernel
crashes.

As individual systems mature enough, we move to improving their
performance (throughput, latency and jitter). This work is still
mostly ahead of us.

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
   api/index
   Internals <dev/index>
   man/index
   papers
   glossary
   appendix/index


Indices and tables
==================

- :ref:`genindex`
- :ref:`search`

