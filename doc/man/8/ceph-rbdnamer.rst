:orphan:

==================================================
 ceph-rbdnamer -- udev helper to name RBD devices
==================================================

.. program:: ceph-rbdnamer


Synopsis
========

| **ceph-rbdnamer** *num*


Description
===========

**ceph-rbdnamer** prints the pool, namespace, image and snapshot names
for a given RBD device to stdout. It is used by `udev` device manager
to set up RBD device symlinks. The appropriate `udev` rules are
provided in a file named `50-rbd.rules`.

Availability
============

**ceph-rbdnamer** is part of Ceph, a massively scalable, open-source, distributed storage system.  Please
refer to the Ceph documentation at https://docs.ceph.com for more
information.


See also
========

:doc:`rbd <rbd>`\(8),
:doc:`ceph <ceph>`\(8)
