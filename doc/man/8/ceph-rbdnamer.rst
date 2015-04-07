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

**ceph-rbdnamer** prints the pool and image name for the given RBD devices
to stdout. It is used by `udev` (using a rule like the one below) to
set up a device symlink.


::

        KERNEL=="rbd[0-9]*", PROGRAM="/usr/bin/ceph-rbdnamer %n", SYMLINK+="rbd/%c{1}/%c{2}"


Availability
============

**ceph-rbdnamer** is part of Ceph, a massively scalable, open-source, distributed storage system.  Please
refer to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`rbd <rbd>`\(8),
:doc:`ceph <ceph>`\(8)
