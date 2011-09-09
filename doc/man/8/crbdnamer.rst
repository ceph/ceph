==============================================
 crbdnamer -- udev helper to name RBD devices
==============================================

.. program:: crbdnamer


Synopsis
========

| **crbdnamer** *num*


Description
===========

**crbdnamer** prints the pool and image name for the given RBD devices
to stdout. It is used by `udev` (using a rule like the one below) to
set up a device symlink.


::

        KERNEL=="rbd[0-9]*", PROGRAM="/usr/bin/crbdnamer %n", SYMLINK+="rbd/%c{1}/%c{2}:%n"


Availability
============

**crbdnamer** is part of the Ceph distributed file system.  Please
refer to the Ceph wiki at http://ceph.newdream.net/wiki for more
information.


See also
========

:doc:`rbd <rbd>`\(8),
:doc:`ceph <ceph>`\(8)
