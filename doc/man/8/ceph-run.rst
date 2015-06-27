:orphan:

=========================================
 ceph-run -- restart daemon on core dump
=========================================

.. program:: ceph-run

Synopsis
========

| **ceph-run** *command* ...


Description
===========

**ceph-run** is a simple wrapper that will restart a daemon if it exits
with a signal indicating it crashed and possibly core dumped (that is,
signals 3, 4, 5, 6, 8, or 11).

The command should run the daemon in the foreground. For Ceph daemons,
that means the ``-f`` option.


Options
=======

None


Availability
============

**ceph-run** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`ceph-mon <ceph-mon>`\(8),
:doc:`ceph-mds <ceph-mds>`\(8),
:doc:`ceph-osd <ceph-osd>`\(8)
