=====================================
 crun -- restart daemon on core dump
=====================================

.. program:: crun

Synopsis
========

| **crun** *command* ...


Description
===========

**crun** is a simple wrapper that will restart a daemon if it exits
with a signal indicating it crashed and possibly core dumped (that is,
signals 3, 4, 5, 6, 8, or 11).

The command should run the daemon in the foreground. For Ceph daemons,
that means the ``-f`` option.


Options
=======

None


Availability
============

**crun** is part of the Ceph distributed file system. Please refer to
the Ceph wiki at http://ceph.newdream.net/wiki for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`cmon <cmon>`\(8),
:doc:`cmds <cmds>`\(8),
:doc:`cosd <cosd>`\(8)
