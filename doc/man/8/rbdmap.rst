:orphan:

=========================================
 rbdmap -- map RBD devices at boot time
=========================================

.. program:: rbdmap

Synopsis
========

| **rbdmap**


Description
===========

**rbdmap** is a shell script that is intended to be run at boot time
by the init system (sysvinit, upstart, systemd). It looks for an
environment variable ``RBDMAPFILE``, which defaults to
``/etc/ceph/rbdmap``. This file is expected to contain a list of RBD
devices to be mapped (via the ``rbd map`` command) and mounted at boot
time.


Options
=======

None


Availability
============

**rbdmap** is part of Ceph, a massively scalable, open-source, distributed
storage system. Please refer to the Ceph documentation at
http://ceph.com/docs for more information.


See also
========

:doc:`rbd <rbd>`\(8),
