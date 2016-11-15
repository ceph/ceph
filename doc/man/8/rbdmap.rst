:orphan:

=========================================
 rbdmap -- map RBD devices at boot time
=========================================

.. program:: rbdmap

Synopsis
========

| **rbdmap map**
| **rbdmap unmap**


Description
===========

**rbdmap** is a shell script that can be run manually by the system
administrator at any time, or automatically at boot time by the init system
(sysvinit, upstart, systemd). The script looks for an environment variable
``RBDMAPFILE``, which defaults to ``/etc/ceph/rbdmap``. This file is
expected to contain a list of RBD images and, possibly, parameters to be
passed to the underlying ``rbd`` command. The syntax of
``/etc/ceph/rbdmap`` is described in the comments at the top of that file.

The script mounts devices after mapping, and unmounts them before
unmapping.


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
