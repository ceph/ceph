=================================
 ceph-mon -- ceph monitor daemon
=================================

.. program:: ceph-mon

Synopsis
========

| **ceph-mon** -i *monid* [ --mon-data *mondatapath* ]


Description
===========

**ceph-mon** is the cluster monitor daemon for the Ceph distributed
file system. One or more instances of **ceph-mon** form a Paxos
part-time parliament cluster that provides extremely reliable and
durable storage of cluster membership, configuration, and state.

The *mondatapath* refers to a directory on a local file system storing
monitor data. It is normally specified via the ``mon data`` option in
the configuration file.

Options
=======

.. option:: -f, --foreground

   Foreground: do not daemonize after startup (run in foreground). Do
   not generate a pid file. Useful when run via :doc:`ceph-run <ceph-run>`\(8).

.. option:: -d

   Debug mode: like ``-f``, but also send all log output to stderr.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during
   startup.


Availability
============

**ceph-mon** is part of the Ceph distributed file system. Please refer
to the Ceph wiki at http://ceph.newdream.net/wiki for more
information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`ceph-mds <ceph-mds>`\(8),
:doc:`ceph-osd <ceph-osd>`\(8)
