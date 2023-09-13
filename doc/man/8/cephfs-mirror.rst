:orphan:

============================================================
 cephfs-mirror -- Ceph daemon for mirroring CephFS snapshots
============================================================

.. program:: cephfs-mirror

Synopsis
========

| **cephfs-mirror**


Description
===========

:program:`cephfs-mirror` is a daemon for asynchronous mirroring of Ceph
Filesystem snapshots among Ceph clusters.

It connects to remote clusters via libcephfs, relying on default search
paths to find ceph.conf files, i.e. ``/etc/ceph/$cluster.conf`` where
``$cluster`` is the human-friendly name of the cluster.


Options
=======

.. option:: --mon-host monaddress[:port]

   Connect to specified monitor (instead of looking through
   ``ceph.conf``).

.. option:: --keyring=<path-to-keyring>

   Provide path to keyring; useful when it's absent in standard locations.

.. option:: --log-file=<logfile>

   file to log debug output

.. option:: --debug-cephfs-mirror=<log-level>/<memory-level>

   set cephfs-mirror debug level

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ``ceph.conf`` configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: -i ID, --id ID

   Set the ID portion of name for cephfs-mirror

.. option:: -n TYPE.ID, --name TYPE.ID

   Set the rados user name (eg. client.mirror)

.. option:: --cluster NAME

   Set the cluster name (default: ceph)

.. option:: -d

   Run in foreground, log to stderr

.. option:: -f

   Run in foreground, log to usual location


Availability
============

:program:`cephfs-mirror` is part of Ceph, a massively scalable, open-source, distributed
storage system. Please refer to the Ceph documentation at https://docs.ceph.com for
more information.


See also
========

:doc:`ceph <ceph>`\(8)
