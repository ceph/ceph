:orphan:

===================================================
 rbd-mirror -- Ceph daemon for mirroring RBD images
===================================================

.. program:: rbd-mirror

Synopsis
========

| **rbd-mirror**


Description
===========

:program:`rbd-mirror` is a daemon for asynchronous mirroring of RADOS
block device (rbd) images among Ceph clusters. It replays changes to
images in remote clusters in a local cluster, for disaster recovery.

It connects to remote clusters via the RADOS protocol, relying on
default search paths to find ceph.conf files monitor addresses and
authentication information for them, i.e. ``/etc/ceph/$cluster.conf``,
``/etc/ceph/$cluster.keyring``, and
``/etc/ceph/$cluster.$name.keyring``, where ``$cluster`` is the
human-friendly name of the cluster, and ``$name`` is the rados user to
connect as, e.g. ``client.rbd-mirror``.


Options
=======

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ``ceph.conf`` configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ``ceph.conf``).

.. option:: -i ID, --id ID

   Set the ID portion of name for rbd-mirror

.. option:: -n TYPE.ID, --name TYPE.ID

   Set the rados user name for the gateway (eg. client.rbd-mirror)

.. option:: --cluster NAME

   Set the cluster name (default: ceph)

.. option:: -d

   Run in foreground, log to stderr

.. option:: -f

   Run in foreground, log to usual location


Availability
============

:program:`rbd-mirror` is part of Ceph, a massively scalable, open-source, distributed
storage system. Please refer to the Ceph documentation at http://ceph.com/docs for
more information.


See also
========

:doc:`rbd <rbd>`\(8)
