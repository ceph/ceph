:orphan:

.. _ceph_mds_man:

=========================================
 ceph-mds -- ceph metadata server daemon
=========================================

.. program:: ceph-mds

Synopsis
========

| **ceph-mds** -i <*ID*> [flags]


Description
===========

**ceph-mds** is the metadata server daemon for the Ceph distributed file
system. One or more instances of ceph-mds collectively manage the file
system namespace, coordinating access to the shared OSD cluster.

Each ceph-mds daemon instance should have a unique name. The name is used
to identify daemon instances in the ceph.conf.

Once the daemon has started, the Monitor cluster will normally assign
it a logical rank, or put it in a standby pool to take over for
another daemon that crashes. Some of the specified options can cause
other behaviors.


Options
=======

.. option:: -f, --foreground

   Foreground: do not daemonize after startup (run in foreground). Do
   not generate a pid file. Useful when run via :doc:`ceph-run
   <ceph-run>`\(8).

.. option:: -d

   Debug mode: like ``-f``, but also send all log output to stderr.

.. option:: --setuser userorgid

   Set uid after starting.  If a username is specified, the user
   record is looked up to get a uid and a gid, and the gid is also set
   as well, unless --setgroup is also specified.

.. option:: --setgroup grouporgid

   Set gid after starting.  If a group name is specified the group
   record is looked up to get a gid.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine Monitor addresses during
   startup.

.. option:: -m monaddress[:port]

   Connect to specified Monitor (instead of looking through
   ``ceph.conf``).

.. option:: --id/-i ID

   Set ID portion of the MDS name. The ID should not start with a numeric digit.

.. option:: --name/-n TYPE.ID

   Set the MDS name of the format TYPE.ID. The TYPE is obviously 'mds'.
   The ID should not start with a numeric digit.

Configuration Options
=====================

Any Ceph configuration option may be given on the command line, in either the
``--name=value`` or the ``--name value`` form.

An option that is not recognized is ignored with a warning rather than treated
as an error, because a configuration option may have been renamed, retyped or
removed since the daemon was deployed, and such a daemon should still start.
The warning naming each ignored option is written to the daemon log. This
matches how an unknown option is treated when it is read from a configuration
file or from the Monitor configuration database.

Note that this applies only to options carrying a value. An unrecognized
argument that takes no value is far more likely to be a mistyped command
argument than a configuration option, so it is still an error.

Which options are recognized is decided by the version of this daemon's own
package: the set of known options is compiled in, not retrieved from the
cluster. In a cluster whose packages are at mixed revisions, as during a
staggered upgrade, the same option may therefore be applied by one daemon and
ignored with a warning by another.

Availability
============

**ceph-mds** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to the Ceph documentation at
https://docs.ceph.com for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`ceph-mon <ceph-mon>`\(8),
:doc:`ceph-osd <ceph-osd>`\(8)
