:orphan:

========================================
 ceph-osd -- ceph object storage daemon
========================================

.. program:: ceph-osd

Synopsis
========

| **ceph-osd** -i *osdnum* [ --osd-data *datapath* ] [ --osd-journal
  *journal* ] [ --mkfs ] [ --mkjournal ] [--flush-journal] [--check-allows-journal] [--check-wants-journal] [--check-needs-journal] [ --mkkey ] [ --osdspec-affinity ]


Description
===========

**ceph-osd** is the object storage daemon for the Ceph distributed file
system. It is responsible for storing objects on a local file system
and providing access to them over the network.

The datapath argument should be a directory on a xfs file system
where the object data resides. The journal is optional, and is only
useful performance-wise when it resides on a different disk than
datapath with low latency (ideally, an NVRAM device).


Options
=======

.. option:: -f, --foreground

   Foreground: do not daemonize after startup (run in foreground). Do
   not generate a pid file. Useful when run via :doc:`ceph-run <ceph-run>`\(8).

.. option:: -d

   Debug mode: like ``-f``, but also send all log output to stderr.

.. option:: --setuser userorgid

   Set uid after starting.  If a username is specified, the user
   record is looked up to get a uid and a gid, and the gid is also set
   as well, unless --setgroup is also specified.

.. option:: --setgroup grouporgid

   Set gid after starting.  If a group name is specified the group
   record is looked up to get a gid.

.. option:: --osd-data osddata

   Use object store at *osddata*.

.. option:: --osd-journal journal

   Journal updates to *journal*.

.. option:: --check-wants-journal

   Check whether a journal is desired.

.. option:: --check-allows-journal

   Check whether a journal is allowed.

.. option:: --check-needs-journal

   Check whether a journal is required.

.. option:: --mkfs

   Create an empty object repository. This also initializes the journal
   (if one is defined).

.. option:: --mkkey

   Generate a new secret key. This is normally used in combination
   with ``--mkfs`` as it is more convenient than generating a key by
   hand with :doc:`ceph-authtool <ceph-authtool>`\(8).

.. option:: --mkjournal

   Create a new journal file to match an existing object repository.
   This is useful if the journal device or file is wiped out due to a
   disk or file system failure.

.. option:: --flush-journal

   Flush the journal to permanent store. This runs in the foreground
   so you know when it's completed. This can be useful if you want to
   resize the journal or need to otherwise destroy it: this guarantees
   you won't lose data.

.. option:: --get-cluster-fsid

   Print the cluster fsid (uuid) and exit.

.. option:: --get-osd-fsid

   Print the OSD's fsid and exit.  The OSD's uuid is generated at
   --mkfs time and is thus unique to a particular instantiation of
   this OSD.

.. option:: --get-journal-fsid

   Print the journal's uuid.  The journal fsid is set to match the OSD
   fsid at --mkfs time.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` for runtime configuration options.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through
   ``ceph.conf``).

.. option:: --osdspec-affinity

   Set an affinity to a certain OSDSpec.
   This option can only be used in conjunction with --mkfs.

Availability
============

**ceph-osd** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.

See also
========

:doc:`ceph <ceph>`\(8),
:doc:`ceph-mds <ceph-mds>`\(8),
:doc:`ceph-mon <ceph-mon>`\(8),
:doc:`ceph-authtool <ceph-authtool>`\(8)
