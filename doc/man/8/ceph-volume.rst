:orphan:

========================================
 ceph-volume -- Ceph OSD deployment tool
========================================

.. program:: ceph-volume

Synopsis
========

| **ceph-volume** [-h] [--cluster CLUSTER] [--log-level LOG_LEVEL]
|                 [--log-path LOG_PATH]

| **ceph-volume** **lvm** [ *trigger* | *create* | *activate* | *prepare* ]

Description
===========

:program:`ceph-volume` is a single purpose command line tool to deploy logical
volumes as OSDs, trying to maintain a similar API to ``ceph-disk`` when
preparing, activating, and creating OSDs.

It deviates from ``ceph-disk`` by not interacting or relying on the udev rules
that come installed for Ceph. These rules allow automatic detection of
previously setup devices that are in turn fed into ``ceph-disk`` to activate
them.


Commands
========

lvm
---

By making use of LVM tags, the ``lvm`` sub-command is able to store and later
re-discover and query devices associated with OSDs so that they can later
activated.

Subcommands:

**activate**
Enables a systemd unit that persists the OSD ID and its UUID (also called
``fsid`` in Ceph CLI tools), so that at boot time it can understand what OSD is
enabled and needs to be mounted.

Usage::

    ceph-volume lvm activate --filestore <osd id> <osd fsid>

Optional Arguments:

* [-h, --help]  show the help message and exit
* [--bluestore] filestore objectstore (not yet implemented)
* [--filestore] filestore objectstore (current default)


**prepare**
Prepares a logical volume to be used as an OSD and journal using a ``filestore`` setup
(``bluestore`` support is planned). It will not create or modify the logical volumes
except for adding extra metadata.

Usage::

    ceph-volume lvm prepare --filestore --data <data lv> --journal <journal device>

Optional arguments:

* [-h, --help]          show the help message and exit
* [--journal JOURNAL]   A logical group name, path to a logical volume, or path to a device
* [--journal-size GB]   Size (in GB) A logical group name or a path to a logical volume
* [--bluestore]         Use the bluestore objectstore (not currently supported)
* [--filestore]         Use the filestore objectstore (currently the only supported object store)
* [--osd-id OSD_ID]     Reuse an existing OSD id
* [--osd-fsid OSD_FSID] Reuse an existing OSD fsid

Required arguments:

* --data                A logical group name or a path to a logical volume

**create**
Wraps the two-step process to provision a new osd (calling ``prepare`` first
and then ``activate``) into a single one. The reason to prefer ``prepare`` and
then ``activate`` is to gradually introduce new OSDs into a cluster, and
avoiding large amounts of data being rebalanced.

The single-call process unifies exactly what ``prepare`` and ``activate`` do,
with the convenience of doing it all at once. Flags and general usage are
equivalent to those of the ``prepare`` subcommand.

**trigger**
This subcommand is not meant to be used directly, and it is used by systemd so
that it proxies input to ``ceph-volume lvm activate`` by parsing the
input from systemd, detecting the UUID and ID associated with an OSD.

Usage::

    ceph-volume lvm trigger <SYSTEMD-DATA>

The systemd "data" is expected to be in the format of::

    <OSD ID>-<OSD UUID>

The lvs associated with the OSD need to have been prepared previously,
so that all needed tags and metadata exist.

Positional arguments:

* <SYSTEMD_DATA>  Data from a systemd unit containing ID and UUID of the OSD.

Availability
============

:program:`ceph-volume` is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the documentation at http://docs.ceph.com/ for more information.


See also
========

:doc:`ceph-osd <ceph-osd>`\(8),
:doc:`ceph-disk <ceph-disk>`\(8),
