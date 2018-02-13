:orphan:

========================================
 ceph-volume -- Ceph OSD deployment tool
========================================

.. program:: ceph-volume

Synopsis
========

| **ceph-volume** [-h] [--cluster CLUSTER] [--log-level LOG_LEVEL]
|                 [--log-path LOG_PATH]

| **ceph-volume** **lvm** [ *trigger* | *create* | *activate* | *prepare*
| *zap* | *list*]

| **ceph-volume** **simple** [ *trigger* | *scan* | *activate* ]


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
* [--bluestore] bluestore objectstore (default)
* [--filestore] filestore objectstore


**prepare**
Prepares a logical volume to be used as an OSD and journal using a ``filestore``
or ``bluestore`` (default) setup. It will not create or modify the logical volumes
except for adding extra metadata.

Usage::

    ceph-volume lvm prepare --filestore --data <data lv> --journal <journal device>

Optional arguments:

* [-h, --help]          show the help message and exit
* [--journal JOURNAL]   A logical group name, path to a logical volume, or path to a device
* [--journal-size GB]   Size (in GB) A logical group name or a path to a logical volume
* [--bluestore]         Use the bluestore objectstore (default)
* [--filestore]         Use the filestore objectstore
* [--dmcrypt]           Enable encryption for the underlying OSD devices
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

**list**
List devices or logical volumes associated with Ceph. An association is
determined if a device has information relating to an OSD. This is
verified by querying LVM's metadata and correlating it with devices.

The lvs associated with the OSD need to have been prepared previously by
ceph-volume so that all needed tags and metadata exist.

Usage::

    ceph-volume lvm list

List a particular device, reporting all metadata about it::

    ceph-volume lvm list /dev/sda1

List a logical volume, along with all its metadata (vg is a volume
group, and lv the logical volume name)::

    ceph-volume lvm list {vg/lv}

Positional arguments:

* <DEVICE>  Either in the form of ``vg/lv`` for logical volumes or
  ``/path/to/sda1`` for regular devices.


**zap**
Zaps the given logical volume or partition. If given a path to a logical
volume it must be in the format of vg/lv. Any filesystems present
on the given lv or partition will be removed and all data will be purged.

However, the lv or partition will be kept intact.

Usage, for logical volumes::

      ceph-volume lvm zap {vg/lv}

Usage, for logical partitions::

      ceph-volume lvm zap /dev/sdc1

Positional arguments:

* <DEVICE>  Either in the form of ``vg/lv`` for logical volumes or
  ``/path/to/sda1`` for regular devices.


simple
------

Scan legacy OSD directories or data devices that may have been created by
ceph-disk, or manually.

Subcommands:

**activate**
Enables a systemd unit that persists the OSD ID and its UUID (also called
``fsid`` in Ceph CLI tools), so that at boot time it can understand what OSD is
enabled and needs to be mounted, while reading information that was previously
created and persisted at ``/etc/ceph/osd/`` in JSON format.

Usage::

    ceph-volume simple activate --bluestore <osd id> <osd fsid>

Optional Arguments:

* [-h, --help]  show the help message and exit
* [--bluestore] bluestore objectstore (default)
* [--filestore] filestore objectstore

Note: It requires a matching JSON file with the following format::

    /etc/ceph/osd/<osd id>-<osd fsid>.json


**scan**
Scan a running OSD or data device for an OSD for metadata that can later be
used to activate and manage the OSD with ceph-volume. The scan method will
create a JSON file with the required information plus anything found in the OSD
directory as well.

Optionally, the JSON blob can be sent to stdout for further inspection.

Usage on data devices::

    ceph-volume simple scan <data device>

Running OSD directories::

    ceph-volume simple scan <path to osd dir>


Optional arguments:

* [-h, --help]          show the help message and exit
* [--stdout]            Send the JSON blob to stdout
* [--force]             If the JSON file exists at destination, overwrite it

Required Positional arguments:

* <DATA DEVICE or OSD DIR>  Actual data partition or a path to the running OSD

**trigger**
This subcommand is not meant to be used directly, and it is used by systemd so
that it proxies input to ``ceph-volume simple activate`` by parsing the
input from systemd, detecting the UUID and ID associated with an OSD.

Usage::

    ceph-volume simple trigger <SYSTEMD-DATA>

The systemd "data" is expected to be in the format of::

    <OSD ID>-<OSD UUID>

The JSON file associated with the OSD need to have been persisted previously by
a scan (or manually), so that all needed metadata can be used.

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
