:orphan:

=======================================================
 ceph-volume -- Ceph OSD deployment and inspection tool
=======================================================

.. program:: ceph-volume

Synopsis
========

| **ceph-volume** [-h] [--cluster CLUSTER] [--log-level LOG_LEVEL]
|                 [--log-path LOG_PATH]

| **ceph-volume** **inventory**

| **ceph-volume** **lvm** [ *trigger* | *create* | *activate* | *prepare*
| *zap* | *list* | *batch*]

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

inventory
---------

This subcommand provides information about a host's physical disc inventory and
reports metadata about these discs. Among this metadata one can find disc
specific data items (like model, size, rotational or solid state) as well as
data items specific to ceph using a device, such as if it is available for
use with ceph or if logical volumes are present.

Examples::

    ceph-volume inventory
    ceph-volume inventory /dev/sda
    ceph-volume inventory --format json-pretty

Optional arguments:

* [-h, --help]          show the help message and exit
* [--format]            report format, valid values are ``plain`` (default),
                        ``json`` and ``json-pretty``

lvm
---

By making use of LVM tags, the ``lvm`` sub-command is able to store and later
re-discover and query devices associated with OSDs so that they can later
activated.

Subcommands:

**batch**
Creates OSDs from a list of devices using a ``filestore``
or ``bluestore`` (default) setup. It will create all necessary volume groups
and logical volumes required to have a working OSD.

Example usage with three devices::

    ceph-volume lvm batch --bluestore /dev/sda /dev/sdb /dev/sdc

Optional arguments:

* [-h, --help]          show the help message and exit
* [--bluestore]         Use the bluestore objectstore (default)
* [--filestore]         Use the filestore objectstore
* [--yes]               Skip the report and prompt to continue provisioning
* [--prepare]           Only prepare OSDs, do not activate
* [--dmcrypt]           Enable encryption for the underlying OSD devices
* [--crush-device-class] Define a CRUSH device class to assign the OSD to
* [--no-systemd]         Do not enable or create any systemd units
* [--report]         Report what the potential outcome would be for the
                     current input (requires devices to be passed in)
* [--format]         Output format when reporting (used along with
                     --report), can be one of 'pretty' (default) or 'json'
* [--block-db-size]     Set (or override) the "bluestore_block_db_size" value,
                        in bytes
* [--journal-size]      Override the "osd_journal_size" value, in megabytes

Required positional arguments:

* <DEVICE>    Full path to a raw device, like ``/dev/sda``. Multiple
              ``<DEVICE>`` paths can be passed in.


**activate**
Enables a systemd unit that persists the OSD ID and its UUID (also called
``fsid`` in Ceph CLI tools), so that at boot time it can understand what OSD is
enabled and needs to be mounted.

Usage::

    ceph-volume lvm activate --bluestore <osd id> <osd fsid>

Optional Arguments:

* [-h, --help]  show the help message and exit
* [--auto-detect-objectstore] Automatically detect the objectstore by inspecting
  the OSD
* [--bluestore] bluestore objectstore (default)
* [--filestore] filestore objectstore
* [--all] Activate all OSDs found in the system
* [--no-systemd] Skip creating and enabling systemd units and starting of OSD
  services

Multiple OSDs can be activated at once by using the (idempotent) ``--all`` flag::

    ceph-volume lvm activate --all


**prepare**
Prepares a logical volume to be used as an OSD and journal using a ``filestore``
or ``bluestore`` (default) setup. It will not create or modify the logical volumes
except for adding extra metadata.

Usage::

    ceph-volume lvm prepare --filestore --data <data lv> --journal <journal device>

Optional arguments:

* [-h, --help]          show the help message and exit
* [--journal JOURNAL]   A logical group name, path to a logical volume, or path to a device
* [--bluestore]         Use the bluestore objectstore (default)
* [--block.wal]         Path to a bluestore block.wal logical volume or partition
* [--block.db]          Path to a bluestore block.db logical volume or partition
* [--filestore]         Use the filestore objectstore
* [--dmcrypt]           Enable encryption for the underlying OSD devices
* [--osd-id OSD_ID]     Reuse an existing OSD id
* [--osd-fsid OSD_FSID] Reuse an existing OSD fsid
* [--crush-device-class] Define a CRUSH device class to assign the OSD to

Required arguments:

* --data                A logical group name or a path to a logical volume

For encrypting an OSD, the ``--dmcrypt`` flag must be added when preparing
(also supported in the ``create`` sub-command).


**create**
Wraps the two-step process to provision a new osd (calling ``prepare`` first
and then ``activate``) into a single one. The reason to prefer ``prepare`` and
then ``activate`` is to gradually introduce new OSDs into a cluster, and
avoiding large amounts of data being rebalanced.

The single-call process unifies exactly what ``prepare`` and ``activate`` do,
with the convenience of doing it all at once. Flags and general usage are
equivalent to those of the ``prepare`` and ``activate`` subcommand.

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

* <DEVICE>  Either in the form of ``vg/lv`` for logical volumes,
  ``/path/to/sda1`` or ``/path/to/sda`` for regular devices.


**zap**
Zaps the given logical volume or partition. If given a path to a logical
volume it must be in the format of vg/lv. Any filesystems present
on the given lv or partition will be removed and all data will be purged.

However, the lv or partition will be kept intact.

Usage, for logical volumes::

      ceph-volume lvm zap {vg/lv}

Usage, for logical partitions::

      ceph-volume lvm zap /dev/sdc1

For full removal of the device use the ``--destroy`` flag (allowed for all
device types)::

      ceph-volume lvm zap --destroy /dev/sdc1

Multiple devices can be removed by specifying the OSD ID and/or the OSD FSID::

      ceph-volume lvm zap --destroy --osd-id 1
      ceph-volume lvm zap --destroy --osd-id 1 --osd-fsid C9605912-8395-4D76-AFC0-7DFDAC315D59


Positional arguments:

* <DEVICE>  Either in the form of ``vg/lv`` for logical volumes,
  ``/path/to/sda1`` or ``/path/to/sda`` for regular devices.


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
