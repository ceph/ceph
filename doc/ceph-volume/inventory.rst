.. _ceph-volume-inventory:

``inventory``
=============
The ``inventory`` subcommand queries a host's disc inventory and provides
hardware information and metadata on every physical device.

By default the command returns a short, human-readable report of all physical disks.

For programmatic consumption of this report pass ``--format json`` to generate a
JSON formatted report. This report includes extensive information on the
physical drives such as disk metadata (like model and size), logical volumes
and whether they are used by ceph, and if the disk is usable by ceph and
reasons why not.

A device path can be specified to report extensive information on a device in
both plain and json format.
