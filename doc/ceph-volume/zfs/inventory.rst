.. _ceph-volume-zfs-inventory:
lume-zfs-inventory:

``inventory``
=============

The ``inventory`` subcommand queries a host's disk inventory through GEOM and
provides hardware information and metadata on every physical device.

This only works on a FreeBSD platform.

By default, the command returns a short, human-readable report of all physical
disks::

    ceph-volume zfs inventory

For verbose output including partition and ZFS pool membership::

    ceph-volume zfs inventory -v

For programmatic consumption, use ``--format json`` or the ``-j`` shorthand::

    ceph-volume zfs inventory --format json
    ceph-volume zfs inventory -j

A device path can be specified to report information on a single device::

    ceph-volume zfs inventory /dev/ada0

The ``--debug`` flag includes raw command output (e.g. ``gpart show``) in the
report, which is useful for troubleshooting disk detection issues.

The report includes:

* Disk metadata: model, size, rotational status
* Partition table and partition types
* ZFS pool membership (via ``zpool status``)
* Mount status (via ``mount -p``) and ZFS dataset mountpoints
* Swap status (via ``swapinfo``)
* Whether the disk is usable by Ceph and reasons why not

Example output
--------------

Plain output (default)::

    # ceph-volume zfs inventory

    Device Path      Size             rotates Description
    /dev/ada0        223.57 GB        False   INTEL SSDSC2BB240G4
    /dev/ada1        1.82 TB          True    ST2000NX0273
    /dev/ada2        1.82 TB          True    ST2000NX0273
    /dev/ada3        37.27 GB         False   Corsair CSSD-F40GB2
        /dev/ada3p4  35.00 GB         zpool ceph-osd-0  /

Verbose output (``-v``)::

    # ceph-volume zfs inventory -v

    Device Path      Size             rotates Description
    /dev/ada0        223.57 GB        False   INTEL SSDSC2BB240G4
        /dev/ada0p1  209.00 MB        freebsd-boot partition      -
        /dev/ada0p2  223.36 GB        freebsd-zfs partition       /
    /dev/ada1        1.82 TB          True    ST2000NX0273
        /dev/ada1    1.82 TB          zpool zfsraid               -
    /dev/ada2        1.82 TB          True    ST2000NX0273
        /dev/ada2    1.82 TB          zpool zfsraid               -
    /dev/ada3        37.27 GB         False   Corsair CSSD-F40GB2
        /dev/ada3p4  35.00 GB         zpool ceph-osd-0            /

JSON output (``-j``, abbreviated)::

    # ceph-volume zfs inventory -j

    [
        {
            "abspath": "/dev/ada3",
            "available": false,
            "path": "/dev/ada3",
            "sys_api": {
                "descr": "Corsair CSSD-F40GB2",
                "geomname": "ada3",
                "mediasize": "40018599936",
                "rotationrate": "0"
            },
            "used_by": [
                [
                    "ada3p4",
                    "37580963840",
                    "zpool ceph-osd-0",
                    "/"
                ]
            ]
        }
    ]

