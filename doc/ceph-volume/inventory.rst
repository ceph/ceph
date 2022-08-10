.. _ceph-volume-inventory:

``inventory``
=============
The ``inventory`` subcommand queries a host's disc inventory and provides
hardware information and metadata on every physical device.

By default the command returns a short, human-readable report of all physical disks.

For programmatic consumption of this report pass ``--format json`` to generate a
JSON formatted report,pass ``--format json-pretty`` to generate a pretty JSON formatted report.
This report includes extensive information on the
physical drives such as disk metadata (like model and size), logical volumes
and whether they are used by ceph, and if the disk is usable by ceph and
reasons why not.

inventory a device:

ceph-volume inventory  /dev/vdc

====== Device report /dev/vdc ======

     path                      /dev/vdc
     ceph device               None
     lsm data                  {}
     available                 False
     rejected reasons          locked, Insufficient space (<10 extents) on vgs, LVM detected
     device id
     removable                 0
     ro                        0
     vendor                    0x1af4
     model
     sas address
     rotational                1
     scheduler mode            mq-deadline
     human readable size       32.00 GB
    --- Logical Volume ---
     name                      osd-block-bdef121a-82e5-49ab-b844-ced927017677
     osd id                    0
     cluster name              ceph
     type                      block
     osd fsid                  bdef121a-82e5-49ab-b844-ced927017677
     cluster fsid              ba757a9a-01d9-11ed-b2f7-fa163e4c5d9d
     osdspec affinity          None
     block uuid                7FSM8w-wxr4-Wy9c-8Hg9-ONkn-eq7z-8T0cuW

inventory a device with ``--format json-pretty``:

ceph-volume inventory  /dev/vdc --format json-pretty

{
    "available": false,
    "ceph_device": null,
    "device_id": "",
    "lsm_data": {},
    "lvs": [
        {
            "block_uuid": "7FSM8w-wxr4-Wy9c-8Hg9-ONkn-eq7z-8T0cuW",
            "cluster_fsid": "ba757a9a-01d9-11ed-b2f7-fa163e4c5d9d",
            "cluster_name": "ceph",
            "name": "osd-block-bdef121a-82e5-49ab-b844-ced927017677",
            "osd_fsid": "bdef121a-82e5-49ab-b844-ced927017677",
            "osd_id": "0",
            "osdspec_affinity": "None",
            "type": "block"
        }
    ],
    "path": "/dev/vdc",
    "rejected_reasons": [
        "LVM detected",
        "locked",
        "Insufficient space (<10 extents) on vgs"
    ],
    "sys_api": {
        "human_readable_size": "32.00 GB",
        "locked": 1,
        "model": "",
        "nr_requests": "256",
        "partitions": {},
        "path": "/dev/vdc",
        "removable": "0",
        "rev": "",
        "ro": "0",
        "rotational": "1",
        "sas_address": "",
        "sas_device_handle": "",
        "scheduler_mode": "mq-deadline",
        "sectors": 0,
        "sectorsize": "512",
        "size": 34359738368.0,
        "support_discard": "0",
        "vendor": "0x1af4"
    }
}

inventory a device with ``--format json``:
{"path": "/dev/vdc", "sys_api": {"removable": "0", "ro": "0", "vendor": "0x1af4", "model": "", "rev": "", "sas_address": "", "sas_device_handle": "", "support_discard": "0", "rotational": "1", "nr_requests": "256", "scheduler_mode": "mq-deadline", "partitions": {}, "sectors": 0, "sectorsize": "512", "size": 34359738368.0, "human_readable_size": "32.00 GB", "path": "/dev/vdc", "locked": 1}, "ceph_device": null, "lsm_data": {}, "available": false, "rejected_reasons": ["LVM detected", "Insufficient space (<10 extents) on vgs", "locked"], "device_id": "", "lvs": [{"name": "osd-block-bdef121a-82e5-49ab-b844-ced927017677", "osd_id": "0", "cluster_name": "ceph", "type": "block", "osd_fsid": "bdef121a-82e5-49ab-b844-ced927017677", "cluster_fsid": "ba757a9a-01d9-11ed-b2f7-fa163e4c5d9d", "osdspec_affinity": "None", "block_uuid": "7FSM8w-wxr4-Wy9c-8Hg9-ONkn-eq7z-8T0cuW"}]}

For programmatic consumption of this report pass ``--with-lsm`` attempt to retrieve additional health and metadata
through libstoragemgmt.

ceph-volume inventory  /dev/vdc --format json-pretty --with-lsm

{
    "available": false,
    "ceph_device": null,
    "device_id": "",
    "lsm_data": {
        "errors": [
            "Not a SCSI compatible device",
            "SCSI VPD page 0xb1 is not supported",
            "SCSI VPD page 0x83 is not supported",
            "we only support disk path start with '/dev/sd' today"
        ],
        "health": "Unknown",
        "ledSupport": {
            "FAILstatus": "Unsupported",
            "FAILsupport": "Unknown",
            "IDENTstatus": "Unsupported",
            "IDENTsupport": "Unknown"
        },
        "linkSpeed": "Unknown",
        "mediaType": "Unknown",
        "rpm": "Unknown",
        "serialNum": "Unknown",
        "transport": "Unknown"
    },
    "lvs": [
        {
            "block_uuid": "7FSM8w-wxr4-Wy9c-8Hg9-ONkn-eq7z-8T0cuW",
            "cluster_fsid": "ba757a9a-01d9-11ed-b2f7-fa163e4c5d9d",
            "cluster_name": "ceph",
            "name": "osd-block-bdef121a-82e5-49ab-b844-ced927017677",
            "osd_fsid": "bdef121a-82e5-49ab-b844-ced927017677",
            "osd_id": "0",
            "osdspec_affinity": "None",
            "type": "block"
        }
    ],
    "path": "/dev/vdc",
    "rejected_reasons": [
        "Insufficient space (<10 extents) on vgs",
        "locked",
        "LVM detected"
    ],
    "sys_api": {
        "human_readable_size": "32.00 GB",
        "locked": 1,
        "model": "",
        "nr_requests": "256",
        "partitions": {},
        "path": "/dev/vdc",
        "removable": "0",
        "rev": "",
        "ro": "0",
        "rotational": "1",
        "sas_address": "",
        "sas_device_handle": "",
        "scheduler_mode": "mq-deadline",
        "sectors": 0,
        "sectorsize": "512",
        "size": 34359738368.0,
        "support_discard": "0",
        "vendor": "0x1af4"
    }
}


A device path can be specified to report extensive information on a device in
both plain and json format.
