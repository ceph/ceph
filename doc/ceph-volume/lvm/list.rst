.. _ceph-volume-lvm-list:

``list``
========
This subcommand will list any devices (logical and physical) that may be
associated with a Ceph cluster, as long as they contain enough metadata to
allow for that discovery.

Output is grouped by the OSD ID associated with the devices, and unlike
``ceph-disk`` it does not provide any information for devices that aren't
associated with Ceph.

Command line options:

* ``--format`` Allows a ``json`` or ``pretty`` value. Defaults to ``pretty``
  which will group the device information in a human-readable format.

Full Reporting
--------------
When no positional arguments are used, a full reporting will be presented. This
means that all devices and logical volumes found in the system will be
displayed.

Full ``pretty`` reporting for two OSDs, one with a lv as a journal, and another
one with a physical device may look similar to::

    # ceph-volume lvm list


    ====== osd.1 =======

      [journal]    /dev/journals/journal1

          journal uuid              C65n7d-B1gy-cqX3-vZKY-ZoE0-IEYM-HnIJzs
          osd id                    1
          cluster fsid              ce454d91-d748-4751-a318-ff7f7aa18ffd
          type                      journal
          osd fsid                  661b24f8-e062-482b-8110-826ffe7f13fa
          data uuid                 SlEgHe-jX1H-QBQk-Sce0-RUls-8KlY-g8HgcZ
          journal device            /dev/journals/journal1
          data device               /dev/test_group/data-lv2

      [data]    /dev/test_group/data-lv2

          journal uuid              C65n7d-B1gy-cqX3-vZKY-ZoE0-IEYM-HnIJzs
          osd id                    1
          cluster fsid              ce454d91-d748-4751-a318-ff7f7aa18ffd
          type                      data
          osd fsid                  661b24f8-e062-482b-8110-826ffe7f13fa
          data uuid                 SlEgHe-jX1H-QBQk-Sce0-RUls-8KlY-g8HgcZ
          journal device            /dev/journals/journal1
          data device               /dev/test_group/data-lv2

    ====== osd.0 =======

      [data]    /dev/test_group/data-lv1

          journal uuid              cd72bd28-002a-48da-bdf6-d5b993e84f3f
          osd id                    0
          cluster fsid              ce454d91-d748-4751-a318-ff7f7aa18ffd
          type                      data
          osd fsid                  943949f0-ce37-47ca-a33c-3413d46ee9ec
          data uuid                 TUpfel-Q5ZT-eFph-bdGW-SiNW-l0ag-f5kh00
          journal device            /dev/sdd1
          data device               /dev/test_group/data-lv1

      [journal]    /dev/sdd1

          PARTUUID                  cd72bd28-002a-48da-bdf6-d5b993e84f3f

.. note:: Tags are displayed in a readable format. The ``osd id`` key is stored
          as a ``ceph.osd_id`` tag. For more information on lvm tag conventions
          see :ref:`ceph-volume-lvm-tag-api`

Single Reporting
----------------
Single reporting can consume both devices and logical volumes as input
(positional parameters). For logical volumes, it is required to use the group
name as well as the logical volume name.

For example the ``data-lv2`` logical volume, in the ``test_group`` volume group
can be listed in the following way::

    # ceph-volume lvm list test_group/data-lv2


    ====== osd.1 =======

      [data]    /dev/test_group/data-lv2

          journal uuid              C65n7d-B1gy-cqX3-vZKY-ZoE0-IEYM-HnIJzs
          osd id                    1
          cluster fsid              ce454d91-d748-4751-a318-ff7f7aa18ffd
          type                      data
          osd fsid                  661b24f8-e062-482b-8110-826ffe7f13fa
          data uuid                 SlEgHe-jX1H-QBQk-Sce0-RUls-8KlY-g8HgcZ
          journal device            /dev/journals/journal1
          data device               /dev/test_group/data-lv2


.. note:: Tags are displayed in a readable format. The ``osd id`` key is stored
          as a ``ceph.osd_id`` tag. For more information on lvm tag conventions
          see :ref:`ceph-volume-lvm-tag-api`


For plain disks, the full path to the device is required. For example, for
a device like ``/dev/sdd1`` it can look like::


    # ceph-volume lvm list /dev/sdd1


    ====== osd.0 =======

      [journal]    /dev/sdd1

          PARTUUID                  cd72bd28-002a-48da-bdf6-d5b993e84f3f



``json`` output
---------------
All output using ``--format=json`` will show everything the system has stored
as metadata for the devices, including tags.

No changes for readability are done with ``json`` reporting, and all
information is presented as-is. Full output as well as single devices can be
listed.

For brevity, this is how a single logical volume would look with ``json``
output (note how tags aren't modified)::

    # ceph-volume lvm list --format=json test_group/data-lv1
    {
        "0": [
            {
                "lv_name": "data-lv1",
                "lv_path": "/dev/test_group/data-lv1",
                "lv_tags": "ceph.cluster_fsid=ce454d91-d748-4751-a318-ff7f7aa18ffd,ceph.data_device=/dev/test_group/data-lv1,ceph.data_uuid=TUpfel-Q5ZT-eFph-bdGW-SiNW-l0ag-f5kh00,ceph.journal_device=/dev/sdd1,ceph.journal_uuid=cd72bd28-002a-48da-bdf6-d5b993e84f3f,ceph.osd_fsid=943949f0-ce37-47ca-a33c-3413d46ee9ec,ceph.osd_id=0,ceph.type=data",
                "lv_uuid": "TUpfel-Q5ZT-eFph-bdGW-SiNW-l0ag-f5kh00",
                "name": "data-lv1",
                "path": "/dev/test_group/data-lv1",
                "tags": {
                    "ceph.cluster_fsid": "ce454d91-d748-4751-a318-ff7f7aa18ffd",
                    "ceph.data_device": "/dev/test_group/data-lv1",
                    "ceph.data_uuid": "TUpfel-Q5ZT-eFph-bdGW-SiNW-l0ag-f5kh00",
                    "ceph.journal_device": "/dev/sdd1",
                    "ceph.journal_uuid": "cd72bd28-002a-48da-bdf6-d5b993e84f3f",
                    "ceph.osd_fsid": "943949f0-ce37-47ca-a33c-3413d46ee9ec",
                    "ceph.osd_id": "0",
                    "ceph.type": "data"
                },
                "type": "data",
                "vg_name": "test_group"
            }
        ]
    }


Synchronized information
------------------------
Before any listing type, the lvm API is queried to ensure that physical devices
that may be in use haven't changed naming. It is possible that non-persistent
devices like ``/dev/sda1`` could change to ``/dev/sdb1``.

The detection is possible because the ``PARTUUID`` is stored as part of the
metadata in the logical volume for the data lv. Even in the case of a journal
that is a physical device, this information is still stored on the data logical
volume associated with it.

If the name is no longer the same (as reported by ``blkid`` when using the
``PARTUUID``), the tag will get updated and the report will use the newly
refreshed information.
