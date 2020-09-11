.. _ceph-volume-lvm-batch:

``batch``
===========
This subcommand allows for multiple OSDs to be created at the same time given
an input of devices. Depending on the device type (spinning drive, or solid
state), the internal engine will decide the best approach to create the OSDs.

This decision abstracts away the many nuances when creating an OSD: how large
should a ``block.db`` be? How can one mix a solid state device with spinning
devices in an efficient way?

The process is similar to :ref:`ceph-volume-lvm-create`, and will do the
preparation and activation at once, following the same workflow for each OSD.
However, If the ``--prepare`` flag is passed then only the prepare step is taken
and the OSDs are not activated.

All the features that ``ceph-volume lvm create`` supports, like ``dmcrypt``,
avoiding ``systemd`` units from starting, defining bluestore or filestore,
are supported. Any fine-grained option that may affect a single OSD is not
supported, for example: specifying where journals should be placed.




Automatic sorting of disks
--------------------------
If ``batch`` receives only a single list of data devices and other options are
passed , ``ceph-volume`` will auto-sort disks by its rotational
property and use non-rotating disks for ``block.db`` or ``journal`` depending
on the objectstore used. If all devices are to be used for standalone OSDs,
no matter if rotating or solid state, pass ``--no-auto``.
For example assuming :term:`bluestore` is used and ``--no-auto`` is not passed,
the deprecated behavior would deploy the following, depending on the devices
passed:

#. Devices are all spinning HDDs: 1 OSD is created per device
#. Devices are all SSDs: 2 OSDs are created per device
#. Devices are a mix of HDDs and SSDs: data is placed on the spinning device,
   the ``block.db`` is created on the SSD, as large as possible.

.. note:: Although operations in ``ceph-volume lvm create`` allow usage of
          ``block.wal`` it isn't supported with the ``auto`` behavior.

This default auto-sorting behavior is now DEPRECATED and will be changed in future releases.
Instead devices are not automatically sorted unless the ``--auto`` option is passed

It is recommended to make use of the explicit device lists for ``block.db``,
   ``block.wal`` and ``journal``.

.. _ceph-volume-lvm-batch_bluestore:

Reporting
=========
By default ``batch`` will print a report of the computed OSD layout and ask the
user to confirm. This can be overridden by passing ``--yes``.

If one wants to try out several invocations with being asked to deploy
``--report`` can be passed. ``ceph-volume`` will exit after printing the report.

Consider the following invocation::

    $ ceph-volume lvm batch --report /dev/sdb /dev/sdc /dev/sdd --db-devices /dev/nvme0n1

This will deploy three OSDs with external ``db`` and ``wal`` volumes on
an NVME device.

**pretty reporting**
The ``pretty`` report format (the default) would
look like this::

    $ ceph-volume lvm batch --report /dev/sdb /dev/sdc /dev/sdd --db-devices /dev/nvme0n1
    --> passed data devices: 3 physical, 0 LVM
    --> relative data size: 1.0
    --> passed block_db devices: 1 physical, 0 LVM

    Total OSDs: 3

      Type            Path                                                    LV Size         % of device
    ----------------------------------------------------------------------------------------------------
      data            /dev/sdb                                              300.00 GB         100.00%
      block_db        /dev/nvme0n1                                           66.67 GB         33.33%
    ----------------------------------------------------------------------------------------------------
      data            /dev/sdc                                              300.00 GB         100.00%
      block_db        /dev/nvme0n1                                           66.67 GB         33.33%
    ----------------------------------------------------------------------------------------------------
      data            /dev/sdd                                              300.00 GB         100.00%
      block_db        /dev/nvme0n1                                           66.67 GB         33.33%


.. note:: Although operations in ``ceph-volume lvm create`` allow usage of
          ``block.wal`` it isn't supported with the ``batch`` sub-command



**JSON reporting**
Reporting can produce a structured output with ``--format json`` or
``--format json-pretty``::

    $ ceph-volume lvm batch --report --format json-pretty /dev/sdb /dev/sdc /dev/sdd --db-devices /dev/nvme0n1
    --> passed data devices: 3 physical, 0 LVM
    --> relative data size: 1.0
    --> passed block_db devices: 1 physical, 0 LVM
    [
        {
            "block_db": "/dev/nvme0n1",
            "block_db_size": "66.67 GB",
            "data": "/dev/sdb",
            "data_size": "300.00 GB",
            "encryption": "None"
        },
        {
            "block_db": "/dev/nvme0n1",
            "block_db_size": "66.67 GB",
            "data": "/dev/sdc",
            "data_size": "300.00 GB",
            "encryption": "None"
        },
        {
            "block_db": "/dev/nvme0n1",
            "block_db_size": "66.67 GB",
            "data": "/dev/sdd",
            "data_size": "300.00 GB",
            "encryption": "None"
        }
    ]

Sizing
======
When no sizing arguments are passed, `ceph-volume` will derive the sizing from
the passed device lists (or the sorted lists when using the automatic sorting).
`ceph-volume batch` will attempt to fully utilize a device's available capacity.
Relying on automatic sizing is recommended.

#. Devices are all the same type (for example all spinning HDD or all SSDs):
   1 OSD is created per device, collocating the journal in the same HDD.
#. Devices are a mix of HDDs and SSDs: data is placed on the spinning device,
   while the journal is created on the SSD using the sizing options from
   ceph.conf and falling back to the default journal size of 5GB.


* ``--block-db-slots``
* ``--block-wal-slots``
* ``--journal-slots``

For example, consider an OSD host that is supposed to contain 5 data devices and
one device for wal/db volumes. However, one data device is currently broken and
is being replaced. Instead of calculating the explicit sizes for the wal/db
volume, one can simply call::

.. _ceph-volume-lvm-batch_report:

Reporting
=========
When a call is received to create OSDs, the tool will prompt the user to
continue if the pre-computed output is acceptable. This output is useful to
understand the outcome of the received devices. Once confirmation is accepted,
the process continues.

* ``--block-db-size``
* ``--block-wal-size``
* ``--journal-size``

**pretty reporting**
For two spinning devices, this is how the ``pretty`` report (the default) would
look::

    $ ceph-volume lvm batch --report /dev/sdb /dev/sdc

    Total OSDs: 2

      Type            Path                      LV Size         % of device
    --------------------------------------------------------------------------------
      [data]          /dev/sdb                  10.74 GB        100%
    --------------------------------------------------------------------------------
      [data]          /dev/sdc                  10.74 GB        100%



**JSON reporting**
Reporting can produce a richer output with ``JSON``, which gives a few more
hints on sizing. This feature might be better for other tooling to consume
information that will need to be transformed.

For two spinning devices, this is how the ``JSON`` report would look::

    $ ceph-volume lvm batch --report --format=json /dev/sdb /dev/sdc
    {
        "osds": [
            {
                "block.db": {},
                "data": {
                    "human_readable_size": "10.74 GB",
                    "parts": 1,
                    "path": "/dev/sdb",
                    "percentage": 100,
                    "size": 11534336000.0
                }
            },
            {
                "block.db": {},
                "data": {
                    "human_readable_size": "10.74 GB",
                    "parts": 1,
                    "path": "/dev/sdc",
                    "percentage": 100,
                    "size": 11534336000.0
                }
            }
        ],
        "vgs": [
            {
                "devices": [
                    "/dev/sdb"
                ],
                "parts": 1
            },
            {
                "devices": [
                    "/dev/sdc"
                ],
                "parts": 1
            }
        ]
    }
