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




.. _ceph-volume-lvm-batch_bluestore:

``bluestore``
-------------
The :term:`bluestore` objectstore (the default) is used when creating multiple OSDs
with the ``batch`` sub-command. It allows a few different scenarios depending
on the input of devices:

#. Devices are all spinning HDDs: 1 OSD is created per device
#. Devices are all SSDs: 2 OSDs are created per device
#. Devices are a mix of HDDs and SSDs: data is placed on the spinning device,
   the ``block.db`` is created on the SSD, as large as possible.


.. note:: Although operations in ``ceph-volume lvm create`` allow usage of
          ``block.wal`` it isn't supported with the ``batch`` sub-command


.. _ceph-volume-lvm-batch_filestore:

``filestore``
-------------
The :term:`filestore` objectstore can be used when creating multiple OSDs
with the ``batch`` sub-command. It allows two different scenarios depending
on the input of devices:

#. Devices are all the same type (for example all spinning HDD or all SSDs):
   1 OSD is created per device, collocating the journal in the same HDD.
#. Devices are a mix of HDDs and SSDs: data is placed on the spinning device,
   while the journal is created on the SSD using the sizing options from
   ceph.conf and falling back to the default journal size of 5GB.


When a mix of solid and spinning devices are used, ``ceph-volume`` will try to
detect existing volume groups on the solid devices. If a VG is found, it will
try to create the logical volume from there, otherwise raising an error if
space is insufficient.

If a raw solid device is used along with a device that has a volume group in
addition to some spinning devices, ``ceph-volume`` will try to extend the
existing volume group and then create a logical volume.

.. _ceph-volume-lvm-batch_report:

Reporting
=========
When a call is received to create OSDs, the tool will prompt the user to
continue if the pre-computed output is acceptable. This output is useful to
understand the outcome of the received devices. Once confirmation is accepted,
the process continues.

Although prompts are good to understand outcomes, it is incredibly useful to
try different inputs to find the best product possible. With the ``--report``
flag, one can prevent any actual operations and just verify outcomes from
inputs.

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
