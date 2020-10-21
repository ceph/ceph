.. _ceph-volume-lvm-batch:

``batch``
===========
The subcommand allows to create multiple OSDs at the same time given
an input of devices.

The subcommand is based to :ref:`ceph-volume-lvm-create`, and will use the very
same code path. All ``batch`` does is to calculate the appropriate sizes of all
volumes and skip over already created volumes.

All the features that ``ceph-volume lvm create`` supports, like ``dmcrypt``,
avoiding ``systemd`` units from starting, defining bluestore or filestore,
are supported.


.. _ceph-volume-lvm-batch_auto:

Automatic sorting of disks
--------------------------
If ``batch`` receives only a single list of devices and the ``--no-auto`` option
   is not passed, ``ceph-volume`` will auto-sort disks by its rotational
   property and use non-rotating disks for ``block.db`` or ``journal`` depending
   on the objectstore used.
This behavior is now DEPRECATED and will be removed in future releases. Instead
   an ``auto`` option will be introduced to retain this behavior.
It is recommended to make use of the explicit device lists for ``block.db``,
   ``block.wal`` and ``journal``.
For example assuming :term:`bluestore` is used and ``--no-auto`` is not passed,
   the deprecated behavior would deploy the following, depending on the devices
   passed:

#. Devices are all spinning HDDs: 1 OSD is created per device
#. Devices are all SSDs: 2 OSDs are created per device
#. Devices are a mix of HDDs and SSDs: data is placed on the spinning device,
   the ``block.db`` is created on the SSD, as large as possible.

.. note:: Although operations in ``ceph-volume lvm create`` allow usage of
          ``block.wal`` it isn't supported with the ``auto`` behavior.

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

    Total OSDs: 3




**JSON reporting**
Reporting can produce a structured output with ``--format json``::

    $ ceph-volume lvm batch --report /dev/sdb /dev/sdc /dev/sdd --db-devices /dev/nvme0n1

Sizing
======
When no sizing arguments are passed, `ceph-volume` will derive the sizing from
the passed device lists (or the sorted lists when using the automatic sorting).
`ceph-volume batch` will attempt to fully utilize a devices available capacity.
Relying on automatic sizing is recommended.

If one requires a different sizing policy for wal, db or journal devices,
`ceph-volume` offers implicit and explicit sizing rules.

Implicit sizing
---------------
Scenarios in which either devices are under-comitted or not all data devices are
currently ready for use (due to a broken disk for example), one can still rely
on `ceph-volume` automatic sizing.
Users can provide hints to `ceph-volume` as to how many data devices should have
their external volumes on a set of fast devices. These options are:

* `--block-db-slots`
* `--block-wal-slots`
* `--journal-slots`

For example consider an OSD host that is supposed to contain 5 data devices and
one device for wal/db volumes. However one data device is currently broken and
is being replaced. Instead of calculating the explicit sizes for the wal/db
volume one can simply call::

    $ ceph-volume lvm batch --report /dev/sdb /dev/sdc /dev/sdd /dev/sde --db-devices /dev/nvme0n1 --block-db-slots 5

Explicit sizing
---------------
It is also possible to provide explicit sizes to `ceph-volume` via the arguments

* `--block-db-size`
* `--block-wal-size`
* `--journal-size`

`ceph-volume` will try to satisfy the requested sizes given the passed disks. If
this is not possible, no OSDs will be deployed.


Idempotency and disk replacements
=================================
`ceph-volume lvm batch` intends to be idempotent, i.e. calling the same command
repeatedly must result in the same outcome. For example calling::

    $ ceph-volume lvm batch --report /dev/sdb /dev/sdc /dev/sdd --db-devices /dev/nvme0n1

will result in three deployed OSDs (if all disks were available). Calling this
command again, you will still end up with three OSDs and ceph-volume will exit
with return code 0.

Suppose /dev/sdc goes bad and needs to be replaced. After destroying the OSD and
replacing the hardware, you can again call the same command and `ceph-volume`
will detect that only two out of the three wanted OSDs are setup and re-create
the missing OSD.
