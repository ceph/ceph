***********
OSD Service
***********
.. _libstoragemgmt: https://github.com/libstorage/libstoragemgmt

List Devices
============

``ceph-volume`` scans each host in the cluster periodically in order
to determine the devices that are present and responsive. It is also
determined whether each is eligible to be used for new OSDs in a block,
DB, or WAL role.

To print a list of devices discovered by ``cephadm``, run this command:

.. prompt:: bash #

  ceph orch device ls [--hostname=...] [--wide] [--refresh]

Example:

.. code-block:: console

  HOST            PATH      TYPE  DEVICE ID                                     SIZE  AVAILABLE  REFRESHED  REJECT REASONS
  davidsthubbins  /dev/sdc  hdd   SEAGATE_ST20000NM002D_ZVTBJNGC17010W339UW25  18.1T  No         22m ago    Has a FileSystem, Insufficient space (<10 extents) on vgs, LVM detected
  nigeltufnel     /dev/sdd  hdd   SEAGATE_ST20000NM002D_ZVTBJNGC17010C3442787  18.1T  No         22m ago    Has a FileSystem, Insufficient space (<10 extents) on vgs, LVM detected

The ``REJECT REASONS`` column shows why a device is not available for a
new OSD.

The ``--wide`` option adds the ``TRANSPORT``, ``RPM``, ``HEALTH``, ``IDENT``,
and ``FAULT`` columns. `libstoragemgmt`_ provides the values in these columns.
By default, this integration is disabled because `libstoragemgmt`_ may not be
100% compatible with your hardware.  To direct Ceph to fill these columns,
enable ``cephadm``'s "enhanced device scan" option as follows:

.. prompt:: bash #

  ceph config set mgr mgr/cephadm/device_enhanced_scan true

Note that the columns reported by ``ceph orch device ls`` may vary from release to
release.

Without the enhanced device scan, the ``TRANSPORT``, ``RPM``, and ``HEALTH``
columns of the ``--wide`` output are empty, and ``IDENT`` and ``FAULT`` show
``N/A``:

.. code-block:: console

  HOST            PATH      TYPE  TRANSPORT  RPM  DEVICE ID                                     SIZE  HEALTH  IDENT  FAULT  AVAILABLE  REFRESHED  REJECT REASONS
  davidsthubbins  /dev/sdc  hdd                   SEAGATE_ST20000NM002D_ZVTBJNGC17010W339UW25  18.1T          N/A    N/A    No         22m ago    Has a FileSystem, Insufficient space (<10 extents) on vgs, LVM detected
  nigeltufnel     /dev/sdd  hdd                   SEAGATE_ST20000NM002D_ZVTBJNGC17010C3442787  18.1T          N/A    N/A    No         22m ago    Has a FileSystem, Insufficient space (<10 extents) on vgs, LVM detected

.. warning::
    Although the ``libstoragemgmt`` library issues standard SCSI (SES) inquiry calls,
    there is no guarantee that your hardware and firmware properly implement these standards.
    This can lead to erratic behavior and even bus resets on some older
    hardware. It is therefore recommended that, before enabling this feature,
    you first test your hardware's compatibility with ``libstoragemgmt`` to avoid
    unplanned interruptions to services.

    There are a number of ways to test compatibility, but the simplest is
    to use the cephadm shell to call ``libstoragemgmt`` directly: ``cephadm shell
    lsmcli ldl``. If your hardware is supported you should see something like
    this: 

    .. code-block:: console

      Path     | SCSI VPD 0x83    | Link Type | Serial Number      | Health Status
      ----------------------------------------------------------------------------
      /dev/sda | 50000396082ba631 | SAS       | 15P0A0R0FRD6       | Good
      /dev/sdb | 50000396082bbbf9 | SAS       | 15P0A0YFFRD6       | Good


After enabling ``libstoragemgmt`` support, the ``--wide`` output looks
something like this:

.. prompt:: bash #

  ceph orch device ls --wide

.. code-block:: console

  HOST            PATH      TYPE  TRANSPORT  RPM   DEVICE ID                                     SIZE  HEALTH  IDENT  FAULT  AVAILABLE  REFRESHED  REJECT REASONS
  davidsthubbins  /dev/sdc  hdd   SAS        7200  SEAGATE_ST20000NM002D_ZVTBJNGC17010W339UW25  18.1T  Good    Off    Off    No         22m ago    Has a FileSystem, Insufficient space (<10 extents) on vgs, LVM detected
  nigeltufnel     /dev/sdd  hdd   SAS        7200  SEAGATE_ST20000NM002D_ZVTBJNGC17010C3442787  18.1T  Good    Off    Off    No         22m ago    Has a FileSystem, Insufficient space (<10 extents) on vgs, LVM detected

In this example, ``libstoragemgmt`` has confirmed the health of the drives and the ability to
interact with the identification and fault LEDs on the drive enclosures. For further
information about interacting with these LEDs, refer to :ref:`devices`.

.. note::
    ``libstoragemgmt`` reports on SCSI, SAS, and SATA based local drives. Since
    release 1.9.0 (March 2021) it also has a health check for NVMe devices.
    There is no official support for SAN LUNs or exotic/complex metadevices.
    The ``libstoragemgmt`` release in use is the one in the Ceph container
    image.

Retrieve Exact Size of Block Devices
====================================

Run a command of the following form to discover the exact size of a block
device. The value returned here is used by the orchestrator when filtering based
on size:

.. prompt:: bash #

   cephadm shell ceph-volume inventory <device-path> --format json | jq .sys_api.human_readable_size

``ceph-volume`` reports this size in units of 1024, rounded to two decimal
places. The orchestrator reads the reported number, and the number in a
``size`` filter, with powers of 1000: ``T`` is 1000 ``G``. To match a device
exactly, write the filter with the number and the unit that ``ceph-volume``
reports.

Example
-------
The following provides a specific example of this command based upon the
general form of the command above:

.. prompt:: bash #

   cephadm shell ceph-volume inventory /dev/sdc --format json | jq .sys_api.human_readable_size

.. code-block:: console

   "3.64 TB"

A ``size`` filter of ``3.64T`` matches this device, and so does ``3640G``. A
filter of ``4T`` or ``3727G`` does not.

This procedure was developed by Frédéric Nass. See `this thread on the
[ceph-users] mailing list
<https://lists.ceph.io/hyperkitty/list/ceph-users@ceph.io/message/5BAAYFCQAZZDRSNCUPCVBNEPGJDARRZA/>`_
for discussion of this matter.

.. _cephadm-deploy-osds:

Deploy OSDs
===========

Listing Storage Devices
-----------------------

In order to deploy an OSD, there must be an available storage device or devices on
which the OSD will be deployed.

Run this command to display an inventory of storage devices on all cluster hosts:

.. prompt:: bash #

  ceph orch device ls

A storage device is considered *available* if all of the following
conditions are met:

* The device must have no partitions.
* The device must not have any LVM state.
* The device must not be mounted.
* The device must not contain a file system.
* The device must not contain a Ceph BlueStore OSD.
* The device must be at least 5 GiB (5,368,709,120 bytes) in size.

Ceph will not provision an OSD on a device that is not *available*.

Creating New OSDs
-----------------

There are multiple ways to create new OSDs:

* Consume any available and unused storage device:

  .. prompt:: bash #

    ceph orch apply osd --all-available-devices

* Create an OSD from a specific device on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd <host>:<device-path>

  For example:

  .. prompt:: bash #

    ceph orch daemon add osd host1:/dev/sdb

* Advanced OSD creation from specific devices on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd host1:data_devices=/dev/sda,/dev/sdb,db_devices=/dev/sdc,osds_per_device=2

* Create an OSD on a specific LVM logical volume on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd <host>:<lvm-path>

  For example:

  .. prompt:: bash #

    ceph orch daemon add osd host1701:/dev/vg_osd/lvm_osd1701

* You can use :ref:`drivegroups` to categorize devices based on their
  properties. This is useful to clarify which
  devices are available to consume. Properties include device type (SSD or
  HDD), device model names, size, and the hosts on which the devices exist:

  .. prompt:: bash #

    ceph orch apply -i spec.yml

.. warning:: When deploying new OSDs with ``cephadm``, ensure that the ``ceph-osd`` package is not installed on the target host. If it is installed, conflicts may arise in the management and control of the OSD that may lead to errors or unexpected behavior.

OSDs created with ``ceph orch daemon add osd`` are added to the ``osd.default``
service as managed OSDs with a valid spec. To attach an existing OSD to a
different managed service, run a command of the following form:

.. prompt:: bash #

   ceph orch osd set-spec-affinity <service_name> <osd_id(s)>

For example:

.. prompt:: bash #

   ceph orch osd set-spec-affinity osd.default_drive_group 0 1

Dry Run
-------

The ``--dry-run`` flag causes the orchestrator to present a preview of what
will happen without actually creating the OSDs.

For example:

.. prompt:: bash #

  ceph orch apply osd --all-available-devices --dry-run

.. code-block:: console

  WARNING! Dry-Runs are snapshots of a certain point in time and are bound
  to the current inventory setup. If any of these conditions change, the
  preview will be invalid. Please make sure to have a minimal
  timeframe between planning and applying the specs.

  ####################
  SERVICESPEC PREVIEWS
  ####################
  +---------------------------------------+
  |         SERVICESPEC PREVIEW           |
  +---------+------+--------+-------------+
  |SERVICE  |NAME  |ADD_TO  |REMOVE_FROM  |
  +---------+------+--------+-------------+
  +---------+------+--------+-------------+

  ################
  OSDSPEC PREVIEWS
  ################
  +---------------------------------------------------------------+
  |                       OSDSPEC PREVIEWS                        |
  +---------+-----------------------+-------+----------+----+-----+
  |SERVICE  |NAME                   |HOST   |DATA      |DB  |WAL  |
  +---------+-----------------------+-------+----------+----+-----+
  |osd      |all-available-devices  |node1  |/dev/vdb  |-   |-    |
  |osd      |all-available-devices  |node2  |/dev/vdc  |-   |-    |
  |osd      |all-available-devices  |node3  |/dev/vdd  |-   |-    |
  +---------+-----------------------+-------+----------+----+-----+

The first run may print ``Preview data is being generated.. Please re-run this
command in a bit.`` in place of the OSD table. Run the command again.

.. _cephadm-osd-declarative:

Declarative State
-----------------

The effect of ``ceph orch apply`` is persistent. This means that drives that
are added to the system after the ``ceph orch apply`` command completes will be
automatically detected and added to the cluster as specified.  It also means that drives that
become available (e.g. by zapping) after the ``ceph orch apply``
command completes will be automatically found and added to the cluster.

We will examine the effects of the following command:

.. prompt:: bash #

  ceph orch apply osd --all-available-devices

After running the above command: 

* When you add new drives to the cluster, they will automatically be used to
  create new OSDs.
* When you remove an OSD and clean the LVM physical volume, a new OSD will be
  created automatically.

If you want to avoid this behavior (disable automatic creation of OSD on available devices), use the ``unmanaged`` parameter:

.. prompt:: bash #

  ceph orch apply osd --all-available-devices --unmanaged=true

.. note::

    Keep these three facts in mind:

    - The default behavior of ``ceph orch apply`` causes ``cephadm`` to constantly reconcile. This means that ``cephadm`` creates OSDs as soon as new drives are detected.

    - Setting ``unmanaged: True`` disables the creation of OSDs. If ``unmanaged: True`` is set, nothing will happen even if you apply a new OSD service.

    - Since Tentacle, ``ceph orch daemon add osd`` places the OSDs that it
      creates in a managed OSD service named ``osd.default``. If that service
      does not exist, ``cephadm`` creates it with the host and the device
      paths given in the command.

For more on ``cephadm``, see also :ref:`cephadm-spec-unmanaged`.

.. _cephadm-osd-removal:

Remove an OSD
=============

Removing an OSD from a cluster involves two steps:

#. Evacuating all placement groups (PGs) from the OSD
#. Removing the PG-free OSD from the cluster

.. warning::
   ``ceph orch osd rm`` does not ask for confirmation: the removal is
   scheduled at once. With ``--zap``, the data on the OSD's drives is erased
   after the OSD is removed. To cancel a removal that has not finished, run
   ``ceph orch osd rm stop <osd_id(s)>``.

The following command performs these two steps:

.. prompt:: bash #

  ceph orch osd rm <osd_id(s)> [--replace] [--force] [--zap]

Example:

.. prompt:: bash #

  ceph orch osd rm 1701
  ceph orch osd rm 1138 --zap

Expected output without ``--zap``:

.. code-block:: console

  Scheduled OSD(s) for removal.
  VG/LV for the OSDs won't be zapped (--zap wasn't passed).
  Run the `ceph-volume lvm zap` command with `--destroy` against the VG/LV if you want them to be destroyed.

With ``--zap``, only the first line is printed.

An OSD that is not safe to destroy is not rejected: it stays in the removal
queue until its PGs have drained and ``ceph osd safe-to-destroy`` succeeds.
Use ``--force`` to remove an OSD without waiting for PGs to drain or for
``osd safe-to-destroy`` to succeed. Without ``--force``, ``cephadm`` skips
both waits only for an OSD whose daemon is in the ``error`` state (for example
when its backing devices are gone) and that holds no PGs. Adding the
``--zap`` flag directs the orchestrator to remove all LVM and partition
information from the OSD's drives, leaving it a blank slate for redeployment
or other reuse.

.. note::
    After removing OSDs, if the OSDs' drives
    become available, ``cephadm`` may automatically try to deploy more OSDs
    on these drives if they match an existing drivegroup spec. If you deployed
    the OSDs you are removing with a spec and don't want any new OSDs deployed on
    the drives after removal, it's best to modify the drivegroup spec before removal.
    Either set ``unmanaged: true`` to stop it from picking up new drives,
    or modify it in some way that it no longer matches the drives used for the
    OSDs you wish to remove. Then re-apply the spec. For more info on drivegroup
    specs see :ref:`drivegroups`. For more info on the declarative nature of
    ``cephadm`` in reference to deploying OSDs, see :ref:`cephadm-osd-declarative`.

Monitoring OSD State During OSD Removal
---------------------------------------

You can query the state of OSD operations during the process of removing OSDs
by running the following command:

.. prompt:: bash #

  ceph orch osd rm status

Expected output:

.. code-block:: console

  OSD  HOST         STATE                    PGS  REPLACE  FORCE  ZAP    DRAIN STARTED AT
  2    cephadm-dev  done, waiting for purge    0  True     False  False  2026-07-17T12:01:43.147684Z
  3    cephadm-dev  draining                  17  False    True   False  2026-07-17T12:01:45.162158Z
  4    cephadm-dev  started                   42  False    True   False

If the queue is empty, the command prints
``No OSD remove/replace operations reported``.


When no PGs are left on the OSD, it will be decommissioned and removed from the cluster.

.. note::
    After removing an OSD, if you wipe the LVM physical volume in the device used by the removed OSD, a new OSD will be created.
    For more information on this, read about the ``unmanaged`` parameter in :ref:`cephadm-osd-declarative`.

Stopping OSD Removal
--------------------

It is possible to stop queued OSD removals by using the following command:

.. prompt:: bash #

  ceph orch osd rm stop <osd_id(s)>

Example:

.. prompt:: bash #

  ceph orch osd rm stop 4

Expected output:

.. code-block:: console

  Stopped OSD(s) removal

This resets the state of the OSD and takes it off the removal queue.

.. _cephadm-replacing-an-osd:

Replacing an OSD
----------------

.. prompt:: bash #

  ceph orch osd rm <osd_id(s)> --replace [--force]

Example:

.. prompt:: bash #

  ceph orch osd rm 1701 --replace

Expected output:

.. code-block:: console

  Scheduled OSD(s) for removal.
  VG/LV for the OSDs won't be zapped (--zap wasn't passed).
  Run the `ceph-volume lvm zap` command with `--destroy` against the VG/LV if you want them to be destroyed.

The message is the same as for a removal without ``--replace``.

This follows the same procedure as the procedure in the :ref:`cephadm-osd-removal` section, with
one exception: the OSD is not permanently removed from the CRUSH hierarchy, but is
instead assigned the ``destroyed`` flag.

.. note::
    The new OSD that will replace the removed OSD must be created on the same host 
    as the OSD that was removed.

**Preserving the OSD ID**

The ``destroyed`` flag is used to determine which OSD IDs will be reused in the
next OSD deployment.

If you use :ref:`OSDSpecs <drivegroups>` for OSD deployment, your newly added drives will be assigned
the OSD IDs of their replaced counterparts. This assumes that the new drives
still match the OSDSpecs.

Use the ``--dry-run`` flag to ensure that the ``ceph orch apply osd`` 
command will do what you intend. The ``--dry-run`` flag shows what the
outcome of the command will be without executing any changes. When
you are satisfied that the command will do what you want, run the command
without the ``--dry-run`` flag.

.. tip::

  The name of your OSDSpec can be retrieved with the command ``ceph orch ls``.

Alternatively, you can use an OSDSpec file:

.. prompt:: bash #

  ceph orch apply -i <osd_spec_file> --dry-run

Expected output:

.. code-block:: console

  WARNING! Dry-Runs are snapshots of a certain point in time and are bound
  to the current inventory setup. If any of these conditions change, the
  preview will be invalid. Please make sure to have a minimal
  timeframe between planning and applying the specs.

  ####################
  SERVICESPEC PREVIEWS
  ####################
  +---------------------------------------+
  |         SERVICESPEC PREVIEW           |
  +---------+------+--------+-------------+
  |SERVICE  |NAME  |ADD_TO  |REMOVE_FROM  |
  +---------+------+--------+-------------+
  +---------+------+--------+-------------+

  ################
  OSDSPEC PREVIEWS
  ################
  +------------------------------------------------------------+
  |                     OSDSPEC PREVIEWS                       |
  +---------+--------------------+-------+----------+----+-----+
  |SERVICE  |NAME                |HOST   |DATA      |DB  |WAL  |
  +---------+--------------------+-------+----------+----+-----+
  |osd      |<name_of_osd_spec>  |node1  |/dev/vdb  |-   |-    |
  +---------+--------------------+-------+----------+----+-----+


When this output reflects your intent, omit the ``--dry-run`` flag to
execute the deployment.


Erasing Devices (Zapping Devices)
---------------------------------

Erase (zap) a device so that it can be reused. ``ceph orch device zap`` runs
``ceph-volume lvm zap --destroy <path>`` on the remote host.

.. warning:: This command erases the device. Without ``--force`` the command
   stops with ``must pass --force to PERMANENTLY ERASE DEVICE DATA``.

.. prompt:: bash #

  ceph orch device zap <hostname> <path> --force

Example command:

.. prompt:: bash #

  ceph orch device zap my_hostname /dev/sdx --force

.. note::
    If the ``unmanaged`` flag is not set, ``cephadm`` automatically deploys drives that
    match the OSDSpec.  For example, if you specify the
    ``all-available-devices`` option when creating OSDs, when you ``zap`` a
    device the ``cephadm`` orchestrator automatically creates a new OSD on the
    device.  To disable this behavior, see :ref:`cephadm-osd-declarative`.


.. _osd_autotune:

Automatically Tuning OSD Memory
===============================

OSD daemons will adjust their memory consumption based on the
:confval:`osd_memory_target` config option.  If Ceph is deployed
on dedicated nodes that are not sharing
memory with other services, ``cephadm`` will automatically adjust the per-OSD
memory consumption target based on the total amount of RAM and the number of deployed
OSDs.  This allows the full use of available memory, and adapts when OSDs or
RAM are added or removed.

.. warning:: Cephadm sets :confval:`osd_memory_target_autotune` to ``true`` by
   default which is usually not appropriate for converged architectures, where
   a given node is used for both Ceph and compute purposes.

``Cephadm`` multiplies the total memory of the host by
:confval:`mgr/cephadm/autotune_memory_target_ratio`, subtracts the memory set
aside for daemons that are not autotuned (non-OSDs and OSDs for which
:confval:`osd_memory_target_autotune` is ``false``), and divides the
balance by the number of autotuned OSDs.

The final targets are reflected in the config database with options like the below:

.. code-block:: console

  WHO   MASK      LEVEL   OPTION              VALUE
  osd   host:foo  basic   osd_memory_target   126092301926
  osd   host:bar  basic   osd_memory_target   6442450944

Both the limits and the current memory consumed by each daemon are visible from
the ``ceph orch ps`` output in the ``MEM USE`` and ``MEM LIM`` columns:

.. code-block:: console

  NAME   HOST  PORTS  STATUS         REFRESHED  AGE  MEM USE  MEM LIM  VERSION  IMAGE ID      CONTAINER ID
  osd.1  dael         running (3h)     10s ago   3h    71.1M     117G  20.2.4   7015fda3cd67  9e183363d39c
  osd.2  dael         running (81m)    10s ago  81m    62.4M     117G  20.2.4   7015fda3cd67  1f0cc479b051
  osd.3  dael         running (62m)    10s ago  62m    62.5M     117G  20.2.4   7015fda3cd67  ac5537492f27

To exclude an OSD from memory autotuning, disable the ``autotune`` option
for that OSD and also set a specific memory target.  For example,

.. prompt:: bash #

  ceph config set osd.1701 osd_memory_target_autotune false
  ceph config set osd.1701 osd_memory_target 16G


.. _drivegroups:

Advanced OSD Service Specifications
===================================

:ref:`orchestrator-cli-service-spec`\s of type ``osd`` provide a way to use the
properties of drives to describe a Ceph cluster's layout. Service specifications
are an abstraction used to tell Ceph which drives to transform into OSDs
and which configurations to apply to those OSDs.
:ref:`orchestrator-cli-service-spec`\s make it possible to target drives
for transformation into OSDs even when the Ceph cluster operator does not know
the specific device names and paths associated with those disks.

:ref:`orchestrator-cli-service-spec`\s make it possible to define a YAML
or JSON file that can be used to reduce the amount of manual work involved
in creating OSDs.

.. note::
   An OSD spec applied with ``ceph orch apply -i`` must include the
   ``service_id`` field. A spec without it is rejected with the error
   ``Please provide the service_id field in your OSD spec``. OSDs created
   with ``ceph orch apply osd --all-available-devices`` belong to the
   ``osd.all-available-devices`` service. Since Tentacle, OSDs created with
   ``ceph orch daemon add osd`` belong to the ``osd.default`` service.

For example, instead of running the following command:

.. prompt:: bash [monitor.1]#

  ceph orch daemon add osd <host>:<path-to-device>

for each device and each host, we can create a ``.yaml`` or ``.json`` file that
allows us to describe the layout. Here is the most basic example:

Create a file called (for example) ``osd_spec.yml``:

.. code-block:: yaml

    service_type: osd
    service_id: default_drive_group  # custom name of the osd spec
    placement:
      host_pattern: '*'              # which hosts to target
    spec:
      data_devices:                  # the type of devices you are applying specs to
        all: true                    # a filter, check below for a full list

This means :

#. Turn any available device (``ceph-volume`` decides which are *available*) into an
   OSD on all hosts that match the glob pattern '*'. The glob pattern matches
   registered hosts from ``ceph orch host ls``. See
   :ref:`cephadm-services-placement-by-pattern-matching` for more on using
   ``host_pattern`` matching to use devices for OSDs.

#. Apply ``osd_spec.yml`` by using the following command:

   .. prompt:: bash [monitor.1]#

     ceph orch apply -i /path/to/osd_spec.yml

   This specification is applied to all the matching hosts to deploy OSDs.

   Strategies more complex than the one specified by the ``all`` filter are
   possible. See :ref:`osd_filters` for details.

   A ``--dry-run`` flag can be passed to the ``apply osd`` command to display a
   synopsis of the proposed layout.

Example

.. prompt:: bash [monitor.1]#

  ceph orch apply -i /path/to/osd_spec.yml --dry-run



.. _osd_filters:

Filters
-------

.. note::
    Filters are applied using an ``AND`` operation by default. This means that a drive
    must match all filter criteria to be selected. This behavior can
    be adjusted by setting ``filter_logic: OR`` in the OSD specification. 

Filters are used to select sets of drives for OSD data or WAL+DB offload based
on various attributes. These attributes are gathered by ``ceph-volume``'s drive
inventory. Retrieve these attributes with this command:

.. prompt:: bash #

    ceph-volume inventory </path/to/drive>

Vendor or Model
^^^^^^^^^^^^^^^

Specific drives can be targeted by vendor (brand, manufacturer) or model (SKU):

.. code-block:: yaml

    model: drive_model_name

or

.. code-block:: yaml

    vendor: drive_vendor_name


Size
^^^^

Specific drive capacities can be targeted with ``size``:

.. code-block:: yaml

    size: size_spec

Size specs
__________

Size specifications can be of the following forms:

* LOW:HIGH
* :HIGH
* LOW:
* EXACT

We explore examples below.

To match only drives of an exact capacity:

.. code-block:: yaml

    size: '10T'

Note that drive capacity is often not an exact multiple of units, so it is
often best practice to match drives within a range of sizes as shown below.
This handles future drives of the same class that may be of a different
model and thus slightly different in size.  Or say you have 10 TB drives
today but may add 16 TB drives next year. Write the range in the sizes that
``ceph-volume`` reports: it reports a drive sold as 10 TB as about ``9.10 TB``
and a drive sold as 16 TB as about ``14.55 TB``. To match both:

.. code-block:: yaml

    size: '9T:40T'

To match only drives that are less than or equal to 1701 GB in size:

.. code-block:: yaml

    size: ':1701G'

To include drives equal to or greater than 666 GB in size:

.. code-block:: yaml

    size: '666G:'

The supported units of size are Megabyte (``M``), Gigabyte (``G``) and Terabyte (``T``).
The ``B`` (*byte*) suffix for units is also acceptable: ``MB``, ``GB``, ``TB``.


Rotational
^^^^^^^^^^

This gates based on the 'rotational' attribute of each drive, as indicated by
the kernel.  This attribute is usually as expected for bare HDDs and SSDs
installed in each node.  Exotic or layered device presentations may however
be reported differently than you might expect or desire:

* Network-accessed SAN LUNs attached to the node
* Composite devices presented by dm-cache, Bcache, OpenCAS, etc.

In such cases you may align the kernel's reporting with your expectations
by adding a ``udev`` rule to override the default behavior.  The below rule
was used for this purpose to override the ``rotational`` attribute on OSD
nodes with no local physical drives and only attached SAN LUNs. It is not
intended for deployment in all scenarios; you will have to determine what is
right for your systems.  If by emplacing such a rule you summon eldritch horrors
from beyond spacetime, that's on you.

.. code-block:: none

    ACTION=="add|change", KERNEL=="sd[a-z]*", ATTR{queue/rotational}="0"
    ACTION=="add|change", KERNEL=="dm*", ATTR{queue/rotational}="0"
    ACTION=="add|change", KERNEL=="nbd*", ATTR{queue/rotational}="0"

Spec file syntax:

.. code-block:: yaml

    rotational: 0 | 1

``1`` to match all drives that the kernel indicates are rotational

``0`` to match all drives that are non-rotational (SATA, SATA, NVMe SSDs, SAN LUNs, etc)


All
^^^

This matches all drives that are available, i.e. they are free of partitions,
GPT labels, etc.

.. note:: This may only be specified for ``data_devices``.

.. code-block:: yaml

    all: true


Limiter
^^^^^^^

If filters are specified but you wish to limit the number of drives that they
match, use the ``limit`` attribute.  This is useful when one uses some
drives for non-Ceph purposes, or when multiple OSD strategies are
intended.

.. code-block:: yaml

    limit: 2

For example, when using ``vendor`` to match all drives branded ``VendorA``
but you wish to use at most two of them per host as OSDs, specify a ``limit``:

.. code-block:: yaml

    data_devices:
      vendor: VendorA
      limit: 2

.. note:: ``limit`` is usually appropriate in only certain specific scenarios.


Additional Options
------------------

There are multiple optional settings that specify the way OSDs are deployed.
Add these options to an OSD spec for them to take effect.

This example deploys encrypted OSDs on all unused drives.  Note that if Linux
MD mirroring is used for the boot, ``/var/log``, or other volumes this spec *may*
grab replacement or added drives before you can employ them for non-OSD purposes.
The ``unmanaged`` attribute may be set to pause automatic deployment until you
are ready.

.. code-block:: yaml

    service_type: osd
    service_id: example_osd_spec
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        all: true
      encrypted: true

Ceph Squid onwards support TPM2 token enrollment for LUKS2 devices.
Add the ``tpm2`` attribute to the OSD spec:

.. code-block:: yaml

    service_type: osd
    service_id: example_osd_spec_with_tpm2
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        all: true
      encrypted: true
      tpm2: true

``ceph-objectstore-tool`` and ``ceph-bluestore-tool`` do not unlock
LUKS. After a cephadm OSD stop, reopen the mapping before using those
tools. See :ref:`cephadm-encrypted-osd-store-tools`.

A full list of supported attributes:

.. py:currentmodule:: ceph.deployment.drive_group

.. autoclass:: DriveGroupSpec
   :members:
   :exclude-members: from_json


Examples
========

The Simple Case
---------------

When all cluster nodes have identical drives and we wish to use
them all as OSDs with offloaded WAL+DB:

.. code-block:: none

    10 HDDs
    Vendor: VendorA
    Model: HDD-123-foo
    Size: 4TB

    2 SAS/SATA SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

This is a common arrangement and can be described easily:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_default
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        model: HDD-123-foo       # Note, HDD-123 would also be valid
      db_devices:
        model: MC-55-44-ZX       # Same here, MC-55-44 is valid

However, we can improve the OSD specification by filtering based on properties
of the drives instead of specific models, as models may change over time as
drives are replaced or added:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_default
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        rotational: 1            # The kernel flags as HDD
      db_devices:
        rotational: 0            # The kernel flags as SSD (SAS/SATA/NVMe)

Here designate all HDDs to be data devices (OSDs) and all SSDs to be used
for WAL+DB offload.

If you know that drives larger than 2 TB should always be used as data devices,
and drives smaller than 2 TB should always be used as WAL/DB devices, you can
filter by size:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_default
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        size: '2TB:'           # Drives larger than 2 TB
      db_devices:
        size: ':2TB'           # Drives smaller than 2TB

.. note:: All of the above OSD specs are equally valid. Which you use depends on taste and on how much you expect your node layout to change.


Multiple OSD Specs for a Single Host
------------------------------------

Here we specify two distinct strategies for deploying OSDs across multiple
types of media, usually for use by separate pools:

.. code-block:: none

    10 HDDs
    Vendor: VendorA
    Model: HDD-123-foo
    Size: 4TB

    12 SAS/SATA SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

    2 NVME SSDs
    Vendor: VendorC
    Model: NVME-QQQQ-987
    Size: 256GB


* 10 HDD OSDs use 2 SATA/SAS SSDs for WAL+DB offload
* 10 SATA/SAS SSD OSDs share 2 NVMe SSDs for WAL+DB offload

This can be specificed with two service specs in the same file:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_hdd
    placement:
      host_pattern: '*'
    spec:
      data_devices:             # Select all drives the kernel identifies as HDDs
        rotational: 1           #  for OSD data
      db_devices:
        model: MC-55-44-ZX      # Select only this model for WAL+DB offload
        limit: 2                # Select at most two for this purpose
      db_slots: 5               # Chop the DB device into this many slices and
                                #  use one for each of this many HDD OSDs
    ---
    service_type: osd
    service_id: osd_spec_ssd    # Unique so it doesn't overwrite the above
    placement:
      host_pattern: '*'
    spec:                       # This scenario is uncommon
      data_devices:
        model: MC-55-44-ZX      # Select drives of this model for OSD data
      db_devices:               # Select drives of this brand for WAL+DB. Since the
        vendor: VendorC         #   data devices are SAS/SATA SSDs this would make sense for NVMe SSDs
      db_slots: 5               # Five DB slices on each NVMe SSD, one for
                                #  each of five SAS/SATA SSD OSDs

This would create the desired layout by using all HDDs as data devices with two
SATA/SAS SSDs assigned as dedicated DB/WAL devices, each backing five HDD OSDs.
The remaining ten SAS/SATA SSDs will be
used as OSD data devices, with ``VendorC`` NVMEs SSDs assigned as
dedicated DB/WAL devices, each serving five SAS/SATA OSDs.  We call these _hybrid OSDs.

Multiple Hosts with the same Disk Layout
----------------------------------------

When a cluster comprises hosts with different drive layouts, or a complex
constellation of multiple media types, it is recommended to apply
multiple OSD specs, each matching only one set of hosts.
Typically you will have a single spec for each type of host.

The ``service_id`` must be unique: if a new OSD spec with an already
applied ``service_id`` is applied, the existing OSD spec will be superseded.
Cephadm will then create new OSD daemons on unused drives based on the new spec
definition. Existing OSD daemons will not be affected. See :ref:`cephadm-osd-declarative`.

Example:

Nodes 1-5:

.. code-block:: none

    10 HDDs
    Vendor: VendorA
    Model: SSD-123-foo
    Size: 4TB
    2 SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

Nodes 6-10:

.. code-block:: none

    5 NVMEs
    Vendor: VendorA
    Model: SSD-123-foo
    Size: 4TB
    20 SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

You can specify a ``placement`` to target only certain nodes.

.. code-block:: yaml

    service_type: osd
    service_id: disk_layout_a
    placement:
      label: disk_layout_a
    spec:
      data_devices:
        rotational: 1           # All drives identified as HDDs
      db_devices:
        rotational: 0           # All drives identified as SSDs
    ---
    service_type: osd
    service_id: disk_layout_b
    placement:
      label: disk_layout_b
    spec:
      data_devices:
        model: MC-55-44-ZX      # Only this model
      db_devices:
        model: SSD-123-foo      # Only this model


This applies different OSD specs to different hosts that match hosts
tagged with ``ceph orch`` labels via the ``placement`` filter.
For more information, see :ref:`orchestrator-cli-placement-spec`.

.. note::

    Assuming each host has a unique disk layout, each OSD 
    spec must have a unique ``service_id``.


Dedicated WAL + DB
------------------

All previous cases colocated the WALs with the DBs.
It is however possible to deploy the WAL on a separate device if desired.

.. code-block:: none

    10 HDDs
    Vendor: VendorA
    Model: SSD-123-foo
    Size: 4TB

    2 SAS/SATA SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

    2 NVME SSDs
    Vendor: VendorC
    Model: NVME-QQQQ-987
    Size: 256GB


The OSD spec for this case would look like the following, using the ``model`` filter:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_default
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        model: SSD-123-foo
      db_devices:
        model: MC-55-44-ZX
      wal_devices:
        model: NVME-QQQQ-987


It is also possible to specify device paths as below, when every matched host
is expected to present devices identically.

.. code-block:: yaml

    service_type: osd
    service_id: osd_using_paths
    placement:
      hosts:
        - node01
        - node02
    spec:
      data_devices:
        paths:
        - /dev/sdb
      db_devices:
        paths:
        - /dev/sdc
      wal_devices:
        paths:
        - /dev/sdd


In most cases it is preferable to accomplish this with other filters
including ``size`` or ``vendor`` so that OSD services adapt when
Linux or an HBA may enumerate devices differently across boots, or when
drives are added or replaced.

It is possible to specify a ``crush_device_class`` parameter
to be applied to OSDs created on devices matched by the ``paths`` filter:

.. code-block:: yaml

    service_type: osd
    service_id: osd_using_paths
    placement:
      hosts:
        - node01
        - node02
    crush_device_class: ssd
    spec:
      data_devices:
        paths:
        - /dev/sdb
        - /dev/sdc
      db_devices:
        paths:
        - /dev/sdd
      wal_devices:
        paths:
        - /dev/sde

The ``crush_device_class`` attribute may be specified at OSD granularity
via the ``paths`` keyword with the following syntax:

.. code-block:: yaml

    service_type: osd
    service_id: osd_using_paths
    placement:
      hosts:
        - node01
        - node02
    crush_device_class: ssd
    spec:
      data_devices:
        paths:
        - path: /dev/sdb
          crush_device_class: ssd
        - path: /dev/sdc
          crush_device_class: nvme
      db_devices:
        paths:
        - /dev/sdd
      wal_devices:
        paths:
        - /dev/sde


.. _cephadm-osd-collocated-db:

DB on the Same Device (Collocated LV Split)
-------------------------------------------

.. versionadded:: 19.2.3

It is possible to deploy OSDs where the ``block.db`` LV is carved out of the
**same physical device** as the data LV, without requiring a separate DB device.
This provides the BlueStore non-collocated layout (data and metadata on separate
LVs) using only a single device per OSD which helps to mitigate BlueStore fragmentation.

This is achieved by specifying ``block_db_size`` **without** ``db_devices`` in
the OSD spec. ``cephadm`` will instruct ``ceph-volume lvm batch`` to create two
LVs per disk: one for data and one for ``block.db``.

.. code-block:: yaml

    service_type: osd
    service_id: osd_collocated_db
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        paths:
          - /dev/sdb
          - /dev/sdc
      block_db_size: 4G           # Carve a 4 GB DB LV out of each data device

The result is that each HDD will host two LVs:

.. code-block:: none

    data     /dev/sdb   196.00 GB   98.00%
    block_db /dev/sdb     4.00 GB    2.00%

This also works with explicit device paths and ``osds_per_device``:

.. code-block:: yaml

    service_type: osd
    service_id: osd_collocated_db_paths
    placement:
      hosts:
        - node01
        - node02
    spec:
      data_devices:
        paths:
          - /dev/sdb
          - /dev/sdc
      block_db_size: 4G           # Carve a DB LV out of each data device
      osds_per_device: 2          # Multiple OSDs per disk, each with its own DB LV

.. note::

    ``block_db_size`` is mandatory for this configuration. ``ceph-volume`` cannot
    derive a sensible default DB size when both data and DB share the same device.
    If the requested size cannot be satisfied, no OSDs will be deployed.

.. note::

    This feature was introduced in Squid 19.2.3. On older releases, you can achieve
    the same result by manually creating the LVs in advance and passing them
    explicitly to ``ceph-volume lvm prepare --bluestore --data <data-vg/lv> --block.db <db-vg/lv>``.

.. _cephadm-osd-activate:

Activate Existing OSDs
======================

If a host's operating system has been reinstalled, existing OSDs
must be activated again. ``cephadm`` provides a wrapper for
:ref:`ceph-volume-lvm-activate` that activates all existing OSDs on a host.

The following procedure explains how to use ``cephadm`` to activate OSDs on a
host that has had its operating system reinstalled.

This example applies to two hosts: ``ceph01`` and ``ceph04``.

- ``ceph01`` is a host equipped with an admin keyring. 
- ``ceph04`` is the host with the recently reinstalled operating system.

#. Install ``cephadm`` and ``podman`` on the host. The command for installing
   these utilities will depend upon the operating system of the host.

#. Retrieve the public key.

   .. prompt:: bash ceph01#
      
      cd /tmp ; ceph cephadm get-pub-key > ceph.pub

#. Copy the key (from ``ceph01``) to the freshly reinstalled host (``ceph04``):

   .. prompt:: bash ceph01#

      ssh-copy-id -f -i ceph.pub root@<hostname>

#. Retrieve the private key in order to test the connection. Create the
   file so that only its owner can read it:

   .. prompt:: bash ceph01#

      (umask 077 ; ceph config-key get mgr/cephadm/ssh_identity_key > ~/ceph-private.key)

#. Log in to ``ceph04`` from ``ceph01`` to test the connection and
   configuration:

   .. prompt:: bash ceph01#

      ssh -i ~/ceph-private.key ceph04

#. While logged into ``ceph01``, remove ``ceph.pub`` and ``ceph-private.key``:

   .. prompt:: bash ceph01#

      rm /tmp/ceph.pub ~/ceph-private.key

#. If you run your own container registry, instruct the orchestrator to log into
   it on each host: 

   .. prompt:: bash #

      ceph cephadm registry-login my-registry.domain <user> <password>

   When the orchestrator performs the registry login, it will attempt to deploy
   any missing daemons to the host. This includes ``crash``, ``node-exporter``,
   and any other daemons that the host ran before its operating system was
   reinstalled.

   To be clear: ``cephadm`` attempts to deploy missing daemons to all
   hosts managed by cephadm, when ``cephadm``
   determines that the hosts are online. In this context, "online" means
   "present in the output of the ``ceph orch host ls`` command and with a
   status that is not ``offline`` or ``maintenance``". If it is necessary to log
   in to the registry in order to pull the images for the missing daemons, then
   deployment of the missing daemons will fail until the process of logging
   in to the registry has been completed.

   .. note:: This step is not necessary if you do not run your own container
             registry. If the host is still in the host list, which can be
             retrieved by running the command ``ceph orch host ls``, you do
             not need to add it again.

#. Activate the OSDs on the host that has recently had its operating system
   reinstalled:

   .. prompt:: bash #

      ceph cephadm osd activate ceph04

   This command causes ``cephadm`` to scan all existing disks for OSDs. This
   command will make ``cephadm`` deploy any missing daemons to the host
   specified. 
  


*This procedure was developed by Eugen Block in Feburary of 2025, and a blog
post pertinent to its development can be seen here:*
`Eugen Block's "Cephadm: Activate existing OSDs" blog post <https://heiterbiswolkig.blogs.nde.ag/2025/02/06/cephadm-activate-existing-osds/>`_.

.. note::
    It is usually not safe to run ``ceph orch restart osd.myosdservice`` on a
    running cluster, as attention is not paid to CRUSH failure domains, and
    parallel OSD restarts may lead to temporary data unavailability or in rare
    cases even data loss.


Further Reading
===============

* :ref:`ceph-volume`
* :ref:`rados-index`
* :ref:`adding-and-removing-osds` (clusters not managed by cephadm)
