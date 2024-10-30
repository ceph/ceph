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

Example::

  Hostname  Path      Type  Serial              Size   Health   Ident  Fault  Available
  srv-01    /dev/sdb  hdd   15P0A0YFFRD6         300G  Unknown  N/A    N/A    No
  srv-01    /dev/sdc  hdd   15R0A08WFRD6         300G  Unknown  N/A    N/A    No
  srv-01    /dev/sdd  hdd   15R0A07DFRD6         300G  Unknown  N/A    N/A    No
  srv-01    /dev/sde  hdd   15P0A0QDFRD6         300G  Unknown  N/A    N/A    No
  srv-02    /dev/sdb  hdd   15R0A033FRD6         300G  Unknown  N/A    N/A    No
  srv-02    /dev/sdc  hdd   15R0A05XFRD6         300G  Unknown  N/A    N/A    No
  srv-02    /dev/sde  hdd   15R0A0ANFRD6         300G  Unknown  N/A    N/A    No
  srv-02    /dev/sdf  hdd   15R0A06EFRD6         300G  Unknown  N/A    N/A    No
  srv-03    /dev/sdb  hdd   15R0A0OGFRD6         300G  Unknown  N/A    N/A    No
  srv-03    /dev/sdc  hdd   15R0A0P7FRD6         300G  Unknown  N/A    N/A    No
  srv-03    /dev/sdd  hdd   15R0A0O7FRD6         300G  Unknown  N/A    N/A    No

In the above examples you can see fields named ``Health``, ``Ident``, and ``Fault``.
This information is provided by integration with `libstoragemgmt`_. By default,
this integration is disabled because `libstoragemgmt`_ may not be 100%
compatible with your hardware.  To direct Ceph to include these fields,
enable ``cephadm``'s "enhanced device scan" option as follows:

.. prompt:: bash #

  ceph config set mgr mgr/cephadm/device_enhanced_scan true

Note that the columns reported by ``ceph orch device ls`` may vary from release to
release.

The ``--wide`` option shows device details,
including any reasons that the device might not be eligible for use as an OSD.
Example (Reef)::

  HOST               PATH          TYPE  DEVICE ID                                      SIZE  AVAILABLE  REFRESHED  REJECT REASONS
  davidsthubbins    /dev/sdc       hdd   SEAGATE_ST20000NM002D_ZVTBJNGC17010W339UW25    18.1T  No         22m ago    Has a FileSystem, Insufficient space (<10 extents) on vgs, LVM detected
  nigeltufnel       /dev/sdd       hdd   SEAGATE_ST20000NM002D_ZVTBJNGC17010C3442787    18.1T  No         22m ago    Has a FileSystem, Insufficient space (<10 extents) on vgs, LVM detected

.. warning::
    Although the ``libstoragemgmt`` library issues standard SCSI (SES) inquiry calls,
    there is no guarantee that your hardware and firmware properly implement these standards.
    This can lead to erratic behaviour and even bus resets on some older
    hardware. It is therefore recommended that, before enabling this feature,
    you first test your hardware's compatibility with ``libstoragemgmt`` to avoid
    unplanned interruptions to services.

    There are a number of ways to test compatibility, but the simplest is
    to use the cephadm shell to call ``libstoragemgmt`` directly: ``cephadm shell
    lsmcli ldl``. If your hardware is supported you should see something like
    this: 

    ::

      Path     | SCSI VPD 0x83    | Link Type | Serial Number      | Health Status
      ----------------------------------------------------------------------------
      /dev/sda | 50000396082ba631 | SAS       | 15P0A0R0FRD6       | Good
      /dev/sdb | 50000396082bbbf9 | SAS       | 15P0A0YFFRD6       | Good


After enabling ``libstoragemgmt`` support, the output will look something
like this:

::

  # ceph orch device ls
  Hostname   Path      Type  Serial              Size   Health   Ident  Fault  Available
  srv-01     /dev/sdb  hdd   15P0A0YFFRD6         300G  Good     Off    Off    No
  srv-01     /dev/sdc  hdd   15R0A08WFRD6         300G  Good     Off    Off    No
  :

In this example, ``libstoragemgmt`` has confirmed the health of the drives and the ability to
interact with the identification and fault LEDs on the drive enclosures. For further
information about interacting with these LEDs, refer to :ref:`devices`.

.. note::
    The current release of `libstoragemgmt`` (1.8.8) supports SCSI, SAS, and SATA based
    local drives only. There is no official support for NVMe devices (PCIe), SAN LUNs,
    or exotic/complex metadevices.

Retrieve Exact Size of Block Devices
====================================

Run a command of the following form to discover the exact size of a block
device. The value returned here is used by the orchestrator when filtering based
on size:

.. prompt:: bash #

   cephadm shell ceph-volume inventory </dev/sda> --format json | jq .sys_api.human_readable_size

The exact size in GB is the size reported in TB, multiplied by 1024.

Example
-------
The following provides a specific example of this command based upon the
general form of the command above:

.. prompt:: bash #

   cephadm shell ceph-volume inventory /dev/sdc --format json | jq .sys_api.human_readable_size

::

   "3.64 TB"

This indicates that the exact device size is 3.64 TB, or 3727.36 GB.

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
* The device must be larger than 5 GB.

Ceph will not provision an OSD on a device that is not *available*.

Creating New OSDs
-----------------

There are multiple ways to create new OSDs:

* Consume any available and unused storage device:

  .. prompt:: bash #

    ceph orch apply osd --all-available-devices

* Create an OSD from a specific device on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd *<host>*:*<device-path>*

  For example:

  .. prompt:: bash #

    ceph orch daemon add osd host1:/dev/sdb

* Advanced OSD creation from specific devices on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd host1:data_devices=/dev/sda,/dev/sdb,db_devices=/dev/sdc,osds_per_device=2

* Create an OSD on a specific LVM logical volume on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd *<host>*:*<lvm-path>*

  For example:

  .. prompt:: bash #

    ceph orch daemon add osd host1:/dev/vg_osd/lvm_osd1701

* You can use :ref:`drivegroups` to categorize devices based on their
  properties. This is useful to clarify which
  devices are available to consume. Properties include device type (SSD or
  HDD), device model names, size, and the hosts on which the devices exist:

  .. prompt:: bash #

    ceph orch apply -i spec.yml

.. warning:: When deploying new OSDs with ``cephadm``, ensure that the ``ceph-osd`` package is not installed on the target host. If it is installed, conflicts may arise in the management and control of the OSD that may lead to errors or unexpected behavior.

* New OSDs created using ``ceph orch daemon add osd`` are added under ``osd.default`` as managed OSDs with a valid spec.

  To attach an existing OSD to a different managed service, ``ceph orch osd set-spec-affinity`` command can be used:

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

::

  NAME                  HOST  DATA      DB  WAL
  all-available-devices node1 /dev/vdb  -   -
  all-available-devices node2 /dev/vdc  -   -
  all-available-devices node3 /dev/vdd  -   -

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

    - ``ceph orch daemon add`` creates OSDs, but does not add an OSD service.

* For more on ``cephadm``, see also :ref:`cephadm-spec-unmanaged`.

.. _cephadm-osd-removal:

Remove an OSD
=============

Removing an OSD from a cluster involves two steps:

#. Evacuating all placement groups (PGs) from the OSD
#. Removing the PG-free OSD from the cluster

The following command performs these two steps:

.. prompt:: bash #

  ceph orch osd rm <osd_id(s)> [--replace] [--force] [--zap]

Example:

.. prompt:: bash #

  ceph orch osd rm 0
  ceph orch osd rm 1138 --zap

Expected output::

  Scheduled OSD(s) for removal

OSDs that are not safe to destroy will be rejected.  Adding the ``--zap`` flag
directs the orchestrator to remove all LVM and partition information from the
OSD's drives, leaving it a blank slate for redeployment or other reuse.

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
    ``cephadm`` in reference to deploying OSDs, see :ref:`cephadm-osd-declarative`

Monitoring OSD State During OSD Removal
---------------------------------------

You can query the state of OSD operations during the process of removing OSDs
by running the following command:

.. prompt:: bash #

  ceph orch osd rm status

Expected output::

  OSD_ID  HOST         STATE                    PG_COUNT  REPLACE  FORCE  STARTED_AT
  2       cephadm-dev  done, waiting for purge  0         True     False  2020-07-17 13:01:43.147684
  3       cephadm-dev  draining                 17        False    True   2020-07-17 13:01:45.162158
  4       cephadm-dev  started                  42        False    True   2020-07-17 13:01:45.162158


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

Expected output::

  Stopped OSD(s) removal

This resets the state of the OSD and takes it off the removal queue.

.. _cephadm-replacing-an-osd:

Replacing an OSD
----------------

.. prompt:: bash #

  ceph orch osd rm <osd_id(s)> --replace [--force]

Example:

.. prompt:: bash #

  ceph orch osd rm 4 --replace

Expected output::

  Scheduled OSD(s) for replacement

This follows the same procedure as the procedure in the "Remove OSD" section, with
one exception: the OSD is not permanently removed from the CRUSH hierarchy, but is
instead assigned the ``destroyed`` flag.

.. note::
    The new OSD that will replace the removed OSD must be created on the same host 
    as the OSD that was removed.

**Preserving the OSD ID**

The ``destroyed`` flag is used to determine which OSD IDs will be reused in the
next OSD deployment.

If you use OSDSpecs for OSD deployment, your newly added drives will be assigned
the OSD IDs of their replaced counterparts. This assumes that the new drives
still match the OSDSpecs.

Use the ``--dry-run`` flag to ensure that the ``ceph orch apply osd`` 
command will do what you intend. The ``--dry-run`` flag shows what the
outcome of the command will be without executing any changes. When
you are satisfied that the command will do what you want, run the command
without the ``--dry-run`` flag.

.. tip::

  The name of your OSDSpec can be retrieved with the command ``ceph orch ls``

Alternatively, you can use an OSDSpec file:

.. prompt:: bash #

  ceph orch apply -i <osd_spec_file> --dry-run

Expected output::

  NAME                  HOST  DATA     DB WAL
  <name_of_osd_spec>    node1 /dev/vdb -  -


When this output reflects your intent, omit the ``--dry-run`` flag to
execute the deployment.


Erasing Devices (Zapping Devices)
---------------------------------

Erase (zap) a device so that it can be reused. ``zap`` calls ``ceph-volume
zap`` on the remote host.

.. prompt:: bash #

  ceph orch device zap <hostname> <path>

Example command:

.. prompt:: bash #

  ceph orch device zap my_hostname /dev/sdx

.. note::
    If the ``unmanaged`` flag is not set, ``cephadm`` automatically deploys drives that
    match the OSDSpec.  For example, if you specify the
    ``all-available-devices`` option when creating OSDs, when you ``zap`` a
    device the ``cephadm`` orchestrator automatically creates a new OSD on the
    device.  To disable this behavior, see :ref:`cephadm-osd-declarative`.


.. _osd_autotune:

Automatically tuning OSD memory
===============================

OSD daemons will adjust their memory consumption based on the
:confval:`osd_memory_target` config option.  If Ceph is deployed
on dedicated nodes that are not sharing
memory with other services, ``cephadm`` will automatically adjust the per-OSD
memory consumption target based on the total amount of RAM and the number of deployed
OSDs.  This allows the full use of available memory, and adapts when OSDs or
RAM are added or removed.

.. warning:: Cephadm sets ``osd_memory_target_autotune`` to ``true`` by default which is usually not appropriate for converged architectures, where a given node is used for both Ceph and compute purposes.

``Cephadm`` will use a fraction
:confval:`mgr/cephadm/autotune_memory_target_ratio` of available memory,
subtracting memory consumed by non-autotuned daemons (non-OSDs and OSDs for which
``osd_memory_target_autotune`` is false), and then divide the balance by the number
of OSDs.

The final targets are reflected in the config database with options like the below::

  WHO   MASK      LEVEL   OPTION              VALUE
  osd   host:foo  basic   osd_memory_target   126092301926
  osd   host:bar  basic   osd_memory_target   6442450944

Both the limits and the current memory consumed by each daemon are visible from
the ``ceph orch ps`` output in the ``MEM LIMIT`` column::

  NAME        HOST  PORTS  STATUS         REFRESHED  AGE  MEM USED  MEM LIMIT  VERSION                IMAGE ID      CONTAINER ID  
  osd.1       dael         running (3h)     10s ago   3h    72857k     117.4G  17.0.0-3781-gafaed750  7015fda3cd67  9e183363d39c  
  osd.2       dael         running (81m)    10s ago  81m    63989k     117.4G  17.0.0-3781-gafaed750  7015fda3cd67  1f0cc479b051  
  osd.3       dael         running (62m)    10s ago  62m    64071k     117.4G  17.0.0-3781-gafaed750  7015fda3cd67  ac5537492f27  

To exclude an OSD from memory autotuning, disable the ``autotune`` option
for that OSD and also set a specific memory target.  For example,

.. prompt:: bash #

  ceph config set osd.123 osd_memory_target_autotune false
  ceph config set osd.123 osd_memory_target 16G


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

:ref:`orchestrator-cli-service-spec`\s make it possible to define a ``.yaml``
or ``.json`` file that can be used to reduce the amount of manual work involved
in creating OSDs.

.. note::
   We recommend that advanced OSD specs include the ``service_id`` field.
   OSDs created using ``ceph orch daemon add`` or ``ceph orch apply osd
   --all-available-devices`` are placed in the plain ``osd`` service. Failing
   to include a ``service_id`` in your OSD spec causes the Ceph cluster to mix
   the OSDs from your spec with those OSDs, which can potentially result in the
   overwriting of service specs created by ``cephadm`` to track them. Newer
   versions of ``cephadm`` block OSD specs that
   do not include the ``service_id``. 

For example, instead of running the following command:

.. prompt:: bash [monitor.1]#

  ceph orch daemon add osd *<host>*:*<path-to-device>*

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

#. Turn any available device (``ceph-volume` decides which are _available_) into an
   OSD on all hosts that match the glob pattern '*'. The glob pattern matches
   registered hosts from `ceph orch host ls`. See
   :ref:`cephadm-services-placement-by-pattern-matching` for more on using
   ``host_pattern`` matching to use devices for OSDs.

#. Pass ``osd_spec.yml`` to ``osd create`` by using the following command:

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
    Filters are applied using an `AND` operation by default. This means that a drive
    must match all filter criteria to be selected. This behavior can
    be adjusted by setting ``filter_logic: OR`` in the OSD specification. 

Filters are used to select sets of drives for OSD data or WAL+DB offload based
on various attributes. These attributes are gathered by ``ceph-volume``'s drive
inventory. Retrieve these attributes with this command:

.. code-block:: bash

    ceph-volume inventory </path/to/drive>

Vendor or Model
^^^^^^^^^^^^^^^

Specific drives can be targeted by vendor brand, manufacturer) or model (SKU):

.. code-block:: yaml

    model: drive_model_name

or

.. code-block:: yaml

    vendor: drive_vendor_name


Size
^^^^

Specific drive capacities can be targeted with `size`:

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
today but may add 16 TB drives next year:

.. code-block:: yaml

    size: '10T:40T'

To match only drives that are less than or equal to 1701 GB in size:

.. code-block:: yaml

    size: ':1701G'

To include drives equal to or greater than 666 GB in size:

.. code-block:: yaml

    size: '666G:'

The supported units of size are Megabyte(M), Gigabyte(G) and Terabyte(T).
The ``B`` (_byte_) suffix for units is also acceptable: ``MB``, ``GB``, ``TB``.


Rotational
^^^^^^^^^^

This gates based on the 'rotational' attribute of each drive, as indicated by
the kernel.  This attribute is usually as expected for bare HDDs and SSDs
installed in each node.  Exotic or layered device presentations may however
be reported differently than you might expect or desire:

* Network-accessed SAN LUNs attached to the node
* Composite devices presented by `dCache`, `Bcache`, `OpenCAS`, etc.

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

`1` to match all drives that the kernel indicates are rotational

`0` to match all drives that are non-rotational (SATA, SATA, NVMe SSDs, SAN LUNs, etc)


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
Add the `tpm2` attribute to the OSD spec:

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

See a full list in the DriveGroupSpecs

.. py:currentmodule:: ceph.deployment.drive_group

.. autoclass:: DriveGroupSpec
   :members:
   :exclude-members: from_json


Examples
========

The simple case
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
        model: MC-55-44-XZ       # Same here, MC-55-44 is valid

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


Multiple OSD specs for a single host
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
        model: MC-55-44-XZ      # Select only this model for WAL+DB offload
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
        model: MC-55-44-XZ      # Select drives of this model for OSD data
      db_devices:               # Select drives of this brand for WAL+DB. Since the
        vendor: VendorC         #   data devices are SAS/SATA SSDs this would make sense for NVMe SSDs
      db_slots: 2               # Back two slower SAS/SATA SSD data devices with each NVMe slice

This would create the desired layout by using all HDDs as data devices with two
SATA/SAS SSDs assigned as dedicated DB/WAL devices, each backing five HDD OSDs.
The remaining ten SAS/SATA SSDs will be
used as OSD data devices, with ``VendorC`` NVMEs SSDs assigned as
dedicated DB/WAL devices, each serving two SAS/SATA OSDs.  We call these _hybrid OSDs.

Multiple hosts with the same disk layout
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

    20 HDDs
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
        model: MC-55-44-XZ      # Only this model
      db_devices:
        model: SSD-123-foo      # Only this model


This applies different OSD specs to different hosts that match hosts
tagged with ``ceph orch`` labels via the ``placement`` filter.
See :ref:`orchestrator-cli-placement-spec`

.. note::

    Assuming each host has a unique disk layout, each OSD 
    spec must have a unique ``service_id``.


Dedicated WAL + DB
------------------

All previous cases colocated the WALs with the DBs.
It is however possible to deploy the WAL on a separate device if desired.

.. code-block:: none

    20 HDDs
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
        model: MC-55-44-XZ
      db_devices:
        model: SSD-123-foo
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

.. _cephadm-osd-activate:

Activate existing OSDs
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

#. Retrieve the private key in order to test the connection:

   .. prompt:: bash ceph01#

      cd /tmp ; ceph config-key get mgr/cephadm/ssh_identity_key > ceph-private.key

#. From ``ceph01``, modify the permissions of ``ceph-private.key``:

   .. prompt:: bash ceph01#

      chmod 400 ceph-private.key

#. Log in to ``ceph04`` from ``ceph01`` to test the connection and
   configuration:

   .. prompt:: bash ceph01#

      ssh -i /tmp/ceph-private.key ceph04

#. While logged into ``ceph01``, remove ``ceph.pub`` and ``ceph-private.key``:

   .. prompt:: bash ceph01#

      cd /tmp ; rm ceph.pub ceph-private.key

#. If you run your own container registry, instruct the orchestrator to log into
   it on each host: 

   .. prompt:: bash #

      ceph cephadm registry-login my-registry.domain <user> <password>

   When the orchestrator performs the registry login, it will attempt to deploy
   any missing daemons to the host. This includes ``crash``, ``node-exporter``,
   and any other daemons that the host ran before its operating system was
   reinstalled.

   To be clea: ``cephadm`` attempts to deploy missing daemons to all
   hosts managed by cephadm, when ``cephadm``
   determines that the hosts are online. In this context, "online" means
   "present in the output of the ``ceph orch host ls`` command and with a
   status that is not ``offline`` or ``maintenance``. If it is necessary to log
   in to the registry in order to pull the images for the missing daemons, then
   deployment of the missing daemons will fail until the process of logging
   in to the registry has been completed.

   .. note:: This step is not necessary if you do not run your own container
             registry. If your host is still in the "host list", which can be
             retrieved by running the command ``ceph orch host ls``, you do not
             need to run this command.

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
