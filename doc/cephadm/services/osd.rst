***********
OSD Service
***********
.. _device management: ../rados/operations/devices
.. _libstoragemgmt: https://github.com/libstorage/libstoragemgmt

List Devices
============

``ceph-volume`` scans each host in the cluster from time to time in order
to determine which devices are present and whether they are eligible to be
used as OSDs.

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

Using the ``--wide`` option provides all details relating to the device,
including any reasons that the device might not be eligible for use as an OSD.

In the above example you can see fields named "Health", "Ident", and "Fault".
This information is provided by integration with `libstoragemgmt`_. By default,
this integration is disabled (because `libstoragemgmt`_ may not be 100%
compatible with your hardware).  To make ``cephadm`` include these fields,
enable cephadm's "enhanced device scan" option as follows;

.. prompt:: bash #

  ceph config set mgr mgr/cephadm/device_enhanced_scan true

.. warning::
    Although the libstoragemgmt library performs standard SCSI inquiry calls,
    there is no guarantee that your firmware fully implements these standards.
    This can lead to erratic behaviour and even bus resets on some older
    hardware. It is therefore recommended that, before enabling this feature,
    you test your hardware's compatibility with libstoragemgmt first to avoid
    unplanned interruptions to services.

    There are a number of ways to test compatibility, but the simplest may be
    to use the cephadm shell to call libstoragemgmt directly - ``cephadm shell
    lsmcli ldl``. If your hardware is supported you should see something like
    this: 

    ::

      Path     | SCSI VPD 0x83    | Link Type | Serial Number      | Health Status
      ----------------------------------------------------------------------------
      /dev/sda | 50000396082ba631 | SAS       | 15P0A0R0FRD6       | Good
      /dev/sdb | 50000396082bbbf9 | SAS       | 15P0A0YFFRD6       | Good


After you have enabled libstoragemgmt support, the output will look something
like this:

::

  # ceph orch device ls
  Hostname   Path      Type  Serial              Size   Health   Ident  Fault  Available
  srv-01     /dev/sdb  hdd   15P0A0YFFRD6         300G  Good     Off    Off    No
  srv-01     /dev/sdc  hdd   15R0A08WFRD6         300G  Good     Off    Off    No
  :

In this example, libstoragemgmt has confirmed the health of the drives and the ability to
interact with the Identification and Fault LEDs on the drive enclosures. For further
information about interacting with these LEDs, refer to `device management`_.

.. note::
    The current release of `libstoragemgmt`_ (1.8.8) supports SCSI, SAS, and SATA based
    local disks only. There is no official support for NVMe devices (PCIe)

.. _cephadm-deploy-osds:

Deploy OSDs
===========

Listing Storage Devices
-----------------------

In order to deploy an OSD, there must be a storage device that is *available* on
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

Ceph will not provision an OSD on a device that is not available.

Creating New OSDs
-----------------

There are a few ways to create new OSDs:

* Tell Ceph to consume any available and unused storage device:

  .. prompt:: bash #

    ceph orch apply osd --all-available-devices

* Create an OSD from a specific device on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd *<host>*:*<device-path>*

  For example:

  .. prompt:: bash #

    ceph orch daemon add osd host1:/dev/sdb

  Advanced OSD creation from specific devices on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd host1:data_devices=/dev/sda,/dev/sdb,db_devices=/dev/sdc,osds_per_device=2

* Create an OSD on a specific LVM logical volume on a specific host:

  .. prompt:: bash #

    ceph orch daemon add osd *<host>*:*<lvm-path>*

  For example:

  .. prompt:: bash #

    ceph orch daemon add osd host1:/dev/vg_osd/lvm_osd1701

* You can use :ref:`drivegroups` to categorize device(s) based on their
  properties. This might be useful in forming a clearer picture of which
  devices are available to consume. Properties include device type (SSD or
  HDD), device model names, size, and the hosts on which the devices exist:

  .. prompt:: bash #

    ceph orch apply -i spec.yml

.. warning:: When deploying new OSDs with ``cephadm``, ensure that the ``ceph-osd`` package is not already installed on the target host. If it is installed, conflicts may arise in the management and control of the OSD that may lead to errors or unexpected behavior.

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
automatically found and added to the cluster.  It also means that drives that
become available (by zapping, for example) after the ``ceph orch apply``
command completes will be automatically found and added to the cluster.

We will examine the effects of the following command:

.. prompt:: bash #

  ceph orch apply osd --all-available-devices

After running the above command: 

* If you add new disks to the cluster, they will automatically be used to
  create new OSDs.
* If you remove an OSD and clean the LVM physical volume, a new OSD will be
  created automatically.

If you want to avoid this behavior (disable automatic creation of OSD on available devices), use the ``unmanaged`` parameter:

.. prompt:: bash #

  ceph orch apply osd --all-available-devices --unmanaged=true

.. note::

    Keep these three facts in mind:

    - The default behavior of ``ceph orch apply`` causes cephadm constantly to reconcile. This means that cephadm creates OSDs as soon as new drives are detected.

    - Setting ``unmanaged: True`` disables the creation of OSDs. If ``unmanaged: True`` is set, nothing will happen even if you apply a new OSD service.

    - ``ceph orch daemon add`` creates OSDs, but does not add an OSD service.

* For cephadm, see also :ref:`cephadm-spec-unmanaged`.

.. _cephadm-osd-removal:

Remove an OSD
=============

Removing an OSD from a cluster involves two steps:

#. evacuating all placement groups (PGs) from the OSD
#. removing the PG-free OSD from the cluster

The following command performs these two steps:

.. prompt:: bash #

  ceph orch osd rm <osd_id(s)> [--replace] [--force]

Example:

.. prompt:: bash #

  ceph orch osd rm 0

Expected output::

  Scheduled OSD(s) for removal

OSDs that are not safe to destroy will be rejected.

.. note::
    After removing OSDs, if the drives the OSDs were deployed on once again
    become available, cephadm may automatically try to deploy more OSDs
    on these drives if they match an existing drivegroup spec. If you deployed
    the OSDs you are removing with a spec and don't want any new OSDs deployed on
    the drives after removal, it's best to modify the drivegroup spec before removal.
    Either set ``unmanaged: true`` to stop it from picking up new drives at all,
    or modify it in some way that it no longer matches the drives used for the
    OSDs you wish to remove. Then re-apply the spec. For more info on drivegroup
    specs see :ref:`drivegroups`. For more info on the declarative nature of
    cephadm in reference to deploying OSDs, see :ref:`cephadm-osd-declarative`

Monitoring OSD State
--------------------

You can query the state of OSD operation with the following command:

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

This resets the initial state of the OSD and takes it off the removal queue.

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
instead assigned a 'destroyed' flag.

.. note::
    The new OSD that will replace the removed OSD must be created on the same host 
    as the OSD that was removed.

**Preserving the OSD ID**

The 'destroyed' flag is used to determine which OSD ids will be reused in the
next OSD deployment.

If you use OSDSpecs for OSD deployment, your newly added disks will be assigned
the OSD ids of their replaced counterparts. This assumes that the new disks
still match the OSDSpecs.

Use the ``--dry-run`` flag to make certain that the ``ceph orch apply osd`` 
command does what you want it to. The ``--dry-run`` flag shows you what the
outcome of the command will be without making the changes you specify. When
you are satisfied that the command will do what you want, run the command
without the ``--dry-run`` flag.

.. tip::

  The name of your OSDSpec can be retrieved with the command ``ceph orch ls``

Alternatively, you can use your OSDSpec file:

.. prompt:: bash #

  ceph orch apply -i <osd_spec_file> --dry-run

Expected output::

  NAME                  HOST  DATA     DB WAL
  <name_of_osd_spec>    node1 /dev/vdb -  -


When this output reflects your intention, omit the ``--dry-run`` flag to
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
    If the unmanaged flag is unset, cephadm automatically deploys drives that
    match the OSDSpec.  For example, if you use the
    ``all-available-devices`` option when creating OSDs, when you ``zap`` a
    device the cephadm orchestrator automatically creates a new OSD in the
    device.  To disable this behavior, see :ref:`cephadm-osd-declarative`.


.. _osd_autotune:

Automatically tuning OSD memory
===============================

OSD daemons will adjust their memory consumption based on the
``osd_memory_target`` config option (several gigabytes, by
default).  If Ceph is deployed on dedicated nodes that are not sharing
memory with other services, cephadm can automatically adjust the per-OSD
memory consumption based on the total amount of RAM and the number of deployed
OSDs.

.. warning:: Cephadm sets ``osd_memory_target_autotune`` to ``true`` by default which is unsuitable for hyperconverged infrastructures.

Cephadm will start with a fraction
(``mgr/cephadm/autotune_memory_target_ratio``, which defaults to
``.7``) of the total RAM in the system, subtract off any memory
consumed by non-autotuned daemons (non-OSDs, for OSDs for which
``osd_memory_target_autotune`` is false), and then divide by the
remaining OSDs.

The final targets are reflected in the config database with options like::

  WHO   MASK      LEVEL   OPTION              VALUE
  osd   host:foo  basic   osd_memory_target   126092301926
  osd   host:bar  basic   osd_memory_target   6442450944

Both the limits and the current memory consumed by each daemon are visible from
the ``ceph orch ps`` output in the ``MEM LIMIT`` column::

  NAME        HOST  PORTS  STATUS         REFRESHED  AGE  MEM USED  MEM LIMIT  VERSION                IMAGE ID      CONTAINER ID  
  osd.1       dael         running (3h)     10s ago   3h    72857k     117.4G  17.0.0-3781-gafaed750  7015fda3cd67  9e183363d39c  
  osd.2       dael         running (81m)    10s ago  81m    63989k     117.4G  17.0.0-3781-gafaed750  7015fda3cd67  1f0cc479b051  
  osd.3       dael         running (62m)    10s ago  62m    64071k     117.4G  17.0.0-3781-gafaed750  7015fda3cd67  ac5537492f27  

To exclude an OSD from memory autotuning, disable the autotune option
for that OSD and also set a specific memory target.  For example,

.. prompt:: bash #

  ceph config set osd.123 osd_memory_target_autotune false
  ceph config set osd.123 osd_memory_target 16G


.. _drivegroups:

Advanced OSD Service Specifications
===================================

:ref:`orchestrator-cli-service-spec`\s of type ``osd`` are a way to describe a
cluster layout, using the properties of disks. Service specifications give the
user an abstract way to tell Ceph which disks should turn into OSDs with which
configurations, without knowing the specifics of device names and paths.

Service specifications make it possible to define a yaml or json file that can
be used to reduce the amount of manual work involved in creating OSDs.

.. note::
    It is recommended that advanced OSD specs include the ``service_id`` field
    set. The plain ``osd`` service with no service id is where OSDs created
    using ``ceph orch daemon add`` or ``ceph orch apply osd --all-available-devices``
    are placed. Not including a ``service_id`` in your OSD spec would mix
    the OSDs from your spec with those OSDs and potentially overwrite services
    specs created by cephadm to track them. Newer versions of cephadm will even
    block creation of advanced OSD specs without the service_id present

For example, instead of running the following command:

.. prompt:: bash [monitor.1]#

  ceph orch daemon add osd *<host>*:*<path-to-device>*

for each device and each host, we can define a yaml or json file that allows us
to describe the layout. Here's the most basic example.

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

#. Turn any available device (ceph-volume decides what 'available' is) into an
   OSD on all hosts that match the glob pattern '*'. (The glob pattern matches
   against the registered hosts from `host ls`) A more detailed section on
   host_pattern is available below.

#. Then pass it to `osd create` like this:

   .. prompt:: bash [monitor.1]#

     ceph orch apply -i /path/to/osd_spec.yml

   This instruction will be issued to all the matching hosts, and will deploy
   these OSDs.

   Setups more complex than the one specified by the ``all`` filter are
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
    Filters are applied using an `AND` gate by default. This means that a drive
    must fulfill all filter criteria in order to get selected. This behavior can
    be adjusted by setting ``filter_logic: OR`` in the OSD specification. 

Filters are used to assign disks to groups, using their attributes to group
them. 

The attributes are based off of ceph-volume's disk query. You can retrieve
information about the attributes with this command:

.. code-block:: bash

    ceph-volume inventory </path/to/disk>

Vendor or Model
^^^^^^^^^^^^^^^

Specific disks can be targeted by vendor or model:

.. code-block:: yaml

    model: disk_model_name

or

.. code-block:: yaml

    vendor: disk_vendor_name


Size
^^^^

Specific disks can be targeted by `Size`:

.. code-block:: yaml

    size: size_spec

Size specs
__________

Size specifications can be of the following forms:

* LOW:HIGH
* :HIGH
* LOW:
* EXACT

Concrete examples:

To include disks of an exact size

.. code-block:: yaml

    size: '10G'

To include disks within a given range of size: 

.. code-block:: yaml

    size: '10G:40G'

To include disks that are less than or equal to 10G in size:

.. code-block:: yaml

    size: ':10G'

To include disks equal to or greater than 40G in size:

.. code-block:: yaml

    size: '40G:'

Sizes don't have to be specified exclusively in Gigabytes(G).

Other units of size are supported: Megabyte(M), Gigabyte(G) and Terabyte(T).
Appending the (B) for byte is also supported: ``MB``, ``GB``, ``TB``.


Rotational
^^^^^^^^^^

This operates on the 'rotational' attribute of the disk.

.. code-block:: yaml

    rotational: 0 | 1

`1` to match all disks that are rotational

`0` to match all disks that are non-rotational (SSD, NVME etc)


All
^^^

This will take all disks that are 'available'

.. note:: This is exclusive for the data_devices section.

.. code-block:: yaml

    all: true


Limiter
^^^^^^^

If you have specified some valid filters but want to limit the number of disks that they match, use the ``limit`` directive:

.. code-block:: yaml

    limit: 2

For example, if you used `vendor` to match all disks that are from `VendorA`
but want to use only the first two, you could use `limit`:

.. code-block:: yaml

    data_devices:
      vendor: VendorA
      limit: 2

.. note:: `limit` is a last resort and shouldn't be used if it can be avoided.


Additional Options
------------------

There are multiple optional settings you can use to change the way OSDs are deployed.
You can add these options to the base level of an OSD spec for it to take effect.

This example would deploy all OSDs with encryption enabled.

.. code-block:: yaml

    service_type: osd
    service_id: example_osd_spec
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        all: true
      encrypted: true

See a full list in the DriveGroupSpecs

.. py:currentmodule:: ceph.deployment.drive_group

.. autoclass:: DriveGroupSpec
   :members:
   :exclude-members: from_json


Examples
========

The simple case
---------------

All nodes with the same setup

.. code-block:: none

    20 HDDs
    Vendor: VendorA
    Model: HDD-123-foo
    Size: 4TB

    2 SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

This is a common setup and can be described quite easily:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_default
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        model: HDD-123-foo # Note, HDD-123 would also be valid
      db_devices:
        model: MC-55-44-XZ # Same here, MC-55-44 is valid

However, we can improve it by reducing the filters on core properties of the drives:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_default
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        rotational: 1
      db_devices:
        rotational: 0

Now, we enforce all rotating devices to be declared as 'data devices' and all non-rotating devices will be used as shared_devices (wal, db)

If you know that drives with more than 2TB will always be the slower data devices, you can also filter by size:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_default
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        size: '2TB:'
      db_devices:
        size: ':2TB'

.. note:: All of the above OSD specs  are equally valid. Which of those you want to use depends on taste and on how much you expect your node layout to change.


Multiple OSD specs for a single host
------------------------------------

Here we have two distinct setups

.. code-block:: none

    20 HDDs
    Vendor: VendorA
    Model: HDD-123-foo
    Size: 4TB

    12 SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

    2 NVMEs
    Vendor: VendorC
    Model: NVME-QQQQ-987
    Size: 256GB


* 20 HDDs should share 2 SSDs
* 10 SSDs should share 2 NVMes

This can be described with two layouts.

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_hdd
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        rotational: 1
      db_devices:
        model: MC-55-44-XZ
        limit: 2 # db_slots is actually to be favoured here, but it's not implemented yet
    ---
    service_type: osd
    service_id: osd_spec_ssd
    placement:
      host_pattern: '*'
    spec:
      data_devices:
        model: MC-55-44-XZ
      db_devices:
        vendor: VendorC

This would create the desired layout by using all HDDs as data_devices with two SSD assigned as dedicated db/wal devices.
The remaining SSDs(10) will be data_devices that have the 'VendorC' NVMEs assigned as dedicated db/wal devices.

Multiple hosts with the same disk layout
----------------------------------------

Assuming the cluster has different kinds of hosts each with similar disk 
layout, it is recommended to apply different OSD specs matching only one
set of hosts. Typically you will have a spec for multiple hosts with the 
same layout. 

The service id as the unique key: In case a new OSD spec with an already
applied service id is applied, the existing OSD spec will be superseded.
cephadm will now create new OSD daemons based on the new spec
definition. Existing OSD daemons will not be affected. See :ref:`cephadm-osd-declarative`.

Node1-5

.. code-block:: none

    20 HDDs
    Vendor: VendorA
    Model: SSD-123-foo
    Size: 4TB
    2 SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

Node6-10

.. code-block:: none

    5 NVMEs
    Vendor: VendorA
    Model: SSD-123-foo
    Size: 4TB
    20 SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

You can use the 'placement' key in the layout to target certain nodes.

.. code-block:: yaml

    service_type: osd
    service_id: disk_layout_a
    placement:
      label: disk_layout_a
    spec:
      data_devices:
        rotational: 1
      db_devices:
        rotational: 0
    ---
    service_type: osd
    service_id: disk_layout_b
    placement:
      label: disk_layout_b
    spec:
      data_devices:
        model: MC-55-44-XZ
      db_devices:
        model: SSD-123-foo


This applies different OSD specs to different hosts depending on the `placement` key.
See :ref:`orchestrator-cli-placement-spec`

.. note::

    Assuming each host has a unique disk layout, each OSD 
    spec needs to have a different service id


Dedicated wal + db
------------------

All previous cases co-located the WALs with the DBs.
It's however possible to deploy the WAL on a dedicated device as well, if it makes sense.

.. code-block:: none

    20 HDDs
    Vendor: VendorA
    Model: SSD-123-foo
    Size: 4TB

    2 SSDs
    Vendor: VendorB
    Model: MC-55-44-ZX
    Size: 512GB

    2 NVMEs
    Vendor: VendorC
    Model: NVME-QQQQ-987
    Size: 256GB


The OSD spec for this case would look like the following (using the `model` filter):

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


It is also possible to specify directly device paths in specific hosts like the following:

.. code-block:: yaml

    service_type: osd
    service_id: osd_using_paths
    placement:
      hosts:
        - Node01
        - Node02
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


This can easily be done with other filters, like `size` or `vendor` as well.

It's possible to specify the `crush_device_class` parameter within the
DriveGroup spec, and it's applied to all the devices defined by the `paths`
keyword:

.. code-block:: yaml

    service_type: osd
    service_id: osd_using_paths
    placement:
      hosts:
        - Node01
        - Node02
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

The `crush_device_class` parameter, however, can be defined for each OSD passed
using the `paths` keyword with the following syntax:

.. code-block:: yaml

    service_type: osd
    service_id: osd_using_paths
    placement:
      hosts:
        - Node01
        - Node02
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

In case the OS of a host was reinstalled, existing OSDs need to be activated
again. For this use case, cephadm provides a wrapper for :ref:`ceph-volume-lvm-activate` that
activates all existing OSDs on a host.

.. prompt:: bash #

  ceph cephadm osd activate <host>...

This will scan all existing disks for OSDs and deploy corresponding daemons.

Further Reading
===============

* :ref:`ceph-volume`
* :ref:`rados-index`
