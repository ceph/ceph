.. _drivegroups:

=========================
OSD Service Specification
=========================

:ref:`orchestrator-cli-service-spec` of type ``osd`` are a way to describe a cluster layout using the properties of disks.
It gives the user an abstract way tell ceph which disks should turn into an OSD
with which configuration without knowing the specifics of device names and paths.

Instead of doing this::

  [monitor 1] # ceph orch daemon add osd *<host>*:*<path-to-device>*

for each device and each host, we can define a yaml|json file that allows us to describe
the layout. Here's the most basic example.

Create a file called i.e. osd_spec.yml

.. code-block:: yaml

    service_type: osd
    service_id: default_drive_group  <- name of the drive_group (name can be custom)
    placement:
      host_pattern: '*'              <- which hosts to target, currently only supports globs
    data_devices:                    <- the type of devices you are applying specs to
      all: true                      <- a filter, check below for a full list

This would translate to:

Turn any available(ceph-volume decides what 'available' is) into an OSD on all hosts that match
the glob pattern '*'. (The glob pattern matches against the registered hosts from `host ls`)
There will be a more detailed section on host_pattern down below.

and pass it to `osd create` like so::

  [monitor 1] # ceph orch apply osd -i /path/to/osd_spec.yml

This will go out on all the matching hosts and deploy these OSDs.

Since we want to have more complex setups, there are more filters than just the 'all' filter.


Filters
=======

.. note::
   Filters are applied using a `AND` gate by default. This essentially means that a drive needs to fulfill all filter
   criteria in order to get selected.
   If you wish to change this behavior you can adjust this behavior by setting

    `filter_logic: OR`  # valid arguments are `AND`, `OR`

   in the OSD Specification.

You can assign disks to certain groups by their attributes using filters.

The attributes are based off of ceph-volume's disk query. You can retrieve the information
with::

  ceph-volume inventory </path/to/disk>

Vendor or Model:
-------------------

You can target specific disks by their Vendor or by their Model

.. code-block:: yaml

    model: disk_model_name

or

.. code-block:: yaml

    vendor: disk_vendor_name


Size:
--------------

You can also match by disk `Size`.

.. code-block:: yaml

    size: size_spec

Size specs:
___________

Size specification of format can be of form:

* LOW:HIGH
* :HIGH
* LOW:
* EXACT

Concrete examples:

Includes disks of an exact size::

    size: '10G'

Includes disks which size is within the range::

    size: '10G:40G'

Includes disks less than or equal to 10G in size::

    size: ':10G'


Includes disks equal to or greater than 40G in size::

    size: '40G:'

Sizes don't have to be exclusively in Gigabyte(G).

Supported units are Megabyte(M), Gigabyte(G) and Terrabyte(T). Also appending the (B) for byte is supported. MB, GB, TB


Rotational:
-----------

This operates on the 'rotational' attribute of the disk.

.. code-block:: yaml

    rotational: 0 | 1

`1` to match all disks that are rotational

`0` to match all disks that are non-rotational (SSD, NVME etc)


All:
------------

This will take all disks that are 'available'

Note: This is exclusive for the data_devices section.

.. code-block:: yaml

    all: true


Limiter:
--------

When you specified valid filters but want to limit the amount of matching disks you can use the 'limit' directive.

.. code-block:: yaml

    limit: 2

For example, if you used `vendor` to match all disks that are from `VendorA` but only want to use the first two
you could use `limit`.

.. code-block:: yaml

  data_devices:
    vendor: VendorA
    limit: 2

Note: Be aware that `limit` is really just a last resort and shouldn't be used if it can be avoided.


Additional Options
===================

There are multiple optional settings you can use to change the way OSDs are deployed.
You can add these options to the base level of a DriveGroup for it to take effect.

This example would deploy all OSDs with encryption enabled.

.. code-block:: yaml

    service_type: osd
    service_id: example_osd_spec
    placement:
      host_pattern: '*'
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

All nodes with the same setup::

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
    data_devices:
      model: HDD-123-foo <- note that HDD-123 would also be valid
    db_devices:
      model: MC-55-44-XZ <- same here, MC-55-44 is valid

However, we can improve it by reducing the filters on core properties of the drives:

.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_default
    placement:
      host_pattern: '*'
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
    data_devices:
      size: '2TB:'
    db_devices:
      size: ':2TB'

Note: All of the above DriveGroups are equally valid. Which of those you want to use depends on taste and on how much you expect your node layout to change.


The advanced case
-----------------

Here we have two distinct setups::

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
    data_devices:
      rotational: 0
    db_devices:
      model: MC-55-44-XZ
      limit: 2 (db_slots is actually to be favoured here, but it's not implemented yet)
      
    service_type: osd
    service_id: osd_spec_ssd
    placement:
      host_pattern: '*'
    data_devices:
      model: MC-55-44-XZ
    db_devices:
      vendor: VendorC

This would create the desired layout by using all HDDs as data_devices with two SSD assigned as dedicated db/wal devices.
The remaining SSDs(8) will be data_devices that have the 'VendorC' NVMEs assigned as dedicated db/wal devices.

The advanced case (with non-uniform nodes)
------------------------------------------

The examples above assumed that all nodes have the same drives. That's however not always the case.

Node1-5::

    20 HDDs
    Vendor: Intel
    Model: SSD-123-foo
    Size: 4TB
    2 SSDs
    Vendor: VendorA
    Model: MC-55-44-ZX
    Size: 512GB

Node6-10::

    5 NVMEs
    Vendor: Intel
    Model: SSD-123-foo
    Size: 4TB
    20 SSDs
    Vendor: VendorA
    Model: MC-55-44-ZX
    Size: 512GB

You can use the 'host_pattern' key in the layout to target certain nodes. Salt target notation helps to keep things easy.


.. code-block:: yaml

    service_type: osd
    service_id: osd_spec_node_one_to_five
    placement:
      host_pattern: 'node[1-5]'
    data_devices:
      rotational: 1
    db_devices:
      rotational: 0
      
      
    service_type: osd
    service_id: osd_spec_six_to_ten
    placement:
      host_pattern: 'node[6-10]'
    data_devices:
      model: MC-55-44-XZ
    db_devices:
      model: SSD-123-foo

This applies different OSD specs to different hosts depending on the `host_pattern` key.

Dedicated wal + db
------------------

All previous cases co-located the WALs with the DBs.
It's however possible to deploy the WAL on a dedicated device as well, if it makes sense.

::

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
    data_devices:
      model: MC-55-44-XZ
    db_devices:
      model: SSD-123-foo
    wal_devices:
      model: NVME-QQQQ-987


This can easily be done with other filters, like `size` or `vendor` as well.

