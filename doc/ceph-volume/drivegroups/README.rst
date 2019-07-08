.. _ceph-volume-drivegroups:

``Concept``
============

Creating OSDs in ceph is a mostly manual process. DriveGroups are aiming to change that.

Instead of targeting drives individually we try to describe them by using attributes.

.. note:: Valid attributes are `vendor`, `model`, `size`, `rotational`
          (Attributes are compared to what ceph-volume inventory --format json-pretty reports)



``Example``
===========

Imagine a node with 10HDDs and 2SSDs.

HDD:

- Vendor: Example_vendor_A
- Model: Slow_rusty_spinner_123
- Size: 2TB

SSD:

- Vendor: Example_vendor_B
- Model: Fast_thing_987
- Size: 512GB


We'd like to have the two SSDs as dedicated DB/WAL devices.

In a traditional deployment procedure we'd find the device names of these drives and pass them
to ceph-volume (given we follow the manual deployment process)::

:: ref to ceph-volume docs

With DriveGroups we just create one yaml file::


    touch example_drive_groups.yaml


Now we add try to describe these drives using their attributes.

The easiest way to differentiate the drives is by their ``rotational`` flag.
Hence our DriveGroups would look as simple as:


.. code-block:: yaml

        default:
            data_devices:
              rotational: 0
            db_devices:
              rotational: 1


If for whatever reason we can't rely on the rotational flag, we may just use any other property.


.. code-block:: yaml

        default:
            data_devices:
              size: 2TB # high:low notation is also implemented (:N, N:, MIN:MAX)
            db_devices:
              size: 512MB # MB or M is fine

We can also mix attributes:


.. code-block:: yaml

        default:
            data_devices:
              model: Slow_rusty_spinner_123  # model and vendor are implemented as a substring matcher
            db_devices:
              vendor: Example_vendor_B


We can also define properties like ``block_wal_size`` or ``encryption`` by adding it to the DriveGroup file.


.. code-block:: yaml

        default:
            data_devices:
              model: Slow_rusty_spinner_123  # model and vendor are implemented as a substring matcher
            db_devices:
              vendor: Example_vendor_B
            encryption: True



``show``
============

How that we described our setup, we can pass this to ceph-volume and validate it::


    ceph-volume drivegroups show /path/to/example_drive_groups.yaml


Internally this re-uses the reporting functionality of ceph-volume. So we'd get back something like this (my example output is with 5xHDD 2xPseudo SSDs)

::


    data1:/ceph-volume drivegroups show /path/to/example_drive_groups.yaml

    --> Processing DriveGroup <default>
    --> Initializing filter <size> with value <20G>
    --> Initializing filter <size> with value <10G>

    Total OSDs: 5


    Solid State VG:
      Targets:   block.db                  Total size: 18.00 GB
      Total LVs: 5                         Size per LV: 3.60 GB
      Devices:   /dev/vdg, /dev/vdh

      Type            Path                                                    LV Size         % of device
    ----------------------------------------------------------------------------------------------------
      [data]          /dev/vdb                                                19.00 GB        100.0%
      [block.db]      vg: vg/lv                                               3.60 GB         20%
    ----------------------------------------------------------------------------------------------------
      [data]          /dev/vdc                                                19.00 GB        100.0%
      [block.db]      vg: vg/lv                                               3.60 GB         20%
    ----------------------------------------------------------------------------------------------------
      [data]          /dev/vdd                                                19.00 GB        100.0%
      [block.db]      vg: vg/lv                                               3.60 GB         20%
    ----------------------------------------------------------------------------------------------------
      [data]          /dev/vde                                                19.00 GB        100.0%
      [block.db]      vg: vg/lv                                               3.60 GB         20%
    ----------------------------------------------------------------------------------------------------
      [data]          /dev/vdf                                                19.00 GB        100.0%
      [block.db]      vg: vg/lv                                               3.60 GB         20%



``apply``
============

If we're satisfied with the results we can continue with the deployment process by ``applying`` the DriveGroups.::

    ceph-volume drivegroups apply /path/to/example_drive_groups.yaml


We get prompted with the summary above again, but for automated deployments there is a ``-n`` (non-interactive) switch.

From there on ceph-volume (specifically Batch()) takes care of the deployment.
