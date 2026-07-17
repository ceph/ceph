=======================
 EXTBLKDEV, FCM plugin
=======================

.. index:: bluestore; extblkdev

ExtBlkDev
=========

With Pacific release Ceph was extended to handle thinly-provisioned and compressing drives.
The extension is made in form of loadable plugins that are part of Ceph codebase: ``src/extblkdev``.

At startup OSD checks if specific plugins are enabled, as controlled by:

.. confval:: osd_extblkdev_plugins

To let OSD load and check for presence of FCM devices set:

.. code-block::

  osd_extblkdev_plugins = fcm


Mode of operation
-----------------

Thin-provisioned and compressing drives have different capacity for data than what is advertised
as addressable logical space. It means that a compressing drive might advertise its size as 10TB,
but in reality only operate on 2TB of physical NAND.
ExtBlkDev plugins provide an additional information channel between devices and BlueStore,
giving Ceph report current disk logical / physical, used / available states.

This information is critical for OSDs: they might otherwise try to write more data to the device
than it can store, which will critically fail when it is really REALLY full.
Without current and accurate drive usage the configurables are useless:

.. confval:: mon_osd_full_ratio

.. confval:: mon_osd_nearfull_ratio

The ExtBlkDev extension is implemented on ``BlockDevice`` level, common for ``Main``, ``DB``, and ``WAL`` devices.
However, BlueStore only uses it in context of ``Main`` device.
For the ``DB`` and ``WAL`` devices, the plugin is detected and loaded, but BlueStore does not interact with it.

Plugin persistence
------------------

.. note::

  The examples below are derived from an actual deployment of the FCM plugin.

Having the proper plugin loaded is critical to OSD operation.

.. prompt:: # bash

  ceph osd df

.. code-block::

  ID  CLASS  WEIGHT    REWEIGHT  SIZE     RAW USE  DATA     OMAP     META    AVAIL    %USE   VAR   PGS   STATUS
   0    ssd  5.07739    1.00000   31 TiB  4.3 TiB  4.2 TiB   11 KiB  12 GiB   26 TiB  13.92  0.77   129      up
   1    ssd  5.07739    1.00000   31 TiB  4.3 TiB  4.2 TiB   12 KiB  12 GiB   26 TiB  13.93  0.77   128      up
   2    ssd  5.07739    1.00000   31 TiB  4.3 TiB  4.2 TiB   12 KiB  12 GiB   26 TiB  13.93  0.77   129      up
   3    ssd  5.07739    1.00000   31 TiB  4.2 TiB  4.2 TiB   12 KiB  12 GiB   26 TiB  13.90  0.77   128      up
   4    ssd  5.07739    1.00000  5.1 TiB  2.2 TiB  4.2 TiB   12 KiB  12 GiB  2.9 TiB  43.08  2.38   128      up
   5    ssd  5.07739    1.00000  5.1 TiB  2.2 TiB  4.3 TiB   12 KiB  12 GiB  2.9 TiB  43.25  2.39   128      up
   6    ssd  5.07739    1.00000  5.1 TiB  2.2 TiB  4.2 TiB   12 KiB  12 GiB  2.9 TiB  43.06  2.38   128      up
   7    ssd  5.07739    1.00000  5.1 TiB  2.2 TiB  4.3 TiB   14 KiB  12 GiB  2.9 TiB  43.44  2.40   129      up
                          TOTAL  143 TiB   26 TiB   34 TiB  101 KiB  94 GiB  117 TiB  18.09        1027        
  MIN/MAX VAR: 0.77/2.40  STDDEV: 18.00

In the above case OSDs 0-3 run without the plugin; OSDs 4-7 have the plugin loaded and active.
All eight OSDs participate in a single 6+2 erasure coded pool and therefore carry equivalent utilization.
With EC6+2 this is moot, but for other pool types the balancer will prioritize OSDs 0-3 which seem to be mostly empty
trying to offload seemingly overburdened OSDs 4-7. 

Tentacle provides a new configurable that signals BlueStore to expect that an ExtBlkDev plugin will be in use.

.. confval:: bluestore_use_ebd

The ExtBlkDev infrastructure adds new category of health warnings:

.. code-block::

  HEALTH_WARN 8 OSD(s) reporting problems with ExtBlkDev plugin;
  [WRN] EXTBLKDEV: 8 OSD(s) reporting problems with ExtBlkDev plugin


With the ``bluestore_use_ebd`` set to ``true``, two EXTBLKDEV health warnings may appear:

  1) "plugin 'fcm' not loaded"

  2) "plugin 'fcm' used on mkfs, but now uses plugin 'disabled'"


Having the configuration option set to ``true`` does not require that a plugin is used. 
If a plugin is not found during OSD creation, this is still a proper and acceptable state.
However, once a plugin is detected during the OSD's creation, its usage becomes mandatory.
Should plugin presence be required for specific OSD, one can check for it:

.. prompt:: bash #

  ceph osd metadata osd.2

.. code-block::

  ...
  "bluestore_bdev_fcm": "true",
  ...

or

.. prompt:: bash #

  ceph-bluestore-tool --path dev/osd4/ show-label

.. code-block::

  ...
  "extblkdev": "fcm",
  ...

The first check is somewhat weaker than the second.
In a specific error case that the plugin was detected and memorized at OSD creation,
but is not loaded, one will not see the value in OSD metadata. Instead a health warning appears.
The second check never fails. It simply stores the setting in BlueStore label metadata.

.. _fix-use-ebd:

A fix
-----

If ``bluestore_use_ebd=true`` was not set when OSDs were created on devices that should run with a plugin,
it may be set after the fact with a command of the following form:

.. prompt:: bash #

  ceph-bluestore-tool --dev dev/osd4/block -k extblkdev -v fcm set-label-key

.. _fcm-plugin:

FCM plugin
==========

Ceph releases beginning with Tentacle provide a plugin to operate with FlashCoreModule devices (FCMs).
Like all ExtBlkDev plugins, it reports true disk usage to BlueStore, effectively overriding
BlueStore reported metrics of used and available device space.
FCM reported metrics are in physical NAND size, not in logical space size.

In addition the plugin reports fixed stats via OSD metadata:

.. prompt:: bash #

  ceph osd metadata osd.2

.. code-block::

  ...
  "bluestore_bdev_fcm_device_logical_size": "33599931809792",
  "bluestore_bdev_fcm_device_physical_size": "5582606303232",
  "bluestore_bdev_fcm_partition_logical_size": "33599931809792",
  "bluestore_bdev_fcm_partition_physical_size": "5582606303232",
  ...

And dynamic current state via performance counters:

.. prompt:: bash #

  ceph tell osd.6 perf dump extblkdev

.. code-block::

  {
    "extblkdev": {
        "fcm": 0,  <- value is irrelevant, it is the name that counts
        "dev_phy_size": 5582606303232,
        "dev_log_size": 33599931809792,
        "dev_phy_util": 2403758935846,
        "dev_log_util": 4714543366144,
        "part_phy_size": 5582606303232,
        "part_log_size": 33599931809792,
        "part_phy_avail": 3178847367386,
        "part_log_avail": 28885388443648
    }
  }

The FCM plugin provides three new health warnings in the EXTBLKDEV class.

  1) "failed accessing FCM utilization log" :ref:`fcm-permissions`

  2) "bdev_enable_discard not enabled - free space will leak" :ref:`fcm-discard`

  3) "multivolume fcm will not work properly" :ref:`fcm-multivolume`


.. _fcm-permissions:

FCM permissions
---------------

The FCM plugin talks to the NVMe controller within each storage device.
In Linux, this is a privileged operation, tied to the `cap_sys_admin` privilege.
To get the privilege do either:

  a) Run Ceph OSD as root.

  b) Grant ``setcap "cap_sys_admin=p" ceph-osd``
     and run with ``ceph-osd --set-keepcaps=true`` .

.. _fcm-discard:

FCM discard
-----------

FCM devices implement compression; their physical NAND capacity is much smaller than their logical device size.
Conventional SSDs storing data that is no longer used by BlueStore experience may experience suboptimal performance.
With FCMs, orphaned data represents a direct loss of available capacity. To prevent this problem, one should always set:

.. code-block::

  bdev_enable_discard = true

.. _fcm-multivolume:

FCM multivolume
---------------

BlueStore works well when there is a one-to-one mapping between FCM devices to BlueStore block devices.

Splitting an FCM device in half by partitioning does not create any boundary for the controller.
Filling one partition will affect available space in the other partition.
Two partitions together will have the total logical capacity exactly the same, but available physical NAND will be counted twice.
This will most likely confuse both balancers and operators.

  NOTE: The FCM plugin does not detect this condition.

Combining multiple FCM devices using the device mapper, LVM, or Linux MD software RAID is not recommended. When doing so,
BlueStore has no visibility of the topology of the backing block devices and cannot know to address storage by specific logical device offsets.
For regular drives it does not matter; when the physical:logical ratio is 1:1 BlueStore will eventually run out of logical space on first drive
and without even noticing start consuming the second drive.
With FCMs, and any other compressing drives, it is no longer the case. When BlueStore is not running out of logical space on first drive,
it will keep asking the drive to accomodate more data. The second drive has it, but first does not.
BlueStore will eventually fail to allocate space and fail with a hard ``-ENOSPC`` error.

Trying to run BlueStore with compound block device when one of them is an FCM will raise the warning.
An OSD cannot operate long-term in this way. It must be redeployed.



Redeploy and discard
--------------------

When one redeploys an FCM OSD without first formatting or issuing a whole-device discard operation,
BlueStore will be logically empty, but the underlying device will still hold the burden of previously held data.

.. code-block::

  ID  CLASS  WEIGHT   REWEIGHT  SIZE     RAW USE  DATA     OMAP     META     AVAIL    %USE   VAR   PGS  STATUS
   0    ssd  5.07739   1.00000  5.1 TiB  2.2 TiB  956 KiB   24 KiB   26 MiB  2.9 TiB  43.16  1.00    1      up
   1    ssd  5.07739   1.00000  5.1 TiB  2.2 TiB  376 KiB   20 KiB   26 MiB  2.9 TiB  43.14  1.00    0      up
   2    ssd  5.07739   1.00000  5.1 TiB  2.2 TiB  956 KiB   20 KiB   26 MiB  2.9 TiB  43.15  1.00    1      up
   3    ssd  5.07739   1.00000  5.1 TiB  2.2 TiB  376 KiB   16 KiB   26 MiB  2.9 TiB  43.05  1.00    0      up
   4    ssd  5.07739   1.00000  5.1 TiB  2.2 TiB  376 KiB   14 KiB   26 MiB  2.9 TiB  43.08  1.00    0      up
   5    ssd  5.07739   1.00000  5.1 TiB  2.2 TiB  376 KiB   11 KiB   26 MiB  2.9 TiB  43.24  1.00    0      up
   6    ssd  5.07739   1.00000  5.1 TiB  2.2 TiB  376 KiB    7 KiB   26 MiB  2.9 TiB  43.05  1.00    0      up
   7    ssd  5.07739   1.00000  5.1 TiB  2.2 TiB  956 KiB    6 KiB   26 MiB  2.9 TiB  43.44  1.01    1      up
                         TOTAL   41 TiB   18 TiB  4.6 MiB  121 KiB  210 MiB   23 TiB  43.17          3        
  MIN/MAX VAR: 1.00/1.01  STDDEV: 0.12

Solution:

  a) ``nvme format`` before deploying
  b) Set ``bluestore_discard_on_mkfs=true`` (Umbrella+).


CRUSH weight
------------

This section describes :ref:`fix-use-ebd` when using the FCM plugin :ref:`fcm-plugin`.

When an OSD is properly deployed on an FCM, the WEIGHT and SIZE values reflect the physical NAND capacity:

.. code-block::

  ID  CLASS  WEIGHT   REWEIGHT  SIZE     RAW USE  DATA     OMAP     META     AVAIL    %USE  VAR   PGS  STATUS
   0    ssd  5.07739   1.00000  5.1 TiB   12 MiB  944 KiB   21 KiB   26 MiB  5.1 TiB     0  1.00    1      up

When an OSD is deployed without the active FCM plugin, the WEIGHT and SIZE reflect logical advertised values:

.. code-block::

  ID  CLASS  WEIGHT    REWEIGHT  SIZE     RAW USE  DATA     OMAP     META     AVAIL    %USE  VAR   PGS  STATUS
   0    ssd  30.55899   1.00000   31 TiB   27 MiB  908 KiB   16 KiB   26 MiB   31 TiB     0  1.01    1      up

When a plugin is active for the underlying device, the result is that the SIZE
is the proper physical NAND capacity, but the CRUSH weight is still locked to original value at deployment:

.. code-block::

  ID  CLASS  WEIGHT    REWEIGHT  SIZE     RAW USE  DATA     OMAP    META     AVAIL    %USE  VAR   PGS  STATUS
   0    ssd  30.55899   1.00000  5.1 TiB   28 KiB  1.0 MiB   7 KiB   27 MiB  5.1 TiB     0  1.42    1      up

The CRUSH weight of such OSDs must be adjusted.
