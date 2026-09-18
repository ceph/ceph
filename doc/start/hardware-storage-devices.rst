.. _hardware-storage-devices:

=================
 Storage Devices
=================

.. meta::
   :description: Drive layout, HDD and SSD selection, controllers, write caches, and benchmarking for Ceph OSD, Monitor, and metadata storage.
   :ceph-page-type: reference
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: docs

This page lists the storage device recommendations for a Ceph cluster: which
drives to use for which data, how to size and connect them, and how to test
them. Figures describe a single daemon; a host needs the sum of the needs of
the daemons it runs. For the principles behind these recommendations, see
:ref:`hardware-recommendations`.

Drive Layout
============

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Rule
     - Detail
   * - One OSD per drive
     - Provision a single :term:`OSD` on any media, other than perhaps SSDs
       larger than 30 TB. Do not host multiple OSDs on a single SAS or SATA
       HDD.
   * - A dedicated OS drive
     - Use a dedicated, ideally mirrored, drive for the operating system.
       Routine OS operations and simultaneous requests from multiple daemons
       against a single drive can impact performance and stability. Many
       "slow OSD" issues that are not hardware failures arise from running
       the OS and multiple OSDs on the same drive.
   * - A separate WAL+DB drive for HDD OSDs
     - Put the :term:`BlueStore` WAL+DB (write-ahead log and metadata
       database) of HDD OSDs on a separate drive. HDD OSDs may see a
       significant write latency improvement by offloading WAL+DB onto an
       SSD. See :ref:`block and block.db <bluestore-mixed-device-config>`
       in the BlueStore Configuration Reference.
   * - No OSD on a Monitor, Manager, or MDS drive
     - Do not colocate an OSD with Monitor, Manager, or :term:`MDS` data on
       the same drive.
   * - Minimum OSD size 1 TiB
     - OSD drives much smaller than 1 TiB use a significant fraction of
       their capacity for metadata. Drives smaller than 100 GiB are not
       effective at all.
   * - SSDs for Monitors, Managers, and metadata
     - Provision enterprise-class SSDs, at a minimum, for hosts that run or
       may run Monitor and Manager daemons. Monitor databases, CephFS
       metadata pools, and RGW index and log pools require SSDs for
       acceptable performance and stability at enterprise scale, even when
       HDDs hold bulk OSD data. RGW deployments that use HDDs for bulk
       bucket data should provision all other pools on SSDs. To keep the
       CephFS ``metadata`` pool on SSDs, give it a :term:`CRUSH` rule that
       selects only SSD media; see
       :ref:`CRUSH Device Class <crush-map-device-class>`.
   * - Many OSDs per host
     - Most hosts run many OSDs, so weigh each host's share of the cluster's
       capacity against the full ratio (see :ref:`Failure Domains
       <hardware-recommendations>`) and the aggregate throughput of its
       drives against its links (see :ref:`Network Sizing
       <hardware-networks>`).

HDD Compared with SSD
=====================

.. list-table::
   :header-rows: 1
   :widths: 18 41 41

   * - Factor
     - HDD
     - SSD
   * - Access time and IOPS (I/O operations per second)
     - Limited by seek time, access time, read and write times, IOPS, and
       total throughput. These limits affect overall performance, especially
       during recovery. Larger HDDs have lower IOPS per TB.
     - Access times at least 100 times faster than HDDs, lower latency, and
       higher throughput. No rotational or seek latency. Avoids hotspot and
       bottleneck issues in busy clusters. SSDs have limitations of their
       own: evaluate sequential and random reads and writes.
   * - Interface
     - The interface becomes a bottleneck at larger capacities. A 32 TB HDD
       has ten times the data of a 3 TB HDD behind the same SATA interface.
       HDDs above 8 TB suit large, performance-insensitive data.
     - NVMe SSDs need no HBA.
   * - Recovery
     - Very slow recovery can result in a lengthy period of increased risk
       after a component fails.
     - Substantially faster rebalancing, with less client impact, when OSDs
       or Monitors are added, removed, or fail.
   * - Cost
     - Lower price per terabyte. Chassis management overhead and data center
       space are key inputs into TCO (total cost of ownership). Large
       deployments often achieve lower TCO with SSDs, especially when the
       cost and management of RAID HBAs for HDDs are avoided.
     - Higher price per terabyte, but the amortized drive cost for a given
       number of IOPS is much lower. QLC (quad-level cell flash, the densest
       and cheapest) SSDs are closing the gap for object storage, with
       greater density, lower power consumption, and less power spent on
       cooling.
   * - Suitability
     - Bulk OSD data. Offload WAL+DB onto an SSD.
     - The metadata pools listed under Drive Layout, and any pool where
       performance matters.

Chassis and interface considerations for HDDs:

- Consider not only the interface of a single drive but the system as a
  whole. Chassis with SAS or SATA ports connect drives through a backplane,
  and chassis that house more than 8 such drives add expanders, once
  separate AIC (add-in card) boards with cables and now embedded in the
  backplane. In dense chassis, where 24, 36, or even 100 drives contend for
  these shared paths, backplanes and expanders are shared bottlenecks. NVMe
  drives avoid them.
- A chassis built for LFF (3.5") drives is space-inefficient when SFF
  (2.5") drives are fitted through adapters.
- To weigh the factors that determine the total cost of storage, use the
  `Storage Networking Industry Association's Total Cost of Ownership
  calculator`_.

SSD Selection
=============

.. important:: Before making a significant investment in SSDs, review the
   performance metrics of the SSD and test it in a test configuration.
   Acceptable IOPS is not the only factor to consider.

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Criterion
     - Recommendation
   * - Class
     - Enterprise-class SSDs. They feature power loss protection (PLP,
       capacitors that let the drive finish in-flight writes when power is
       lost) and do not suffer the dramatic "cliffing" of client (desktop)
       models, where sustained performance declines considerably once a
       limited cache fills. Client SKUs are intended for much lighter and
       intermittent duty cycles. Bargain client-class or off-brand SSDs are
       a false economy.
   * - Endurance for OSDs
     - Most OSD deployments do not require more than 1 DWPD (Drive Writes
       Per Day, also called "read-optimized"). "Mixed-use" SSDs in the
       3 DWPD class are often overkill and cost significantly more. A drive
       rated for 0.3 DWPD may be fine for OSDs dedicated to
       sequentially-written, read-mostly data, but is not a good choice for
       an RBD pool serving hundreds of VMs.
   * - Boot plus Monitor or Manager SSD
     - For a single SSD, or mirrored pair, that holds both the OS and
       Monitor or Manager data: 256 GB minimum, and at least 960 GB
       recommended. Choose a model rated at 1+ DWPD or the equivalent in
       TBW (TeraBytes Written). A larger SSD than technically required
       provides more endurance, because it effectively has greater
       overprovisioning.
   * - Partition alignment
     - Align partitions, if any, properly. Improperly aligned partitions
       reduce performance and endurance. For details and example commands,
       see `Werner Fischer's blog post on partition alignment`_.

Controllers
===========

Disk controllers (HBAs) can have a significant impact on write throughput.
Select HBAs that do not create a performance bottleneck.

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Controller
     - Guidance
   * - RAID HBA (IR mode, RAID-on-Chip)
     - May exhibit higher latency than a plain HBA. A RAID HBA with cache
       and battery adds purchase cost and yearly support cost. Software
       mirroring (Linux MD or ZFS) is enough for boot volumes. Many RAID
       HBAs can be configured with an IT-mode "personality" or "JBOD mode"
       for streamlined operation.
   * - Plain HBA (IT or JBOD mode)
     - Lower latency and lower cost. Forgoing HBA RAID with SAS or SATA
       drives narrows the HDD versus SSD cost gap.
   * - NVMe SSDs
     - Need no HBA at all. This further narrows the HDD versus SSD cost gap
       when the system as a whole is considered.

Benchmarking
============

BlueStore opens storage devices with ``O_DIRECT`` (which bypasses the page
cache) and issues ``fsync()`` (which forces writes to media) frequently to
ensure that data is safely persisted. Evaluate a drive's low-level write
performance with ``fio`` (the fio benchmarking tool). This example measures
4 KiB random write performance:

.. warning:: This test overwrites the device. Run it only on a drive that
   holds no data.

.. code-block:: console

   # fio --name=osd-test --filename=/dev/sdX --ioengine=libaio --direct=1 --fsync=1 --rw=randwrite --bs=4k --runtime=300

Compare the reported IOPS and completion latency between candidate drives
and between write cache settings.

Write Caches
============

Enterprise storage drives include power loss protection, which ensures data
durability when power is lost during operation, and multi-level caches that
speed up direct or synchronous writes. These drives have two caching modes:

- Volatile write cache enabled (Linux mode "write back"): the volatile cache
  is flushed to persistent media with ``fsync``.
- Volatile write cache disabled (Linux mode "write through"): the
  non-volatile cache is written synchronously.

The default for HDDs, usually caching enabled, may not be optimal.
Disabling the write cache may dramatically increase OSD IOPS and decrease
commit latency. Benchmark each device with ``fio`` as described above, then
persist the optimal cache configuration.

Query and change the cache setting, which ``sdparm`` calls WCE (write cache
enable), with any of these tools:

.. list-table::
   :header-rows: 1
   :widths: 16 42 42

   * - Tool
     - Query
     - Disable
   * - ``hdparm``
     - ``hdparm -W /dev/sda`` reports ``write-caching = 1 (on)``
     - ``hdparm -W0 /dev/sda``
   * - ``sdparm``
     - ``sdparm --get WCE /dev/sda`` reports ``WCE 1``
     - ``sdparm --clear WCE /dev/sda``
   * - ``smartctl``
     - ``smartctl -g wcache /dev/sda`` reports ``Write cache is: Enabled``
     - ``smartctl -s wcache,off /dev/sda``
   * - sysfs
     - ``cat /sys/class/scsi_disk/*/cache_type`` (for example
       ``/sys/class/scsi_disk/0:0:0:0/cache_type``) reports ``write back``
     - ``echo "write through" > /sys/class/scsi_disk/0:0:0:0/cache_type``

Notes:

- In most cases, disabling the cache with ``hdparm``, ``sdparm``, or
  ``smartctl`` changes the sysfs ``cache_type`` to ``write through``
  automatically. If not, set ``cache_type`` directly as shown in the table.
- ``sdparm`` can view or change the volatile write cache on several
  devices at once, for example ``sdparm --get WCE /dev/sd*`` and
  ``sdparm --clear WCE /dev/sd*``.
- This udev rule, for systemd-based distributions, sets all SATA and SAS
  devices to ``write through``:

  .. code-block:: console

     # cat /etc/udev/rules.d/99-ceph-write-through.rules
     ACTION=="add", SUBSYSTEM=="scsi_disk", ATTR{cache_type}:="write through"

.. note:: Confirm that the setting persists across reboots. Some drives
   require it to be set again at every boot, which the udev rule above
   handles.

Additional Resources
====================

- :ref:`hardware-recommendations`
- :ref:`minimum-hardware`
- :ref:`CPU and Memory Sizing <hardware-cpu-memory>`
- :ref:`Network Sizing <hardware-networks>`
- :ref:`block and block.db <bluestore-mixed-device-config>`

.. _Storage Networking Industry Association's Total Cost of Ownership calculator: https://www.snia.org/forums/cmsi/programs/TCOcalc
.. _Werner Fischer's blog post on partition alignment: https://www.thomas-krenn.com/en/wiki/Partition_Alignment_detailed_explanation
