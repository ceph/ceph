========================
 mClock Config Reference
========================

.. index:: mclock; configuration

Mclock profiles mask the low level details from users, making it
easier for them to configure mclock.  

To use mclock, you must provide the following input parameters:

* total capacity of each OSD

* an mclock profile to enable

Using the settings in the specified profile, the OSD determines and applies the
lower-level mclock and Ceph parameters. The parameters applied by the mclock
profile make it possible to tune the QoS between client I/O, recovery/backfill
operations, and other background operations (for example, scrub, snap trim, and
PG deletion). These background activities are considered best-effort internal
clients of Ceph. 


.. index:: mclock; profile definition

mClock Profiles - Definition and Purpose
========================================

A mclock profile is *“a configuration setting that when applied on a running
Ceph cluster enables the throttling of the operations(IOPS) belonging to
different client classes (background recovery, scrub, snaptrim, client op,
osd subop)”*.

The mclock profile uses the capacity limits and the mclock profile selected by
the user to determine the low-level mclock resource control parameters.

Depending on the profile, lower-level mclock resource-control parameters and
some Ceph-configuration parameters are transparently applied.

The low-level mclock resource control parameters are the *reservation*,
*limit*, and *weight* that provide control of the resource shares, as
described in the `OSD Config Reference`_.


.. index:: mclock; profile types

mClock Profile Types
====================

mclock profiles can be broadly classified into two types,

- **Built-in**: Users can choose between the following built-in profile types:

  - **high_client_ops** (*default*):
    This profile allocates more reservation and limit to external-client ops
    as compared to background recoveries and other internal clients within
    Ceph. This profile is enabled by default.
  - **high_recovery_ops**:
    This profile allocates more reservation to background recoveries as 
    compared to external clients and other internal clients within Ceph. For
    example, an admin may enable this profile temporarily to speed-up background
    recoveries during non-peak hours.
  - **balanced**:
    This profile allocates equal reservation to client ops and background
    recovery ops.

- **Custom**: This profile gives users complete control over all mclock and
  Ceph configuration parameters. Using this profile is not recommended without
  a deep understanding of mclock and related Ceph-configuration options.

.. note:: Across the built-in profiles, internal clients of mclock (for example
          "scrub", "snap trim", and "pg deletion") are given slightly lower 
          reservations, but higher weight and no limit. This ensures that 
          these operations are able to complete quickly if there are no other 
          competing services.


.. index:: mclock; built-in profiles

mClock Built-in Profiles
========================

When a built-in profile is enabled, the mClock scheduler calculates the low
level mclock parameters [*reservation*, *weight*, *limit*] based on the profile
enabled for each client type. The mclock parameters are calculated based on
the max OSD capacity provided beforehand. As a result, the following mclock
config parameters cannot be modified when using any of the built-in profiles:

- ``osd_mclock_scheduler_client_res``
- ``osd_mclock_scheduler_client_wgt``
- ``osd_mclock_scheduler_client_lim``
- ``osd_mclock_scheduler_background_recovery_res``
- ``osd_mclock_scheduler_background_recovery_wgt``
- ``osd_mclock_scheduler_background_recovery_lim``
- ``osd_mclock_scheduler_background_best_effort_res``
- ``osd_mclock_scheduler_background_best_effort_wgt``
- ``osd_mclock_scheduler_background_best_effort_lim``

The following Ceph options will not be modifiable by the user:

- ``osd_max_backfills``
- ``osd_recovery_max_active``

This is because the above options are internally modified by the mclock
scheduler in order to maximize the impact of the set profile.

By default, the *high_client_ops* profile is enabled to ensure that a larger
chunk of the bandwidth allocation goes to client ops. Background recovery ops
are given lower allocation (and therefore take a longer time to complete). But
there might be instances that necessitate giving higher allocations to either
client ops or recovery ops. In order to deal with such a situation, you can
enable one of the alternate built-in profiles mentioned above.

If a built-in profile is active, the following Ceph config sleep options will
be disabled,

- ``osd_recovery_sleep``
- ``osd_recovery_sleep_hdd``
- ``osd_recovery_sleep_ssd``
- ``osd_recovery_sleep_hybrid``
- ``osd_scrub_sleep``
- ``osd_delete_sleep``
- ``osd_delete_sleep_hdd``
- ``osd_delete_sleep_ssd``
- ``osd_delete_sleep_hybrid``
- ``osd_snap_trim_sleep``
- ``osd_snap_trim_sleep_hdd``
- ``osd_snap_trim_sleep_ssd``
- ``osd_snap_trim_sleep_hybrid``

The above sleep options are disabled to ensure that mclock scheduler is able to
determine when to pick the next op from its operation queue and transfer it to
the operation sequencer. This results in the desired QoS being provided across
all its clients.


.. index:: mclock; enable built-in profile

Steps to Enable mClock Profile
==============================

The following sections outline the steps required to enable a mclock profile.

Determining OSD Capacity Using Benchmark Tests
----------------------------------------------

To allow mclock to fulfill its QoS goals across its clients, it is most
important to have a good understanding of each OSD's capacity in terms of its
baseline throughputs (IOPS) across the Ceph nodes. To determine this capacity,
you must perform appropriate benchmarking tests. The steps for performing these
benchmarking tests are broadly outlined below.

Any existing benchmarking tool can be used for this purpose. The following
steps use the *Ceph Benchmarking Tool* (cbt_). Regardless of the tool
used, the steps described below remain the same.

As already described in the `OSD Config Reference`_ section, the number of
shards and the bluestore's throttle parameters have an impact on the mclock op
queues. Therefore, it is critical to set these values carefully in order to
maximize the impact of the mclock scheduler.

:Number of Operational Shards:
  We recommend using the default number of shards as defined by the
  configuration options ``osd_op_num_shards``, ``osd_op_num_shards_hdd``, and
  ``osd_op_num_shards_ssd``. In general, a lower number of shards will increase
  the impact of the mclock queues.

:Bluestore Throttle Parameters:
  We recommend using the default values as defined by
  ``bluestore_throttle_bytes`` and ``bluestore_throttle_deferred_bytes``. But
  these parameters may also be determined during the benchmarking phase as
  described below.

Benchmarking Test Steps Using CBT
`````````````````````````````````

The steps below use the default shards and detail the steps used to determine the
correct bluestore throttle values.

.. note:: These steps, although manual in April 2021, will be automated in the future.

1. On the Ceph node hosting the OSDs, download cbt_ from git.
2. Install cbt and all the dependencies mentioned on the cbt github page.
3. Construct the Ceph configuration file and the cbt yaml file.
4. Ensure that the bluestore throttle options ( i.e.
   ``bluestore_throttle_bytes`` and ``bluestore_throttle_deferred_bytes``) are
   set to the default values.
5. Ensure that the test is performed on similar device types to get reliable
   OSD capacity data.
6. The OSDs can be grouped together with the desired replication factor for the
   test to ensure reliability of OSD capacity data.
7. After ensuring that the OSDs nodes are in the desired configuration, run a
   simple 4KiB random write workload on the OSD(s) for 300 secs.
8. Note the overall throughput(IOPS) obtained from the cbt output file. This
   value is the baseline throughput(IOPS) when the default bluestore
   throttle options are in effect.
9. If the intent is to determine the bluestore throttle values for your
   environment, then set the two options, ``bluestore_throttle_bytes`` and
   ``bluestore_throttle_deferred_bytes`` to 32 KiB(32768 Bytes) each to begin
   with. Otherwise, you may skip to the next section.
10. Run the 4KiB random write workload as before on the OSD(s) for 300 secs.
11. Note the overall throughput from the cbt log files and compare the value
    against the baseline throughput in step 8.
12. If the throughput doesn't match with the baseline, increment the bluestore
    throttle options by 2x and repeat steps 9 through 11 until the obtained
    throughput is very close to the baseline value.

For example, during benchmarking on a machine with NVMe SSDs, a value of 256 KiB for
both bluestore throttle and deferred bytes was determined to maximize the impact
of mclock. For HDDs, the corresponding value was 40 MiB, where the overall
throughput was roughly equal to the baseline throughput. Note that in general
for HDDs, the bluestore throttle values are expected to be higher when compared
to SSDs.

.. _cbt: https://github.com/ceph/cbt


Specifying  Max OSD Capacity
----------------------------

The steps in this section may be performed only if the max osd capacity is
different from the default values (SSDs: 21500 IOPS and HDDs: 315 IOPS). The
option ``osd_mclock_max_capacity_iops_[hdd, ssd]`` can be set by specifying it
in either the **[global]** section or in a specific OSD section (**[osd.x]** of
your Ceph configuration file).

Alternatively, commands of the following form may be used:

  .. prompt:: bash #

     ceph config set [global, osd] osd_mclock_max_capacity_iops_[hdd,ssd] <value>

For example, the following command sets the max capacity for all the OSDs in a
Ceph node whose underlying device type is SSDs:

  .. prompt:: bash #

    ceph config set osd osd_mclock_max_capacity_iops_ssd 25000

To set the capacity for a specific OSD (for example "osd.0") whose underlying
device type is HDD, use a command like this:

  .. prompt:: bash #

    ceph config set osd.0 osd_mclock_max_capacity_iops_hdd 350


Specifying Which mClock Profile to Enable
-----------------------------------------

As already mentioned, the default mclock profile is set to *high_client_ops*.
The other values for the built-in profiles include *balanced* and
*high_recovery_ops*.

If there is a requirement to change the default profile, then the option
``osd_mclock_profile`` may be set in the **[global]** or **[osd]** section of
your Ceph configuration file before bringing up your cluster.

Alternatively, to change the profile during runtime, use the following command:

  .. prompt:: bash #

    ceph config set [global,osd] osd_mclock_profile <value>

For example, to change the profile to allow faster recoveries, the following
command can be used to switch to the *high_recovery_ops* profile:

  .. prompt:: bash #

    ceph config set osd osd_mclock_profile high_recovery_ops

.. note:: The *custom* profile is not recommended unless you are an advanced user.

And that's it! You are ready to run workloads on the cluster and check if the
QoS requirements are being met.


.. index:: mclock; config settings

mClock Config Options
=====================

``osd_mclock_profile``

:Description: This sets the type of mclock profile to use for providing QoS
              based on operations belonging to different classes (background
              recovery, scrub, snaptrim, client op, osd subop). Once a built-in
              profile is enabled, the lower level mclock resource control
              parameters [*reservation, weight, limit*] and some Ceph
              configuration parameters are set transparently. Note that the
              above does not apply for the *custom* profile.

:Type: String
:Valid Choices: high_client_ops, high_recovery_ops, balanced, custom
:Default: ``high_client_ops``

``osd_mclock_max_capacity_iops``

:Description: Max IOPS capacity (at 4KiB block size) to consider per OSD
              (overrides _ssd and _hdd if non-zero)

:Type: Float
:Default: ``0.0``

``osd_mclock_max_capacity_iops_hdd``

:Description: Max IOPS capacity (at 4KiB block size) to consider per OSD (for
              rotational media)

:Type: Float
:Default: ``315.0``

``osd_mclock_max_capacity_iops_ssd``

:Description: Max IOPS capacity (at 4KiB block size) to consider per OSD (for
              solid state media)

:Type: Float
:Default: ``21500.0``

``osd_mclock_cost_per_io_usec``

:Description: Cost per IO in microseconds to consider per OSD (overrides _ssd
              and _hdd if non-zero)

:Type: Float
:Default: ``0.0``

``osd_mclock_cost_per_io_usec_hdd``

:Description: Cost per IO in microseconds to consider per OSD (for rotational
              media)

:Type: Float
:Default: ``25000.0``

``osd_mclock_cost_per_io_usec_ssd``

:Description: Cost per IO in microseconds to consider per OSD (for solid state
              media)

:Type: Float
:Default: ``50.0``

``osd_mclock_cost_per_byte_usec``

:Description: Cost per byte in microseconds to consider per OSD (overrides _ssd
              and _hdd if non-zero)

:Type: Float
:Default: ``0.0``

``osd_mclock_cost_per_byte_usec_hdd``

:Description: Cost per byte in microseconds to consider per OSD (for rotational
              media)

:Type: Float
:Default: ``5.2``

``osd_mclock_cost_per_byte_usec_ssd``

:Description: Cost per byte in microseconds to consider per OSD (for solid state
              media)

:Type: Float
:Default: ``0.011``



.. _OSD Config Reference: ../osd-config-ref#dmclock-qos
