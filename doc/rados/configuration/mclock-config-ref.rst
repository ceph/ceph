========================
 mClock Config Reference
========================

.. index:: mclock; configuration

Mclock profiles mask the low level details from users, making it
easier for them to configure mclock.

The following input parameters are required for a mclock profile to configure
the QoS related parameters:

* total capacity (IOPS) of each OSD (determined automatically)

* an mclock profile type to enable

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

The mclock profile uses the capacity limits and the mclock profile type selected
by the user to determine the low-level mclock resource control parameters.

Depending on the profile type, lower-level mclock resource-control parameters
and some Ceph-configuration parameters are transparently applied.

The low-level mclock resource control parameters are the *reservation*,
*limit*, and *weight* that provide control of the resource shares, as
described in the :ref:`dmclock-qos` section.


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

- **Custom**: This profile gives users complete control over all the mclock
  configuration parameters. Using this profile is not recommended without
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

- :confval:`osd_mclock_scheduler_client_res`
- :confval:`osd_mclock_scheduler_client_wgt`
- :confval:`osd_mclock_scheduler_client_lim`
- :confval:`osd_mclock_scheduler_background_recovery_res`
- :confval:`osd_mclock_scheduler_background_recovery_wgt`
- :confval:`osd_mclock_scheduler_background_recovery_lim`
- :confval:`osd_mclock_scheduler_background_best_effort_res`
- :confval:`osd_mclock_scheduler_background_best_effort_wgt`
- :confval:`osd_mclock_scheduler_background_best_effort_lim`

The following Ceph options will not be modifiable by the user:

- :confval:`osd_max_backfills`
- :confval:`osd_recovery_max_active`

This is because the above options are internally modified by the mclock
scheduler in order to maximize the impact of the set profile.

By default, the *high_client_ops* profile is enabled to ensure that a larger
chunk of the bandwidth allocation goes to client ops. Background recovery ops
are given lower allocation (and therefore take a longer time to complete). But
there might be instances that necessitate giving higher allocations to either
client ops or recovery ops. In order to deal with such a situation, you can
enable one of the alternate built-in profiles by following the steps mentioned
in the next section.

If any mClock profile (including "custom") is active, the following Ceph config
sleep options will be disabled,

- :confval:`osd_recovery_sleep`
- :confval:`osd_recovery_sleep_hdd`
- :confval:`osd_recovery_sleep_ssd`
- :confval:`osd_recovery_sleep_hybrid`
- :confval:`osd_scrub_sleep`
- :confval:`osd_delete_sleep`
- :confval:`osd_delete_sleep_hdd`
- :confval:`osd_delete_sleep_ssd`
- :confval:`osd_delete_sleep_hybrid`
- :confval:`osd_snap_trim_sleep`
- :confval:`osd_snap_trim_sleep_hdd`
- :confval:`osd_snap_trim_sleep_ssd`
- :confval:`osd_snap_trim_sleep_hybrid`

The above sleep options are disabled to ensure that mclock scheduler is able to
determine when to pick the next op from its operation queue and transfer it to
the operation sequencer. This results in the desired QoS being provided across
all its clients.


.. index:: mclock; enable built-in profile

Steps to Enable mClock Profile
==============================

As already mentioned, the default mclock profile is set to *high_client_ops*.
The other values for the built-in profiles include *balanced* and
*high_recovery_ops*.

If there is a requirement to change the default profile, then the option
:confval:`osd_mclock_profile` may be set during runtime by using the following
command:

  .. prompt:: bash #

    ceph config set [global,osd] osd_mclock_profile <value>

For example, to change the profile to allow faster recoveries, the following
command can be used to switch to the *high_recovery_ops* profile:

  .. prompt:: bash #

    ceph config set osd osd_mclock_profile high_recovery_ops

.. note:: The *custom* profile is not recommended unless you are an advanced
          user.

And that's it! You are ready to run workloads on the cluster and check if the
QoS requirements are being met.


OSD Capacity Determination (Automated)
======================================

The OSD capacity in terms of total IOPS is determined automatically during OSD
initialization. This is achieved by running the OSD bench tool and overriding
the default value of ``osd_mclock_max_capacity_iops_[hdd, ssd]`` option
depending on the device type. No other action/input is expected from the user
to set the OSD capacity. You may verify the capacity of an OSD after the
cluster is brought up by using the following command:

  .. prompt:: bash #

    ceph config show osd.x osd_mclock_max_capacity_iops_[hdd, ssd]

For example, the following command shows the max capacity for osd.0 on a Ceph
node whose underlying device type is SSD:

  .. prompt:: bash #

    ceph config show osd.0 osd_mclock_max_capacity_iops_ssd


Steps to Manually Benchmark an OSD (Optional)
=============================================

.. note:: These steps are only necessary if you want to override the OSD
          capacity already determined automatically during OSD initialization.
          Otherwise, you may skip this section entirely.

Any existing benchmarking tool can be used for this purpose. In this case, the
steps use the *Ceph OSD Bench* command described in the next section. Regardless
of the tool/command used, the steps outlined further below remain the same.

As already described in the :ref:`dmclock-qos` section, the number of
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
  :confval:`bluestore_throttle_bytes` and
  :confval:`bluestore_throttle_deferred_bytes`. But these parameters may also be
  determined during the benchmarking phase as described below.

OSD Bench Command Syntax
````````````````````````

The :ref:`osd-subsystem` section describes the OSD bench command. The syntax
used for benchmarking is shown below :

.. prompt:: bash #

  ceph tell osd.N bench [TOTAL_BYTES] [BYTES_PER_WRITE] [OBJ_SIZE] [NUM_OBJS]

where,

* ``TOTAL_BYTES``: Total number of bytes to write
* ``BYTES_PER_WRITE``: Block size per write
* ``OBJ_SIZE``: Bytes per object
* ``NUM_OBJS``: Number of objects to write

Benchmarking Test Steps Using OSD Bench
```````````````````````````````````````

The steps below use the default shards and detail the steps used to determine
the correct bluestore throttle values (optional).

#. Bring up your Ceph cluster and login to the Ceph node hosting the OSDs that
   you wish to benchmark.
#. Run a simple 4KiB random write workload on an OSD using the following
   commands:

   .. note:: Note that before running the test, caches must be cleared to get an
             accurate measurement.

   For example, if you are running the benchmark test on osd.0, run the following
   commands:

   .. prompt:: bash #

     ceph tell osd.0 cache drop

   .. prompt:: bash #

     ceph tell osd.0 bench 12288000 4096 4194304 100

#. Note the overall throughput(IOPS) obtained from the output of the osd bench
   command. This value is the baseline throughput(IOPS) when the default
   bluestore throttle options are in effect.
#. If the intent is to determine the bluestore throttle values for your
   environment, then set the two options, :confval:`bluestore_throttle_bytes`
   and :confval:`bluestore_throttle_deferred_bytes` to 32 KiB(32768 Bytes) each
   to begin with. Otherwise, you may skip to the next section.
#. Run the 4KiB random write test as before using OSD bench.
#. Note the overall throughput from the output and compare the value
   against the baseline throughput recorded in step 3.
#. If the throughput doesn't match with the baseline, increment the bluestore
   throttle options by 2x and repeat steps 5 through 7 until the obtained
   throughput is very close to the baseline value.

For example, during benchmarking on a machine with NVMe SSDs, a value of 256 KiB
for both bluestore throttle and deferred bytes was determined to maximize the
impact of mclock. For HDDs, the corresponding value was 40 MiB, where the
overall throughput was roughly equal to the baseline throughput. Note that in
general for HDDs, the bluestore throttle values are expected to be higher when
compared to SSDs.


Specifying  Max OSD Capacity
````````````````````````````

The steps in this section may be performed only if you want to override the
max osd capacity automatically determined during OSD initialization. The option
``osd_mclock_max_capacity_iops_[hdd, ssd]`` can be set by running the
following command:

  .. prompt:: bash #

     ceph config set [global,osd] osd_mclock_max_capacity_iops_[hdd,ssd] <value>

For example, the following command sets the max capacity for all the OSDs in a
Ceph node whose underlying device type is SSDs:

  .. prompt:: bash #

    ceph config set osd osd_mclock_max_capacity_iops_ssd 25000

To set the capacity for a specific OSD (for example "osd.0") whose underlying
device type is HDD, use a command like this:

  .. prompt:: bash #

    ceph config set osd.0 osd_mclock_max_capacity_iops_hdd 350


.. index:: mclock; config settings

mClock Config Options
=====================

.. confval:: osd_mclock_profile
.. confval:: osd_mclock_max_capacity_iops_hdd
.. confval:: osd_mclock_max_capacity_iops_ssd
.. confval:: osd_mclock_cost_per_io_usec
.. confval:: osd_mclock_cost_per_io_usec_hdd
.. confval:: osd_mclock_cost_per_io_usec_ssd
.. confval:: osd_mclock_cost_per_byte_usec
.. confval:: osd_mclock_cost_per_byte_usec_hdd
.. confval:: osd_mclock_cost_per_byte_usec_ssd
