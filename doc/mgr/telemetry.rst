.. _telemetry:

Telemetry Module
================

The telemetry module sends anonymous data about the cluster back to the
developers to report how Ceph is used and to report problems experienced by
users. 

This data is visualized on the `public dashboards
<https://telemetry-public.ceph.com/>`_ that allow the community to see a
summary of statistics including how many clusters are reporting, their total
capacity and OSD count, and version distribution trends.

Channels
--------

The telemetry report is broken down into several "channels", each with a
different type of information. When telemetry is enabled, individual channels
can be turned on and off. (If telemetry is disabled, the per-channel setting
has no effect.)

* **basic** (default: on): Basic information about the cluster

    - capacity of the cluster
    - number of Monitors, Managers, OSDs, MDSs, object gateways, or other
      daemons
    - software version currently being used
    - number and types of RADOS pools and CephFS file systems
    - names of configuration options that have been changed from their
      default (but *not* their values)

* **crash** (default: on): Information about daemon crashes, including

    - type of daemon
    - version of the daemon
    - operating system (OS distribution, kernel version)
    - stack trace, identifying where in the Ceph code the crash occurred

* **device** (default: on): Information about device metrics, including

    - anonymized SMART metrics

* **ident** (default: off): User-provided identifying information about
  the cluster

    - cluster description
    - contact email address

* **perf** (default: off): Various performance metrics of a cluster, which can
  be used to

    - reveal overall cluster health
    - identify workload patterns
    - troubleshoot issues with latency, throttling, memory management, etc.
    - monitor cluster performance by daemon

The reported data does *not* contain any sensitive data. This means that the
reported data does not include pool names, object names, object contents,
hostnames, or device serial numbers.

The reported data contains counters and statistics pertaining to how the
cluster has been deployed, the version of Ceph, the OS distribution, and other
parameters that help the project develop a better understanding of the way Ceph
is used.

Data is sent secured to
`https://telemetry.ceph.com<https://telemetry.ceph.com>`_.

Individual channels can be enabled or disabled by running the following
commands:

.. prompt:: bash #

   ceph telemetry enable channel basic
   ceph telemetry enable channel crash
   ceph telemetry enable channel device
   ceph telemetry enable channel ident
   ceph telemetry enable channel perf
  
   ceph telemetry disable channel basic
   ceph telemetry disable channel crash
   ceph telemetry disable channel device
   ceph telemetry disable channel ident
   ceph telemetry disable channel perf

Multiple channels can be enabled or disabled at the same time by running
commands of the following form:

.. prompt:: bash #

   ceph telemetry enable channel basic crash device ident perf
   ceph telemetry disable channel basic crash device ident perf

All channels can be enabled or disabled at once by running the following
commands:

.. prompt:: bash #

   ceph telemetry enable channel all
   ceph telemetry disable channel all

Note that telemetry must be enabled for these commands to take effect.

List all channels with:

.. prompt:: bash #

  ceph telemetry channel ls

::

  NAME      ENABLED    DEFAULT    DESC
  basic     ON         ON         Share basic cluster information (size, version)
  crash     ON         ON         Share metadata about Ceph daemon crashes (version, stack straces, etc)
  device    ON         ON         Share device health metrics (e.g., SMART data, minus potentially identifying info like serial numbers)
  ident     OFF        OFF        Share a user-provided description and/or contact email for the cluster
  perf      ON         OFF        Share various performance metrics of a cluster


Enabling Telemetry
------------------

To allow the *telemetry* module to share data, run the following command:

.. prompt:: bash #

   ceph telemetry on

Please note: Telemetry data is licensed under the `Community Data License
Agreement - Sharing - Version 1.0 <https://cdla.io/sharing-1-0/>`_.  This means
that telemetry module can be enabled only after you add ``--license
sharing-1-0`` to the ``ceph telemetry on`` command. After telemetry is on,
consider enabling channels which are off by default, such as the
``perf`` channel.  ``ceph telemetry on`` output will list the exact command to
enable these channels.

Telemetry can be disabled at any time by running the following command:

.. prompt:: bash #

   ceph telemetry off

Sample report
-------------

Show reported data by running the following command: 

.. prompt:: bash #

   ceph telemetry show

If telemetry is disabled, run the following command to preview a sample report:

.. prompt:: bash #

   ceph telemetry preview

The generation of a sample report might take a few moments in big clusters
(clusters with hundreds of OSDs or more).

To protect your privacy, device reports are generated separately. Data
including  hostnames and device serial numbers are anonymized. The device
telemetry is sent to a different endpoint and does not associate the device
data with a particular cluster. To see a preview of the device report, run the
following command: 

.. prompt:: bash #

   ceph telemetry show-device

If telemetry is disabled, run the following command to preview a sample device
report:

.. prompt:: bash #

   ceph telemetry preview-device

.. note:: ``smartmontools`` version 7.0 or later must be installed so that JSON
   output can be specified and parsed. If you have any concerns about privacy
   with regard to the information included in this report, contact the Ceph
   developers.

When telemetry is enabled, run the following command to generate both reports
in a single output: 

.. prompt:: bash #

   ceph telemetry show-all

When telemetry is disabled, run the following command to view both reports in a
single output:

.. prompt:: bash #

   ceph telemetry preview-all

**Sample report by channel**

Run the following command when telemetry is enabled to show the data reported
by a specified channel:

.. prompt:: bash #

   ceph telemetry show <channel_name>

Please note: If telemetry is on, and ``<channel_name>`` is disabled, the
command above will output a sample report by that channel, according to the
collections the user is enrolled to. However this data is not reported, since
the channel is disabled.

If telemetry is off you can preview a sample report by channel with:

.. prompt:: bash #

  ceph telemetry preview <channel_name>

Collections
-----------

Collections represent different aspects of data collected within a
channel.

To list all collections, run the following command:

.. prompt:: bash #

   ceph telemetry collection ls

::

  NAME                            STATUS                                               DESC
  basic_base                      NOT REPORTING: NOT OPTED-IN                          Basic information about the cluster (capacity, number and type of daemons, version, etc.)
  basic_mds_metadata              NOT REPORTING: NOT OPTED-IN                          MDS metadata
  basic_pool_flags                NOT REPORTING: NOT OPTED-IN                          Per-pool flags
  basic_pool_options_bluestore    NOT REPORTING: NOT OPTED-IN                          Per-pool bluestore config options
  basic_pool_usage                NOT REPORTING: NOT OPTED-IN                          Default pool application and usage statistics
  basic_rook_v01                  NOT REPORTING: NOT OPTED-IN                          Basic Rook deployment data
  basic_stretch_cluster           NOT REPORTING: NOT OPTED-IN                          Stretch Mode information for stretch clusters deployments
  basic_usage_by_class            NOT REPORTING: NOT OPTED-IN                          Default device class usage statistics
  crash_base                      NOT REPORTING: NOT OPTED-IN                          Information about daemon crashes (daemon type and version, backtrace, etc.)
  device_base                     NOT REPORTING: NOT OPTED-IN                          Information about device health metrics
  ident_base                      NOT REPORTING: NOT OPTED-IN, CHANNEL ident IS OFF    User-provided identifying information about the cluster
  perf_memory_metrics             NOT REPORTING: NOT OPTED-IN, CHANNEL perf IS OFF     Heap stats and mempools for mon and mds
  perf_perf                       NOT REPORTING: NOT OPTED-IN, CHANNEL perf IS OFF     Information about performance counters of the cluster

Where:

.. glossary:: 

        NAME
                Collection name; prefix indicates the channel the collection
                belongs to.

        STATUS
                Indicates whether the collection metrics are reported; this is
                determined by the status (enabled / disabled) of the channel
                the collection belongs to, along with the enrollment status of
                the collection (whether the user is opted-in to this
                collection).

        DESC
                General description of the collection.

To print the diff that displays the differences between (1) the collections you
are enrolled to and (2) the new, available collections, run the following
command:

.. prompt:: bash #

   ceph telemetry diff

To enroll to the most recent collections, run the following command:

.. prompt:: bash #

   ceph telemetry on

Enable a new channel that is disabled by running a command of the following
form:

.. prompt:: bash #

   ceph telemetry enable channel <channel_name>

Interval
--------

The telemetry module compiles and sends a new report every 24 hours by default.
Adjust this interval by running a command of the following form:

.. prompt:: bash #

   ceph config set mgr mgr/telemetry/interval 72    # report every three days

Status
--------

To print the current configuration of the telemetry module, run a command of
the following form:

.. prompt:: bash #

   ceph telemetry status

Manually sending telemetry
--------------------------

To send an ansynchronous, one-time set of telemetry data, run the following
command:

.. prompt:: bash #

   ceph telemetry send

If telemetry has not been enabled (by running the command ``ceph telemetry
on``), add ``--license sharing-1-0`` to the ``ceph telemetry send`` command.

Sending telemetry through a proxy
---------------------------------

If the cluster cannot directly connect to the configured telemetry
endpoint (default: ``telemetry.ceph.com``), configure an HTTP/HTTPS
proxy server by running a command of the following form:

.. prompt:: bash #

   ceph config set mgr mgr/telemetry/proxy https://10.0.0.1:8080

Include a colon-separated user and password (``user:pass``) if needed by
running a command of the following form:

.. prompt:: bash #

   ceph config set mgr mgr/telemetry/proxy https://ceph:telemetry@10.0.0.1:8080


Contact and Description
-----------------------

A contact and description can be added to the report. This is optional and is
disabled by default. Run commands of the following forms to add contacts and
descriptions:

.. prompt:: bash #

   ceph config set mgr mgr/telemetry/contact 'John Doe <john.doe@example.com>'
   ceph config set mgr mgr/telemetry/description 'My first Ceph cluster'
   ceph config set mgr mgr/telemetry/channel_ident true

Leaderboard
-----------

To participate in a leaderboard in the `public dashboards
<https://telemetry-public.ceph.com/>`_, run the following command:

.. prompt:: bash #

   ceph config set mgr mgr/telemetry/leaderboard true

The leaderboard displays basic information about the cluster. This includes the
cluster's total storage capacity and the number of OSDs. To add a description
of the cluster so that it can more easily be identified on the leaderboard, run
a command of the following form: 

.. prompt:: bash #

   ceph config set mgr mgr/telemetry/leaderboard_description 'Ceph cluster for Computational Biology at the University of XYZ'

If the ``ident`` channel is enabled, its details will not be displayed in the
leaderboard.
