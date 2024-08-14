.. _telemetry:

Telemetry Module
================

The telemetry module sends anonymous data about the cluster back to the Ceph
developers to help understand how Ceph is used and what problems users may
be experiencing.

This data is visualized on `public dashboards <https://telemetry-public.ceph.com/>`_
that allow the community to quickly see summary statistics on how many clusters
are reporting, their total capacity and OSD count, and version distribution
trends.

Channels
--------

The telemetry report is broken down into several "channels", each with
a different type of information.  Assuming telemetry has been enabled,
individual channels can be turned on and off.  (If telemetry is off,
the per-channel setting has no effect.)

* **basic** (default: on): Basic information about the cluster

    - capacity of the cluster
    - number of monitors, managers, OSDs, MDSs, object gateways, or other daemons
    - software version currently being used
    - number and types of RADOS pools and CephFS file systems
    - names of configuration options that have been changed from their
      default (but *not* their values)

* **crash** (default: on): Information about daemon crashes, including

    - type of daemon
    - version of the daemon
    - operating system (OS distribution, kernel version)
    - stack trace identifying where in the Ceph code the crash occurred

* **device** (default: on): Information about device metrics, including

    - anonymized SMART metrics

* **ident** (default: off): User-provided identifying information about
  the cluster

    - cluster description
    - contact email address

* **perf** (default: off): Various performance metrics of a cluster, which can be used to

    - reveal overall cluster health
    - identify workload patterns
    - troubleshoot issues with latency, throttling, memory management, etc.
    - monitor cluster performance by daemon

The data being reported does *not* contain any sensitive
data like pool names, object names, object contents, hostnames, or device
serial numbers.

It contains counters and statistics on how the cluster has been
deployed, the version of Ceph, the distribution of the hosts and other
parameters which help the project to gain a better understanding of
the way Ceph is used.

Data is sent secured to *https://telemetry.ceph.com*.

Individual channels can be enabled or disabled with::

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

Multiple channels can be enabled or disabled with::

  ceph telemetry enable channel basic crash device ident perf
  ceph telemetry disable channel basic crash device ident perf

Channels can be enabled or disabled all at once with::

  ceph telemetry enable channel all
  ceph telemetry disable channel all

Please note that telemetry should be on for these commands to take effect.

List all channels with::

  ceph telemetry channel ls

  NAME      ENABLED    DEFAULT    DESC
  basic     ON         ON         Share basic cluster information (size, version)
  crash     ON         ON         Share metadata about Ceph daemon crashes (version, stack straces, etc)
  device    ON         ON         Share device health metrics (e.g., SMART data, minus potentially identifying info like serial numbers)
  ident     OFF        OFF        Share a user-provided description and/or contact email for the cluster
  perf      ON         OFF        Share various performance metrics of a cluster


Enabling Telemetry
------------------

To allow the *telemetry* module to start sharing data::

  ceph telemetry on

Please note: Telemetry data is licensed under the Community Data License
Agreement - Sharing - Version 1.0 (https://cdla.io/sharing-1-0/). Hence,
telemetry module can be enabled only after you add '--license sharing-1-0' to
the 'ceph telemetry on' command.
Once telemetry is on, please consider enabling channels which are off by
default, such as the 'perf' channel. 'ceph telemetry on' output will list the
exact command to enable these channels.

Telemetry can be disabled at any time with::

  ceph telemetry off

Sample report
-------------

You can look at what data is reported at any time with the command::

  ceph telemetry show

If telemetry is off, you can preview a sample report with::

  ceph telemetry preview

Generating a sample report might take a few moments in big clusters (clusters
with hundreds of OSDs or more).

To protect your privacy, device reports are generated separately, and data such
as hostname and device serial number is anonymized. The device telemetry is
sent to a different endpoint and does not associate the device data with a
particular cluster. To see a preview of the device report use the command::

  ceph telemetry show-device

If telemetry is off, you can preview a sample device report with::

  ceph telemetry preview-device

Please note: In order to generate the device report we use Smartmontools
version 7.0 and up, which supports JSON output. 
If you have any concerns about privacy with regard to the information included in
this report, please contact the Ceph developers.

In case you prefer to have a single output of both reports, and telemetry is on, use::

  ceph telemetry show-all

If you would like to view a single output of both reports, and telemetry is off, use::

  ceph telemetry preview-all

**Sample report by channel**

When telemetry is on you can see what data is reported by channel with::

  ceph telemetry show <channel_name>

Please note: If telemetry is on, and <channel_name> is disabled, the command
above will output a sample report by that channel, according to the collections
the user is enrolled to. However this data is not reported, since the channel
is disabled.

If telemetry is off you can preview a sample report by channel with::

  ceph telemetry preview <channel_name>

Collections
-----------

Collections represent different aspects of data that we collect within a channel.

List all collections with::

  ceph telemetry collection ls

  NAME                            STATUS                                               DESC
  basic_base                      NOT REPORTING: NOT OPTED-IN                          Basic information about the cluster (capacity, number and type of daemons, version, etc.)
  basic_mds_metadata              NOT REPORTING: NOT OPTED-IN                          MDS metadata
  basic_pool_flags                NOT REPORTING: NOT OPTED-IN                          Per-pool flags
  basic_pool_options_bluestore    NOT REPORTING: NOT OPTED-IN                          Per-pool bluestore config options
  basic_pool_usage                NOT REPORTING: NOT OPTED-IN                          Default pool application and usage statistics
  basic_rook_v01                  NOT REPORTING: NOT OPTED-IN                          Basic Rook deployment data
  basic_usage_by_class            NOT REPORTING: NOT OPTED-IN                          Default device class usage statistics
  crash_base                      NOT REPORTING: NOT OPTED-IN                          Information about daemon crashes (daemon type and version, backtrace, etc.)
  device_base                     NOT REPORTING: NOT OPTED-IN                          Information about device health metrics
  ident_base                      NOT REPORTING: NOT OPTED-IN, CHANNEL ident IS OFF    User-provided identifying information about the cluster
  perf_memory_metrics             NOT REPORTING: NOT OPTED-IN, CHANNEL perf IS OFF     Heap stats and mempools for mon and mds
  perf_perf                       NOT REPORTING: NOT OPTED-IN, CHANNEL perf IS OFF     Information about performance counters of the cluster

Where:

**NAME**: Collection name; prefix indicates the channel the collection belongs to.

**STATUS**: Indicates whether the collection metrics are reported; this is
determined by the status (enabled / disabled) of the channel the collection
belongs to, along with the enrollment status of the collection (whether the user
is opted-in to this collection).

**DESC**: General description of the collection.

See the diff between the collections you are enrolled to, and the new,
available collections with::

  ceph telemetry diff

Enroll to the most recent collections with::

  ceph telemetry on

Then enable new channels that are off with::

  ceph telemetry enable channel <channel_name>

Interval
--------

The module compiles and sends a new report every 24 hours by default.
You can adjust this interval with::

  ceph config set mgr mgr/telemetry/interval 72    # report every three days

Status
--------

The see the current configuration::

  ceph telemetry status

Manually sending telemetry
--------------------------

To ad hoc send telemetry data::

  ceph telemetry send

In case telemetry is not enabled (with 'ceph telemetry on'), you need to add
'--license sharing-1-0' to 'ceph telemetry send' command.

Sending telemetry through a proxy
---------------------------------

If the cluster cannot directly connect to the configured telemetry
endpoint (default *telemetry.ceph.com*), you can configure a HTTP/HTTPS
proxy server with::

  ceph config set mgr mgr/telemetry/proxy https://10.0.0.1:8080

You can also include a *user:pass* if needed::

  ceph config set mgr mgr/telemetry/proxy https://ceph:telemetry@10.0.0.1:8080


Contact and Description
-----------------------

A contact and description can be added to the report.  This is
completely optional, and disabled by default.::

  ceph config set mgr mgr/telemetry/contact 'John Doe <john.doe@example.com>'
  ceph config set mgr mgr/telemetry/description 'My first Ceph cluster'
  ceph config set mgr mgr/telemetry/channel_ident true

Leaderboard
-----------

To participate in a leaderboard in the `public dashboards
<https://telemetry-public.ceph.com/>`_, run the following command:

.. prompt:: bash $

   ceph config set mgr mgr/telemetry/leaderboard true

The leaderboard displays basic information about the cluster. This includes the
total storage capacity and the number of OSDs. To add a description of the
cluster, run a command of the following form: 

.. prompt:: bash $

   ceph config set mgr mgr/telemetry/leaderboard_description 'Ceph cluster for Computational Biology at the University of XYZ'

If the ``ident`` channel is enabled, its details will not be displayed in the
leaderboard.

