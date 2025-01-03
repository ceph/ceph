.. _mgr-prometheus:

=================
Prometheus Module
=================

Provides a Prometheus exporter to pass on Ceph performance counters
from the collection point in ceph-mgr.  Ceph-mgr receives MMgrReport
messages from all MgrClient processes (mons and OSDs, for instance)
with performance counter schema data and actual counter data, and keeps
a circular buffer of the last N samples.  This module creates an HTTP
endpoint (like all Prometheus exporters) and retrieves the latest sample
of every counter when polled (or "scraped" in Prometheus terminology).
The HTTP path and query parameters are ignored; all extant counters
for all reporting entities are returned in text exposition format.
(See the Prometheus `documentation <https://prometheus.io/docs/instrumenting/exposition_formats/#text-format-details>`_.)

Enabling prometheus output
==========================

The *prometheus* module is enabled with:

.. prompt:: bash $

   ceph mgr module enable prometheus

Configuration
-------------

.. note::

    The Prometheus manager module needs to be restarted for configuration changes to be applied.

.. mgr_module:: prometheus
.. confval:: server_addr
.. confval:: server_port
.. confval:: scrape_interval
.. confval:: cache
.. confval:: stale_cache_strategy
.. confval:: rbd_stats_pools
.. confval:: rbd_stats_pools_refresh_interval
.. confval:: standby_behaviour
.. confval:: standby_error_status_code
.. confval:: exclude_perf_counters

By default the module will accept HTTP requests on port ``9283`` on all IPv4
and IPv6 addresses on the host.  The port and listen address are both
configurable with ``ceph config set``, with keys
``mgr/prometheus/server_addr`` and ``mgr/prometheus/server_port``.  This port
is registered with Prometheus's `registry
<https://github.com/prometheus/prometheus/wiki/Default-port-allocations>`_.

.. prompt:: bash $
   
   ceph config set mgr mgr/prometheus/server_addr 0.0.0.
   ceph config set mgr mgr/prometheus/server_port 9283

.. warning::

    The :confval:`mgr/prometheus/scrape_interval` of this module should always be set to match
    Prometheus' scrape interval to work properly and not cause any issues.

The scrape interval in the module is used for caching purposes
and to determine when a cache is stale.

It is not recommended to use a scrape interval below 10 seconds.  It is
recommended to use 15 seconds as scrape interval, though, in some cases it
might be useful to increase the scrape interval.

To set a different scrape interval in the Prometheus module, set
``scrape_interval`` to the desired value:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/scrape_interval 20

On large clusters (>1000 OSDs), the time to fetch the metrics may become
significant.  Without the cache, the Prometheus manager module could, especially
in conjunction with multiple Prometheus instances, overload the manager and lead
to unresponsive or crashing Ceph manager instances.  Hence, the cache is enabled
by default.  This means that there is a possibility that the cache becomes
stale.  The cache is considered stale when the time to fetch the metrics from
Ceph exceeds the configured :confval:`mgr/prometheus/scrape_interval`.

If that is the case, **a warning will be logged** and the module will either

* respond with a 503 HTTP status code (service unavailable) or,
* it will return the content of the cache, even though it might be stale.

This behavior can be configured. By default, it will return a 503 HTTP status
code (service unavailable). You can set other options using the ``ceph config
set`` commands.

To tell the module to respond with possibly stale data, set it to ``return``:

.. prompt:: bash $

    ceph config set mgr mgr/prometheus/stale_cache_strategy return

To tell the module to respond with "service unavailable", set it to ``fail``:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/stale_cache_strategy fail

If you are confident that you don't require the cache, you can disable it:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/cache false

If you are using the prometheus module behind some kind of reverse proxy or
loadbalancer, you can simplify discovering the active instance by switching
to ``error``-mode:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/standby_behaviour error

If set, the prometheus module will respond with a HTTP error when requesting ``/``
from the standby instance. The default error code is 500, but you can configure
the HTTP response code with:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/standby_error_status_code 503

Valid error codes are between 400-599.

To switch back to the default behaviour, simply set the config key to ``default``:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/standby_behaviour default

.. _prometheus-rbd-io-statistics:

Ceph Health Checks
------------------

The mgr/prometheus module also tracks and maintains a history of Ceph health checks,
exposing them to the Prometheus server as discrete metrics. This allows Prometheus
alert rules to be configured for specific health check events.

The metrics take the following form;

::

    # HELP ceph_health_detail healthcheck status by type (0=inactive, 1=active)
    # TYPE ceph_health_detail gauge
    ceph_health_detail{name="OSDMAP_FLAGS",severity="HEALTH_WARN"} 0.0
    ceph_health_detail{name="OSD_DOWN",severity="HEALTH_WARN"} 1.0
    ceph_health_detail{name="PG_DEGRADED",severity="HEALTH_WARN"} 1.0

The health check history is made available through the following commands;

::

    healthcheck history ls [--format {plain|json|json-pretty}]
    healthcheck history clear

The ``ls`` command provides an overview of the health checks that the cluster has
encountered, or since the last ``clear`` command was issued. The example below;

::

    [ceph: root@c8-node1 /]# ceph healthcheck history ls
    Healthcheck Name          First Seen (UTC)      Last seen (UTC)       Count  Active
    OSDMAP_FLAGS              2021/09/16 03:17:47   2021/09/16 22:07:40       2    No
    OSD_DOWN                  2021/09/17 00:11:59   2021/09/17 00:11:59       1   Yes
    PG_DEGRADED               2021/09/17 00:11:59   2021/09/17 00:11:59       1   Yes
    3 health check(s) listed


RBD IO statistics
-----------------

The module can optionally collect RBD per-image IO statistics by enabling
dynamic OSD performance counters. The statistics are gathered for all images
in the pools that are specified in the ``mgr/prometheus/rbd_stats_pools``
configuration parameter. The parameter is a comma or space separated list
of ``pool[/namespace]`` entries. If the namespace is not specified the
statistics are collected for all namespaces in the pool.

Example to activate the RBD-enabled pools ``pool1``, ``pool2`` and ``poolN``:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/rbd_stats_pools "pool1,pool2,poolN"

The wildcard can be used to indicate all pools or namespaces:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/rbd_stats_pools "*"

The module makes the list of all available images scanning the specified
pools and namespaces and refreshes it periodically. The period is
configurable via the ``mgr/prometheus/rbd_stats_pools_refresh_interval``
parameter (in sec) and is 300 sec (5 minutes) by default. The module will
force refresh earlier if it detects statistics from a previously unknown
RBD image.

Example to turn up the sync interval to 10 minutes:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/rbd_stats_pools_refresh_interval 600

Ceph daemon performance counters metrics
-----------------------------------------

With the introduction of ``ceph-exporter`` daemon, the prometheus module will no longer export Ceph daemon
perf counters as prometheus metrics by default. However, one may re-enable exporting these metrics by setting
the module option ``exclude_perf_counters`` to ``false``:

.. prompt:: bash $

   ceph config set mgr mgr/prometheus/exclude_perf_counters false

Statistic names and labels
==========================

The names of the stats are exactly as Ceph names them, with
illegal characters ``.``, ``-`` and ``::`` translated to ``_``,
and ``ceph_`` prefixed to all names.


All *daemon* statistics have a ``ceph_daemon`` label such as "osd.123"
that identifies the type and ID of the daemon they come from.  Some
statistics can come from different types of daemon, so when querying
e.g. an OSD's RocksDB stats, you would probably want to filter
on ceph_daemon starting with "osd" to avoid mixing in the monitor
rocksdb stats.


The *cluster* statistics (i.e. those global to the Ceph cluster)
have labels appropriate to what they report on.  For example,
metrics relating to pools have a ``pool_id`` label.


The long running averages that represent the histograms from core Ceph
are represented by a pair of ``<name>_sum`` and ``<name>_count`` metrics.
This is similar to how histograms are represented in `Prometheus <https://prometheus.io/docs/concepts/metric_types/#histogram>`_
and they can also be treated `similarly <https://prometheus.io/docs/practices/histograms/>`_.

Pool and OSD metadata series
----------------------------

Special series are output to enable displaying and querying on
certain metadata fields.

Pools have a ``ceph_pool_metadata`` field like this:

::

    ceph_pool_metadata{pool_id="2",name="cephfs_metadata_a"} 1.0

OSDs have a ``ceph_osd_metadata`` field like this:

::

    ceph_osd_metadata{cluster_addr="172.21.9.34:6802/19096",device_class="ssd",ceph_daemon="osd.0",public_addr="172.21.9.34:6801/19096",weight="1.0"} 1.0


Correlating drive statistics with node_exporter
-----------------------------------------------

The prometheus output from Ceph is designed to be used in conjunction
with the generic host monitoring from the Prometheus node_exporter.

To enable correlation of Ceph OSD statistics with node_exporter's
drive statistics, special series are output like this:

::

    ceph_disk_occupation_human{ceph_daemon="osd.0", device="sdd", exported_instance="myhost"}

To use this to get disk statistics by OSD ID, use either the ``and`` operator or
the ``*`` operator in your prometheus query. All metadata metrics (like ``
ceph_disk_occupation_human`` have the value 1 so they act neutral with ``*``. Using ``*``
allows to use ``group_left`` and ``group_right`` grouping modifiers, so that
the resulting metric has additional labels from one side of the query.

See the
`prometheus documentation`__ for more information about constructing queries.

__ https://prometheus.io/docs/prometheus/latest/querying/basics

The goal is to run a query like

::

    rate(node_disk_written_bytes_total[30s]) and
    on (device,instance) ceph_disk_occupation_human{ceph_daemon="osd.0"}

Out of the box the above query will not return any metrics since the ``instance`` labels of
both metrics don't match. The ``instance`` label of ``ceph_disk_occupation_human``
will be the currently active MGR node.

The following two section outline two approaches to remedy this.

.. note::

    If you need to group on the `ceph_daemon` label instead of `device` and
    `instance` labels, using `ceph_disk_occupation_human` may not work reliably.
    It is advised that you use `ceph_disk_occupation` instead.

    The difference is that `ceph_disk_occupation_human` may group several OSDs
    into the value of a single `ceph_daemon` label in cases where multiple OSDs
    share a disk.

Use label_replace
=================

The ``label_replace`` function (cp.
`label_replace documentation <https://prometheus.io/docs/prometheus/latest/querying/functions/#label_replace>`_)
can add a label to, or alter a label of, a metric within a query.

To correlate an OSD and its disks write rate, the following query can be used:

::

    label_replace(
        rate(node_disk_written_bytes_total[30s]),
        "exported_instance",
        "$1",
        "instance",
        "(.*):.*"
    ) and on (device, exported_instance) ceph_disk_occupation_human{ceph_daemon="osd.0"}

Configuring Prometheus server
=============================

honor_labels
------------

To enable Ceph to output properly-labeled data relating to any host,
use the ``honor_labels`` setting when adding the ceph-mgr endpoints
to your prometheus configuration.

This allows Ceph to export the proper ``instance`` label without prometheus
overwriting it. Without this setting, Prometheus applies an ``instance`` label
that includes the hostname and port of the endpoint that the series came from.
Because Ceph clusters have multiple manager daemons, this results in an
``instance`` label that changes spuriously when the active manager daemon
changes.

If this is undesirable a custom ``instance`` label can be set in the
Prometheus target configuration: you might wish to set it to the hostname
of your first mgr daemon, or something completely arbitrary like "ceph_cluster".

node_exporter hostname labels
-----------------------------

Set your ``instance`` labels to match what appears in Ceph's OSD metadata
in the ``instance`` field.  This is generally the short hostname of the node.

This is only necessary if you want to correlate Ceph stats with host stats,
but you may find it useful to do it in all cases in case you want to do
the correlation in the future.

Example configuration
---------------------

This example shows a single node configuration running ceph-mgr and
node_exporter on a server called ``senta04``. Note that this requires one
to add an appropriate and unique ``instance`` label to each ``node_exporter`` target.

This is just an example: there are other ways to configure prometheus
scrape targets and label rewrite rules.

prometheus.yml
~~~~~~~~~~~~~~

::

    global:
      scrape_interval:     15s
      evaluation_interval: 15s

    scrape_configs:
      - job_name: 'node'
        file_sd_configs:
          - files:
            - node_targets.yml
      - job_name: 'ceph'
        honor_labels: true
        file_sd_configs:
          - files:
            - ceph_targets.yml


ceph_targets.yml
~~~~~~~~~~~~~~~~


::

    [
        {
            "targets": [ "senta04.mydomain.com:9283" ],
            "labels": {}
        }
    ]


node_targets.yml
~~~~~~~~~~~~~~~~

::

    [
        {
            "targets": [ "senta04.mydomain.com:9100" ],
            "labels": {
                "instance": "senta04"
            }
        }
    ]


Notes
=====

Counters and gauges are exported; currently histograms and long-running
averages are not.  It's possible that Ceph's 2-D histograms could be
reduced to two separate 1-D histograms, and that long-running averages
could be exported as Prometheus' Summary type.

Timestamps, as with many Prometheus exporters, are established by
the server's scrape time (Prometheus expects that it is polling the
actual counter process synchronously).  It is possible to supply a
timestamp along with the stat report, but the Prometheus team strongly
advises against this.  This means that timestamps will be delayed by
an unpredictable amount; it's not clear if this will be problematic,
but it's worth knowing about.
