=======
Metrics
=======

The Ceph Object Gateway uses :ref:`Perf Counters` to track metrics. The counters can be labeled (:ref:`Labeled Perf Counters`). When counters are labeled, they are stored in the Ceph Object Gateway specific caches.

These metrics can be sent to the time series database Prometheus to visualize a cluster-wide view of usage data (for example, number of S3 put operations on a specific bucket) over time.

.. contents::

Op Metrics
==========

The following metrics related to S3 or Swift operations are tracked per Ceph Object Gateway.

.. list-table:: Ceph Object Gateway Op Metrics
   :widths: 25 25 75
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - put_obj_ops
     - Counter
     - Number of put operations
   * - put_obj_bytes
     - Counter
     - Number of bytes put
   * - put_obj_lat
     - Gauge
     - Total latency of put operations
   * - get_obj_ops
     - Counter
     - Number of get operations
   * - get_obj_bytes
     - Counter
     - Number of bytes from get requests
   * - get_obj_lat
     - Gauge
     - Total latency of get operations
   * - del_obj_ops
     - Counter
     - Number of delete object operations
   * - del_obj_bytes
     - Counter
     - Number of bytes deleted
   * - del_obj_lat
     - Gauge
     - Total latency of delete object operations
   * - del_bucket_ops
     - Counter
     - Number of delete bucket operations
   * - del_bucket_lat
     - Gauge
     - Total latency of delete bucket operations
   * - copy_obj_ops
     - Counter
     - Number of copy object operations
   * - copy_obj_bytes
     - Counter
     - Number of bytes copied
   * - copy_obj_lat
     - Gauge
     - Total latency of copy object operations
   * - list_object_ops
     - Counter
     - Number of list object operations
   * - list_object_lat
     - Gauge
     - Total latency of list object operations
   * - list_bucket_ops
     - Counter
     - Number of list bucket operations
   * - list_bucket_lat
     - Gauge
     - Total latency of list bucket operations
   * - head_obj_ops
     - Counter
     - Number of successful HEAD Object operations
   * - head_obj_lat
     - Gauge
     - Total latency of HEAD Object operations

There are three different sections in the output of the ``counter dump`` and ``counter schema`` commands that show the op metrics and their information.
The sections are ``rgw_op``, ``rgw_op_per_user``, and ``rgw_op_per_bucket``.

The counters in the ``rgw_op`` section reflect the totals of each op metric for a given Ceph Object Gateway.
The counters in the ``rgw_op_per_user`` and ``rgw_op_per_bucket`` sections are labeled counters of op metrics for a user or bucket respectively.

Information about op metrics can be seen in the ``rgw_op`` sections of the output of the ``counter schema`` command.

To view op metrics in the Ceph Object Gateway go to the ``rgw_op`` sections of the output of the ``counter dump`` command::

    "rgw_op": [
        {
            "labels": {},
            "counters": {
                "put_obj_ops": 2,
                "put_obj_bytes": 5327,
                "put_obj_lat": {
                    "avgcount": 2,
                    "sum": 2.818064835,
                    "avgtime": 1.409032417
                },
                "get_obj_ops": 5,
                "get_obj_bytes": 5325,
                "get_obj_lat": {
                    "avgcount": 2,
                    "sum": 0.003000069,
                    "avgtime": 0.001500034
                },
                ...
                "list_buckets_ops": 1,
                "list_buckets_lat": {
                    "avgcount": 1,
                    "sum": 0.002300000,
                    "avgtime": 0.002300000
                },
                "head_obj_ops": 3,
                "head_obj_lat": {
                    "avgcount": 3,
                    "sum": 0.006123456,
                    "avgtime": 0.002041152
                }
            }
        },
    ]

Op Metrics Labels
-----------------

Op metrics can also be tracked per-user or per-bucket. These metrics are exported to Prometheus with labels ``bucket = {name}`` or ``user = {userid}``::

    "rgw_op_per_bucket": [
        ...
        {
            "labels": {
                "Bucket": "bucket1"
            },
            "counters": {
                "put_obj_ops": 2,
                "put_obj_bytes": 5327,
                "put_obj_lat": {
                    "avgcount": 2,
                    "sum": 2.818064835,
                    "avgtime": 1.409032417
                },
                "get_obj_ops": 5,
                "get_obj_bytes": 5325,
                "get_obj_lat": {
                    "avgcount": 2,
                    "sum": 0.003000069,
                    "avgtime": 0.001500034
                },
                ...
                "list_buckets_ops": 1,
                "list_buckets_lat": {
                    "avgcount": 1,
                    "sum": 0.002300000,
                    "avgtime": 0.002300000
                },
                "head_obj_ops": 3,
                "head_obj_lat": {
                    "avgcount": 3,
                    "sum": 0.006123456,
                    "avgtime": 0.002041152
                }
            }
        },
        ...
    ]

:ref:`rgw-multitenancy` allows the use of buckets and users with the same name,
if they are created under different tenants.  If a user or bucket lies under a
tenant, a label for the tenant in the form ``Tenant = {tenantid}`` is added to
the metric.

In a large system with many users and buckets, it may not be tractable to export all metrics to Prometheus. For that reason, the collection of these labeled metrics is disabled by default.

Once enabled, the working set of tracked users and buckets is constrained to limit memory and database usage. As a result, the collection of these labeled metrics will not always be reliable.


User & Bucket Counter Caches
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To track op metrics by user the Ceph Object Gateway the config value ``rgw_user_counters_cache`` must be set to ``true``. 

To track op metrics by bucket the Ceph Object Gateway the config value ``rgw_bucket_counters_cache`` must be set to ``true``. 

These config values are set in Ceph via the command ``ceph config set client.rgw rgw_{user,bucket}_counters_cache true``

Since the op metrics are labeled perf counters, they live in memory. If the Ceph Object Gateway is restarted or crashes, all counters in the Ceph Object Gateway, whether in a cache or not, are lost.

User & Bucket Counter Cache Size & Eviction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Both ``rgw_user_counters_cache_size`` and ``rgw_bucket_counters_cache_size`` can be used to set number of entries in each cache.

Counters are evicted from a cache once the number of counters in the cache are greater than the cache size config variable. The counters that are evicted are the least recently used (LRU). 

For example if the number of buckets exceeded ``rgw_bucket_counters_cache_size`` by 1 and the counters with label ``bucket1`` were the last to be updated, the counters for ``bucket1`` would be evicted from the cache. If S3 operations tracked by the op metrics were done on ``bucket1`` after eviction, all of the metrics in the cache for ``bucket1`` would start at 0.

Cache sizing can depend on a number of factors. These factors include:

#. Number of users in the cluster
#. Number of buckets in the cluster
#. Memory usage of the Ceph Object Gateway
#. Disk and memory usage of Prometheus.

To help calculate the Ceph Object Gateway's memory usage of a cache, it should be noted that each cache entry, encompassing all of the op metrics, is 1360 bytes. This is an estimate and subject to change if metrics are added or removed from the op metrics list.

Lifecycle Metrics
=================

The following metrics related to lifecycle (LC) processing are tracked per bucket by the Ceph Object Gateway.

.. list-table:: Ceph Object Gateway Lifecycle Metrics
   :widths: 25 25 75
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - ``start_time``
     - Gauge
     - LC processing start timestamp (Unix epoch seconds)
   * - ``end_time``
     - Gauge
     - LC processing end timestamp (Unix epoch seconds)
   * - ``objects_scanned``
     - Counter
     - Total objects scanned for lifecycle rules (cumulative across all runs since RGW start)
   * - ``objects_pending``
     - Gauge
     - Objects currently pending lifecycle processing
   * - ``objects_expired``
     - Counter
     - Current-version objects expired by lifecycle in current run
   * - ``objects_noncurrent_expired``
     - Counter
     - Noncurrent-version objects expired by lifecycle in current run
   * - ``objects_dm_expired``
     - Counter
     - Delete markers expired by lifecycle in current run
   * - ``objects_transitioned``
     - Counter
     - Objects transitioned to another storage class by lifecycle in current run
   * - ``objects_mpu_aborted``
     - Counter
     - Multipart uploads aborted by lifecycle in current run

Lifecycle metrics are labeled per-bucket in the ``rgw_lc_per_bucket`` section.

The action metrics (``objects_expired``, ``objects_noncurrent_expired``,
``objects_dm_expired``, ``objects_transitioned``, ``objects_mpu_aborted``) and
``objects_scanned`` are monotonic counters that accumulate across every LC run
for the lifetime of the Ceph Object Gateway process.

To compute per-run deltas in Prometheus, use ``increase()`` over a window that
covers a single LC run (for example, ``increase(rgw_lc_per_bucket_objects_scanned[24h])``
on the default daily LC schedule). To detect a new LC run, look for a change
in ``start_time``.

Information about lifecycle metrics can be seen in the ``rgw_lc_per_bucket``
section from the output of the ``counter schema`` command.

To retrieve lifecycle metrics from a ``radosgw`` daemon's admin socket, see the
``rgw_lc_per_bucket`` section in the output of the ``counter dump`` command::

    "rgw_lc_per_bucket": [
        {
            "labels": {
                "bucket": "mybucket",
                "tenant": ""
            },
            "counters": {
                "start_time": 1728139406,
                "end_time": 0,
                "objects_scanned": 15000,
                "objects_pending": 2300,
                "objects_expired": 1200,
                "objects_noncurrent_expired": 300,
                "objects_dm_expired": 50,
                "objects_transitioned": 800,
                "objects_mpu_aborted": 10
            }
        },
        ...
    ]

Lifecycle Counter Cache
-----------------------

To track lifecycle metrics per bucket, set :confval:`rgw_lc_counters_cache` to ``true``. The default value is ``false``.

Lifecycle metrics are stored as labeled performance counters in memory. All counters are lost when the Ceph Object Gateway restarts or crashes.

Since ``ceph-mgr`` cannot expose labeled counters today; use the per-host ``ceph-exporter`` daemon to scrape these metrics.

Lifecycle Counter Cache Size & Eviction
----------------------------------------

The :confval:`rgw_lc_counters_cache_size` option can be used to set number of entries in the cache.

When the number of cached counters exceeds this value, the least recently used (LRU) counters are evicted.

Lifecycle Counter Batching
---------------------------

To minimize performance impact, lifecycle counter updates are batched. The :confval:`rgw_lc_counters_batch_size` option controls how often counter updates are flushed to the cache.

Lower values provide more frequent updates but with slightly higher overhead. Higher values reduce overhead but updates appear less frequently.

Lifecycle Metric Usage Examples
-------------------------------

The following examples show how to use each per-bucket lifecycle metric.
PromQL examples assume that the ``ceph-exporter`` is being scraped by
Prometheus. Admin-socket examples use ``ceph daemon`` directly against a
specific ``radosgw`` daemon instance.

In a multi-RGW deployment only one RGW processes a given bucket per LC cycle. To find which daemon ran (or is running) LC for a bucket, take the series with the highest ``start_time`` for that bucket label.

``start_time`` (gauge, Unix epoch seconds)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The timestamp when the most recent LC run started for the bucket. ``0`` means LC has not run for this bucket since the RGW started.

* Find buckets whose LC run started in the last hour:

  .. code-block:: promql

      time() - rgw_lc_per_bucket_start_time < 3600 and rgw_lc_per_bucket_start_time > 0

* Identify which RGW most recently ran LC for ``mybucket``:

  .. code-block:: promql

      topk(1, rgw_lc_per_bucket_start_time{bucket="mybucket"})

* Detect missed LC runs (no run in over 36 hours on a daily LC schedule):

  .. code-block:: promql

      time() - max by (bucket, tenant) (rgw_lc_per_bucket_start_time) > 36 * 3600

``end_time`` (gauge, Unix epoch seconds)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The timestamp when the most recent LC run finished for the bucket. ``0`` while ``start_time > 0`` means LC is currently in progress on that RGW.

* Buckets currently being processed:

  .. code-block:: promql

      rgw_lc_per_bucket_start_time > 0 and rgw_lc_per_bucket_end_time == 0

* Per-run LC duration (seconds):

  .. code-block:: promql

      rgw_lc_per_bucket_end_time - rgw_lc_per_bucket_start_time
        and rgw_lc_per_bucket_end_time > 0

* Buckets whose last LC run took longer than 30 minutes:

  .. code-block:: promql

      (rgw_lc_per_bucket_end_time - rgw_lc_per_bucket_start_time > 1800)
        and rgw_lc_per_bucket_end_time > 0

``objects_scanned`` (counter)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cumulative count of objects examined by LC for this bucket since the RGW process started. Use ``increase()`` to compute the per-run scan total.

* Objects scanned per bucket in the last 24 hours:

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_scanned[24h])

* Top 10 buckets by LC scan rate over the last hour:

  .. code-block:: promql

      topk(10, rate(rgw_lc_per_bucket_objects_scanned[1h]))

* Live scan progress for an in-flight run via admin socket:

  .. code-block:: bash

      ceph daemon radosgw.<id>.asok counter dump \
        | jq '.rgw_lc_per_bucket[]
              | select(.labels.bucket == "mybucket")
              | .counters.objects_scanned'

``objects_pending`` (gauge)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Number of objects currently queued for LC action evaluation. Returns to 0 once the run finishes.

* Buckets with significant in-flight LC work:

  .. code-block:: promql

      rgw_lc_per_bucket_objects_pending > 1000

* Aggregate pending work across the cluster:

  .. code-block:: promql

      sum(rgw_lc_per_bucket_objects_pending)

* Detect a stalled run (pending > 0 but no scan progress in the last 5 minutes):

  .. code-block:: promql

      rgw_lc_per_bucket_objects_pending > 0
        and rate(rgw_lc_per_bucket_objects_scanned[5m]) == 0

``objects_expired`` (counter)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cumulative current-version objects expired by LC for this bucket since RGW start.

* Objects expired per bucket in the last 24 hours:

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_expired[24h])

* Cluster-wide expiration rate (objects/sec):

  .. code-block:: promql

      sum(rate(rgw_lc_per_bucket_objects_expired[5m]))

* Buckets with no expirations in the last 7 days (possible rule misconfiguration on a bucket where you expect deletes):

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_expired[7d]) == 0
        and rgw_lc_per_bucket_start_time > 0

``objects_noncurrent_expired`` (counter)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cumulative noncurrent-version objects expired by LC. Useful only on versioned buckets.

* Noncurrent-version objects removed per run:

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_noncurrent_expired[24h])

* Ratio of noncurrent vs current expirations (high ratio indicates a versioning-heavy workload):

  .. code-block:: promql

      sum by (bucket) (increase(rgw_lc_per_bucket_objects_noncurrent_expired[24h]))
        /
      (sum by (bucket) (increase(rgw_lc_per_bucket_objects_expired[24h])) > 0)

``objects_dm_expired`` (counter)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cumulative delete markers expired by LC. A non-zero value means LC is reclaiming dangling delete markers on a versioned bucket.

* Delete markers cleaned per run:

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_dm_expired[24h])

* Buckets accumulating delete markers without cleanup (no DM expiration in 7 days but recent runs occurred):

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_dm_expired[7d]) == 0
        and increase(rgw_lc_per_bucket_objects_scanned[7d]) > 0

``objects_transitioned`` (counter)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cumulative objects transitioned to another storage class (including cloud tiers) by LC.

* Objects transitioned per bucket in the last 24 hours:

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_transitioned[24h])

* Cluster-wide transition throughput (objects/sec):

  .. code-block:: promql

      sum(rate(rgw_lc_per_bucket_objects_transitioned[5m]))

* Buckets with a configured transition rule but zero transitions in the last 24h (likely investigation target):

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_transitioned[24h]) == 0
        and increase(rgw_lc_per_bucket_objects_scanned[24h]) > 0

``objects_mpu_aborted`` (counter)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cumulative incomplete multipart uploads aborted by LC. Driven by ``AbortIncompleteMultipartUpload`` rules.

* Aborted multipart uploads per bucket per run:

  .. code-block:: promql

      increase(rgw_lc_per_bucket_objects_mpu_aborted[24h])

* Cluster-wide rate of MPU cleanup:

  .. code-block:: promql

      sum(rate(rgw_lc_per_bucket_objects_mpu_aborted[5m]))

Combined Examples
^^^^^^^^^^^^^^^^^

* Total LC work done per bucket in the last 24 hours (scan + all action types):

  .. code-block:: promql

      sum by (bucket, tenant) (
        increase(rgw_lc_per_bucket_objects_expired[24h])
        + increase(rgw_lc_per_bucket_objects_noncurrent_expired[24h])
        + increase(rgw_lc_per_bucket_objects_dm_expired[24h])
        + increase(rgw_lc_per_bucket_objects_transitioned[24h])
        + increase(rgw_lc_per_bucket_objects_mpu_aborted[24h])
      )

* Per-bucket LC hit rate (fraction of scanned objects acted on):

  .. code-block:: promql

      (
        increase(rgw_lc_per_bucket_objects_expired[24h])
        + increase(rgw_lc_per_bucket_objects_noncurrent_expired[24h])
        + increase(rgw_lc_per_bucket_objects_dm_expired[24h])
        + increase(rgw_lc_per_bucket_objects_transitioned[24h])
        + increase(rgw_lc_per_bucket_objects_mpu_aborted[24h])
      )
      /
      increase(rgw_lc_per_bucket_objects_scanned[24h])

Sending Metrics to Prometheus
=============================

To get metrics from a Ceph Object Gateway into the time series database Prometheus, the ceph-exporter daemon must be running and configured to scrape the Ceph Object Gateway's admin socket.

The ceph-exporter daemon scrapes the Ceph Object Gateway's admin socket at a regular interval, defined by the config variable ``exporter_stats_period``.

Prometheus has a configurable interval in which it scrapes the exporter (see: https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

Config Reference
================
The following Ceph Object Gateway op metrics related settings can be set via ``ceph config set client.rgw CONFIG_VARIABLE VALUE``.

.. confval:: rgw_user_counters_cache
.. confval:: rgw_user_counters_cache_size
.. confval:: rgw_bucket_counters_cache
.. confval:: rgw_bucket_counters_cache_size

The following are notable ceph-exporter related settings can be set via ``ceph config set global CONFIG_VARIABLE VALUE``.

.. confval:: exporter_stats_period
