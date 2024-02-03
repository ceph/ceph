=======
Metrics
=======

The Ceph Object Gateway uses :ref:`Perf Counters` to track metrics. The counters can be labeled (:ref:`Labeled Perf Counters`). When counters are labeled, they are stored in the Ceph Object Gateway specific caches.

These metrics can be sent to the time series database Prometheus to visualize a cluster wide view of usage data (ex: number of S3 put operations on a specific bucket) over time.

.. contents::

Op Metrics
==========

The following metrics related to S3 or Swift operations are tracked per Ceph Object Gateway.

.. list-table:: Radosgw Op Metrics
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
     - Guage
     - Total latency of put operations
   * - get_obj_ops
     - Counter
     - Number of get operations
   * - get_obj_bytes
     - Counter
     - Number of bytes from get requests
   * - get_obj_lat
     - Guage
     - Total latency of get operations
   * - del_obj_ops
     - Counter
     - Number of delete object operations
   * - del_obj_bytes
     - Counter
     - Number of bytes deleted
   * - del_obj_lat
     - Guage
     - Total latency of delete object operations
   * - del_bucket_ops
     - Counter
     - Number of delete bucket operations
   * - del_bucket_lat
     - Guage
     - Total latency of delete bucket operations
   * - copy_obj_ops
     - Counter
     - Number of copy object operations
   * - copy_obj_bytes
     - Counter
     - Number of bytes copied
   * - copy_obj_lat
     - Guage
     - Total latency of copy object operations
   * - list_object_ops
     - Counter
     - Number of list object operations
   * - list_object_lat
     - Guage
     - Total latency of list object operations
   * - list_bucket_ops
     - Counter
     - Number of list bucket operations
   * - list_bucket_lat
     - Guage
     - Total latency of list bucket operations

More information about op metrics can be seen in the ``rgw_op`` section of the output of the ``counter schema`` command.
To view op metrics in the Ceph Object Gateway go to the ``rgw_op`` section of the output of the ``counter dump`` command::

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
                }
            }
        },
    ]

Op Metrics Labels
-----------------

Op metrics can also be tracked per-user or per-bucket. These metrics are exported to Prometheus with labels like Bucket = {name} or User = {userid}::

    "rgw_op": [
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
                }
            }
        },
        ...
    ]

:ref:`rgw-multitenancy` allows to use buckets and users of the same name simultaneously. If a user or bucket lies under a tenant, a label for tenant in the form  Tenant = {tenantid} is added to the metric.

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
#. Disk and memory usage of Promtheus. 

To help calculate the Ceph Object Gateway's memory usage of a cache, it should be noted that each cache entry, encompassing all of the op metrics, is 1360 bytes. This is an estimate and subject to change if metrics are added or removed from the op metrics list.

Sending Metrics to Prometheus
=============================

To get metrics from a Ceph Object Gateway into the time series database Prometheus, the ceph-exporter daemon must be running and configured to scrape the Radogw's admin socket.

The ceph-exporter daemon scrapes the Ceph Object Gateway's admin socket at a regular interval, defined by the config variable ``exporter_stats_period``.

Prometheus has a configurable interval in which it scrapes the exporter (see: https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

Config Reference
================
The following rgw op metrics related settings can be set via ``ceph config set client.rgw CONFIG_VARIABLE VALUE``.

.. confval:: rgw_user_counters_cache
.. confval:: rgw_user_counters_cache_size
.. confval:: rgw_bucket_counters_cache
.. confval:: rgw_bucket_counters_cache_size

The following are notable ceph-exporter related settings can be set via ``ceph config set global CONFIG_VARIABLE VALUE``.

.. confval:: exporter_stats_period
