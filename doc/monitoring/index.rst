.. _monitoring:

===================
Monitoring Overview
===================

This document explains the Ceph monitoring
stack and a number of important Ceph metrics.

Ceph admins can explore the rich observability stack deployed by Ceph, and
can leverage Prometheus, Alertmanager, Grafana, and scripting to create customized
monitoring tools.


Ceph Monitoring Stack
=====================

Ceph deploys an integrated monitoring stack as described
in the :ref:`Monitoring Services <mgr-cephadm-monitoring>` section of
the ``cephadm`` documentation.  Deployments with external fleet-wide monitoring
and observability systems using these or other tools may choose to disable
the stack that Ceph deploys by default.


Ceph Metrics
============

Many Ceph metrics are gathered from the performance counters exposed by each
Ceph daemon. These :ref:`Perf Counters` are native Ceph metrics.

Performance counters are rendered into standard Prometheus metrics by the
``ceph-exporter`` daemon. cephadm runs this daemon on every host. It exposes
an endpoint that presents, as Prometheus metrics, the performance counters of
the Ceph daemons on that host, except Manager daemons.

In addition to ``ceph-exporter``, the Ceph Manager ``prometheus`` module
exposes metrics about the Ceph cluster as a whole.

Ceph provides a Prometheus endpoint from which you can obtain the complete list
of available metrics and against which administrators and Grafana can run
queries. Prometheus also evaluates the alerting rules and sends the resulting
alerts to Alertmanager.

Prometheus (and related systems) accept data queries formatted as PromQL
expressions. Expansive documentation of PromQL can be
viewed in the `PromQL documentation`_ and
several excellent books can be found at the usual sources of digital and print books.

We will explore a number of PromQL queries below. Use the following command
to obtain the Prometheus endpoint for your cluster:

Example:

.. prompt:: bash # auto

  # ceph orch ps --service_name prometheus
  NAME                         HOST                          PORTS   STATUS          REFRESHED  AGE  MEM USE  MEM LIM  VERSION  IMAGE ID      CONTAINER ID
  prometheus.cephtest-node-00  cephtest-node-00.cephlab.com  *:9095  running (103m)    50s ago   5w     142M        -  3.6.0    514e6a882f6e  efe3cbc2e521

With this information you can connect to
``http://cephtest-node-00.cephlab.com:9095`` to access the Prometheus server
interface, which includes a list of targets, an expression browser, and metrics
related to the Prometheus service itself.

The complete list of metrics (with descriptions) is available at the URL of the below form:

::

  http://cephtest-node-00.cephlab.com:9095/api/v1/targets/metadata

The Ceph Dashboard provides a rich set of graphs and other panels that display the
most important cluster and service metrics.  Many of the examples in this document
are taken from Dashboard graphics or extrapolated from metrics exposed by the
Ceph Dashboard.


Ceph Daemon Health Metrics
==========================

In Tentacle 20.2 and later releases, the ``ceph-exporter`` daemon provides a
metric named ``ceph_daemon_socket_up`` that shows whether a Ceph daemon
responds on its admin socket: ``1`` means healthy and ``0`` means unhealthy.
Although a Ceph daemon might still be "alive" when it
reports ``ceph_daemon_socket_up=0``, this status indicates a significant issue
in its functionality. As such, this metric serves as an excellent means of
detecting problems in any of the main Ceph daemons. Manager daemons are not
covered: ``ceph-exporter`` does not read their admin sockets.

The ``ceph_daemon_socket_up`` Prometheus metrics also have labels as described below:

* ``ceph_daemon``: The name of the daemon's admin socket file without the
  ``.asok`` suffix. With the default ``admin_socket`` setting this is
  ``<cluster>-<daemon name>``, for example ``ceph-osd.1``; it does not match
  the ``ceph_daemon`` label of performance counter metrics (``osd.1``).
* ``hostname``: Name of the host where the Ceph daemon is running.

Example:

.. code-block:: bash

   ceph_daemon_socket_up{ceph_daemon="ceph-mds.a",hostname="testhost"} 1
   ceph_daemon_socket_up{ceph_daemon="ceph-osd.1",hostname="testhost"} 0

To identify any Ceph daemons that were not responsive at any point in the last
12 hours, you can use the following PromQL expression:

.. code-block:: bash

   ceph_daemon_socket_up == 0 or min_over_time(ceph_daemon_socket_up[12h]) == 0


Performance Metrics
===================

Below we explore a number of metrics that indicate Ceph cluster performance.

All of these metrics have the following labels:

* ``ceph_daemon``: Identifier of the Ceph daemon from which the metric was harvested
* ``instance``: The scrape target of the ``ceph-exporter`` that exposes the
  metric. In a cephadm deployment, this is the name of the host.
* ``job``: Prometheus scrape job name (``ceph-exporter`` in a cephadm
  deployment)

Below is an example Prometheus query result showing these labels:

.. code-block:: bash

  ceph_osd_op_r{ceph_daemon="osd.0", instance="cephtest-node-00.cephlab.com", job="ceph-exporter"} = 73981

*Cluster throughput:*
Query ``ceph_osd_op_r_out_bytes`` and ``ceph_osd_op_w_in_bytes`` to obtain cluster client throughput:

Example:

.. code-block:: bash

  # Writes (B/s)
  sum(irate(ceph_osd_op_w_in_bytes[1m]))

  # Reads (B/s)
  sum(irate(ceph_osd_op_r_out_bytes[1m]))


*Cluster I/O (operations):*
Query ``ceph_osd_op_r``, ``ceph_osd_op_w`` to obtain the rates of client operations (IOPS):

Example:

.. code-block:: bash

  # Writes (ops/s)
  sum(irate(ceph_osd_op_w[1m]))

  # Reads (ops/s)
  sum(irate(ceph_osd_op_r[1m]))

*Latency:*
Divide the rate of ``ceph_osd_op_latency_sum`` by the rate of
``ceph_osd_op_latency_count`` to obtain the average latency of client
operations, from the time an OSD receives a request until the operation
completes, including queue time:

Example:

.. code-block:: bash

  # Average client operation latency in seconds, across all OSDs
  sum(rate(ceph_osd_op_latency_sum[1m])) / sum(rate(ceph_osd_op_latency_count[1m]))


OSD Performance
===============

The cluster performance metrics described above are gathered from OSD metrics.
By specifying an appropriate label value or regular expression we can retrieve
performance metrics for one or a subset of the cluster's OSDs:

Examples:

.. code-block:: bash

  # OSD 0 read latency
  irate(ceph_osd_op_r_latency_sum{ceph_daemon=~"osd.0"}[1m]) / on (ceph_daemon) irate(ceph_osd_op_r_latency_count[1m])

  # OSD 0 write IOPS
  irate(ceph_osd_op_w{ceph_daemon=~"osd.0"}[1m])

  # OSD 0 write throughput (bytes)
  irate(ceph_osd_op_w_in_bytes{ceph_daemon=~"osd.0"}[1m])

  # Raw capacity (size) of osd.0
  ceph_osd_stat_bytes{ceph_daemon="osd.0", instance="cephtest-node-00.cephlab.com", job="ceph-exporter"} = 536451481


Physical Storage Drive Performance
==================================

By combining Prometheus ``node_exporter`` metrics with Ceph cluster metrics we can
derive performance information for physical storage media backing Ceph OSDs.

Example:

.. code-block:: bash

  # Read latency of device used by osd.0
  label_replace(irate(node_disk_read_time_seconds_total[1m]) / irate(node_disk_reads_completed_total[1m]), "instance", "$1", "instance", "([^:.]*).*") and on (instance, device) label_replace(label_replace(ceph_disk_occupation_human{ceph_daemon=~"osd.0"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")

  # Write latency of device used by osd.0
  label_replace(irate(node_disk_write_time_seconds_total[1m]) / irate(node_disk_writes_completed_total[1m]), "instance", "$1", "instance", "([^:.]*).*") and on (instance, device) label_replace(label_replace(ceph_disk_occupation_human{ceph_daemon=~"osd.0"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")

  # IOPS of device used by osd.0
  # reads:
  label_replace(irate(node_disk_reads_completed_total[1m]), "instance", "$1", "instance", "([^:.]*).*") and on (instance, device) label_replace(label_replace(ceph_disk_occupation_human{ceph_daemon=~"osd.0"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")

  # writes:
  label_replace(irate(node_disk_writes_completed_total[1m]), "instance", "$1", "instance", "([^:.]*).*") and on (instance, device) label_replace(label_replace(ceph_disk_occupation_human{ceph_daemon=~"osd.0"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")

  # Throughput for device used by osd.0
  # reads:
  label_replace(irate(node_disk_read_bytes_total[1m]), "instance", "$1", "instance", "([^:.]*).*") and on (instance, device) label_replace(label_replace(ceph_disk_occupation_human{ceph_daemon=~"osd.0"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")

  # writes:
  label_replace(irate(node_disk_written_bytes_total[1m]), "instance", "$1", "instance", "([^:.]*).*") and on (instance, device) label_replace(label_replace(ceph_disk_occupation_human{ceph_daemon=~"osd.0"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")

  # Physical drive utilization (%) for osd.0 in the last 5 minutes. Note that this value has limited mean for SSDs
  label_replace(irate(node_disk_io_time_seconds_total[5m]), "instance", "$1", "instance", "([^:.]*).*") and on (instance, device) label_replace(label_replace(ceph_disk_occupation_human{ceph_daemon=~"osd.0"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")


Pool Metrics
============

Ceph pool metrics have the following labels:

* ``instance``: The scrape target of the Manager ``prometheus`` module. In a
  cephadm deployment, this is the fixed value ``ceph_cluster``, so it does not
  change when another Manager becomes active.
* ``pool_id``: Numeric identifier of the Ceph pool
* ``job``: Prometheus scrape job name


Pool-specific metrics include:

* ``ceph_pool_metadata``: Information about the pool that can be used
  together with other metrics to provide more information in query results
  and graphs.  In addition to the above three common labels, this metric
  provides the following:

  * ``application``: In Tentacle 20.2 and later releases, the applications
    enabled on the pool, separated by a comma and a space: ``Block`` (rbd),
    ``Filesystem`` (cephfs), ``Object`` (rgw), or the application name.
    Example: ``application="Block"``
  * ``compression_mode``: Compression mode set on the pool: ``none``,
    ``passive``, ``aggressive`` or ``force``. The value is ``none`` when
    the pool has no ``compression_mode`` setting; the OSD option
    ``bluestore_compression_mode`` then applies. Example:
    ``compression_mode="none"``
  * ``description``: Brief description of the pool data protection strategy
    including replica number or EC profile. Example:
    ``description="replica:3"``
  * ``name``: Name of the pool. Example: ``name=".mgr"``
  * ``type``: Data protection strategy: ``replicated`` or ``erasure``.
    Example: ``type="replicated"``

* ``ceph_pool_bytes_used``: Total raw capacity (after replication or EC)
  consumed by user data and metadata
* ``ceph_pool_stored``: Total client data stored in the pool (before data
  protection)
* ``ceph_pool_compress_under_bytes``: Amount of data in the pool that has
  passed through compression and is stored in compressed form, measured
  before compression (summed over all replicas)
* ``ceph_pool_compress_bytes_used``: Space allocated for compressed data in
  the pool, including replication and allocation overhead
* ``ceph_pool_rd``: Client read operations on the pool, as a running total.
  Use ``rate()`` or ``irate()`` for reads per second.
* ``ceph_pool_rd_bytes``: Bytes read by clients from the pool, as a running
  total
* ``ceph_pool_wr``: Client write operations on the pool, as a running
  total. Use ``rate()`` or ``irate()`` for writes per second.
* ``ceph_pool_wr_bytes``: Bytes written by clients to the pool, as a running
  total


Useful Queries
--------------

.. code-block:: bash

  # Total raw capacity of all OSDs in the cluster
  sum(ceph_osd_stat_bytes)

  # Raw capacity still free in the cluster
  sum(ceph_osd_stat_bytes) - sum(ceph_osd_stat_bytes_used)

  # Total raw capacity consumed in the cluster (including metadata + redundancy)
  sum(ceph_pool_bytes_used)

  # Total client data stored in the cluster
  sum(ceph_pool_stored)

  # Compression savings
  sum(ceph_pool_compress_under_bytes - ceph_pool_compress_bytes_used)

  # Client IOPS for a specific pool
  # reads:
  irate(ceph_pool_rd[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"testrbdpool"}
  # writes:
  irate(ceph_pool_wr[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"testrbdpool"}

  # Client throughput for a specific pool
  # reads:
  irate(ceph_pool_rd_bytes[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"testrbdpool"}
  # writes:
  irate(ceph_pool_wr_bytes[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"testrbdpool"}


RGW Metrics
===========

These metrics have the following labels:

* ``instance``: The scrape target of the ``ceph-exporter`` that exposes the
  metric. In a cephadm deployment, this is the name of the host.
  ``ceph_rgw_metadata`` comes from the Manager ``prometheus`` module instead,
  so in a cephadm deployment its ``instance`` label is ``ceph_cluster``.
* ``instance_id``: Identifier of the RGW daemon instance
* ``job``: Prometheus scrape job name

Example:

.. code-block:: bash

  ceph_rgw_req{instance="cephtest-node-00.cephlab.com", instance_id="sxizyq", job="ceph-exporter"} = 12345


Generic Metrics
---------------

* ``ceph_rgw_metadata``: Provides generic information about an RGW daemon.
  This can be used together with other metrics to provide contextual
  information in query results and graphs. In addition to the three common labels,
  this metric provides the following:

  * ``ceph_daemon``: Name of the RGW daemon instance. Example:
    ``ceph_daemon="rgw.rgwtest.cephtest-node-00.sxizyq"``
  * ``ceph_version``: Version of the RGW daemon. Example:
    ``ceph_version="ceph version 20.2.4 (<sha1>) tentacle (stable)"``
  * ``hostname``: Name of the host where the daemon runs. Example:
    ``hostname="cephtest-node-00.cephlab.com"``
  * ``zone_id``: ID of the RGW zone the daemon serves. Matches the ``zone``
    label of the ``ceph_rgw_bucket_usage_*`` metrics. Example:
    ``zone_id="e2471ec1-6da5-4aaa-be5a-1f25f0929acc"``

* ``ceph_rgw_req``: Number of requests the daemon has started, of every
  type, as a running total. Useful for detecting bottlenecks and optimizing
  load distribution.
* ``ceph_rgw_qlen``: Operations queue length for the daemon. Useful for
  detecting bottlenecks and optimizing load distribution.
* ``ceph_rgw_failed_req``: Aborted requests. Useful for detecting daemon
  errors.


GET Operation Metrics
---------------------

* ``ceph_rgw_op_get_obj_lat_count``: Number of ``GET`` requests
* ``ceph_rgw_op_get_obj_lat_sum``: Total latency for ``GET`` requests
* ``ceph_rgw_op_get_obj_ops``: Total number of ``GET`` requests
* ``ceph_rgw_op_get_obj_bytes``: Total bytes transferred for ``GET`` requests


PUT Operation Metrics
---------------------

* ``ceph_rgw_op_put_obj_lat_count``: Number of ``PUT`` operations
* ``ceph_rgw_op_put_obj_lat_sum``: Total latency time for ``PUT`` operations
* ``ceph_rgw_op_put_obj_ops``: Total number of ``PUT`` operations
* ``ceph_rgw_op_put_obj_bytes``: Total bytes transferred in ``PUT`` operations


Additional Useful Queries
-------------------------

.. code-block:: bash

  # Average GET latency
  rate(ceph_rgw_op_get_obj_lat_sum[30s]) / rate(ceph_rgw_op_get_obj_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # Average PUT latency
  rate(ceph_rgw_op_put_obj_lat_sum[30s]) / rate(ceph_rgw_op_put_obj_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # Requests per second
  rate(ceph_rgw_req[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # Total number of other operations (LIST, DELETE, and so on)
  rate(ceph_rgw_req[30s]) -  (rate(ceph_rgw_op_get_obj_ops[30s]) + rate(ceph_rgw_op_put_obj_ops[30s]))

  # GET latency per RGW instance
  rate(ceph_rgw_op_get_obj_lat_sum[30s]) /  rate(ceph_rgw_op_get_obj_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # PUT latency per RGW instance
  rate(ceph_rgw_op_put_obj_lat_sum[30s]) /  rate(ceph_rgw_op_put_obj_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # Bandwidth consumed by GET operations
  sum(rate(ceph_rgw_op_get_obj_bytes[30s]))

  # Bandwidth consumed by PUT operations
  sum(rate(ceph_rgw_op_put_obj_bytes[30s]))

  # Bandwidth consumed by RGW instance (PUTs + GETs)
  sum by (instance_id) (rate(ceph_rgw_op_get_obj_bytes[30s]) + rate(ceph_rgw_op_put_obj_bytes[30s])) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # HTTP errors and other request failures
  rate(ceph_rgw_failed_req[30s])


CephFS Metrics
==============

These metrics have the following labels:

* ``ceph_daemon``: The name of the MDS daemon
* ``instance``: The scrape target of the ``ceph-exporter`` that exposes the
  metric. In a cephadm deployment, this is the name of the host.
  ``ceph_mds_metadata`` comes from the Manager ``prometheus`` module instead,
  so in a cephadm deployment its ``instance`` label is ``ceph_cluster``.
* ``job``: Prometheus scrape job name

Example:

.. code-block:: bash

  ceph_mds_request{ceph_daemon="mds.test.cephtest-node-00.hmhsoh", instance="cephtest-node-00.cephlab.com", job="ceph-exporter"} = 1452


Important Metrics
-----------------

* ``ceph_mds_metadata``: Provides general information about the MDS daemon.  It
  can be used together with other metrics to provide contextual
  information in query results and graphs.  The following extra labels are populated:

  * ``ceph_version``: MDS daemon version
  * ``fs_id``: CephFS filesystem ID
  * ``hostname``: Name of the host where the MDS daemon runs
  * ``public_addr``: Public address of the host where the MDS daemon runs
  * ``rank``: Rank of the MDS daemon

Example:

.. code-block:: bash

 ceph_mds_metadata{ceph_daemon="mds.test.cephtest-node-00.hmhsoh", ceph_version="ceph version 20.2.4 (<sha1>) tentacle (stable)", fs_id="-1", hostname="cephtest-node-00.cephlab.com", instance="ceph_cluster", job="ceph", public_addr="192.168.122.145:6801/118896446", rank="-1"}

* ``ceph_mds_request``: Total number of requests for the MDS
* ``ceph_mds_reply_latency_sum``: Reply latency total
* ``ceph_mds_reply_latency_count``: Reply latency count
* ``ceph_mds_server_handle_client_request``: Number of client requests
* ``ceph_mds_sessions_session_count``: Session count
* ``ceph_mds_sessions_total_load``: Total load
* ``ceph_mds_sessions_sessions_open``: Sessions currently open
* ``ceph_mds_sessions_sessions_stale``: Sessions currently stale
* ``ceph_objecter_op_r``: Number of read operations
* ``ceph_objecter_op_w``: Number of write operations
* ``ceph_mds_root_rbytes``: Total size in bytes of all files in the file
  system, from the recursive statistics of the root directory as known to
  this MDS
* ``ceph_mds_root_rfiles``: Total number of files in the file system, from
  the recursive statistics of the root directory as known to this MDS


Useful Queries
--------------

.. code-block:: bash

  # Total MDS read workload
  sum(rate(ceph_objecter_op_r{ceph_daemon=~"mds.*"}[1m]))

  # Total MDS write workload
  sum(rate(ceph_objecter_op_w{ceph_daemon=~"mds.*"}[1m]))

  # Read workload for a specific MDS
  sum(rate(ceph_objecter_op_r{ceph_daemon="mds.test.cephtest-node-00.hmhsoh"}[1m]))

  # Write workload for a specific MDS
  sum(rate(ceph_objecter_op_w{ceph_daemon="mds.test.cephtest-node-00.hmhsoh"}[1m]))

  # Average reply latency
  rate(ceph_mds_reply_latency_sum[30s]) / rate(ceph_mds_reply_latency_count[30s])

  # Requests per second for each MDS daemon, with its host name and rank
  rate(ceph_mds_request[30s]) * on (ceph_daemon) group_left (hostname, rank) ceph_mds_metadata


Block Metrics
=============

By default RBD metrics for images are not gathered, as their cardinality may
be high.  This helps ensure the performance of the Manager's ``prometheus`` module.

To produce metrics for RBD images, configure the
Manager option :confval:`mgr/prometheus/rbd_stats_pools`. For more information,
see :ref:`prometheus-rbd-io-statistics`.

These metrics have the following labels:

* ``image``: Name of the image (volume)
* ``instance``: The scrape target of the Manager ``prometheus`` module. In a
  cephadm deployment, this is the fixed value ``ceph_cluster``, so it does not
  change when another Manager becomes active.
* ``job``: Name of the Prometheus scrape job
* ``namespace``: RBD namespace of the image (absent for the default namespace)
* ``pool``: RBD pool name

Example:

.. code-block:: bash

  ceph_rbd_read_bytes{image="test2", instance="ceph_cluster", job="ceph", pool="testrbdpool"}


Important Metrics
-----------------

* ``ceph_rbd_read_bytes``: RBD bytes read
* ``ceph_rbd_write_bytes``: RBD image bytes written
* ``ceph_rbd_read_latency_count``: RBD read operation latency count
* ``ceph_rbd_read_latency_sum``: RBD read operation latency total time
* ``ceph_rbd_read_ops``: RBD read operation count
* ``ceph_rbd_write_ops``: RBD write operation count
* ``ceph_rbd_write_latency_count``: RBD write operation latency count
* ``ceph_rbd_write_latency_sum``: RBD write operation latency total


Useful Queries
--------------

.. code-block:: bash

  # Average read latency of each image, in nanoseconds
  rate(ceph_rbd_read_latency_sum[30s]) / rate(ceph_rbd_read_latency_count[30s])


Hardware Monitoring
===================

See :ref:`hardware-monitoring`.

.. _PromQL documentation: https://prometheus.io/docs/prometheus/latest/querying/basics/

