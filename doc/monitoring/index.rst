.. _monitoring:

===================
Monitoring overview
===================

This document explains the Ceph monitoring
stack and a number of important Ceph metrics.

Ceph admins can explore the rich observability stack deployed by Ceph, and
can leverage Prometheus, Alertmanager, Grafana, and scripting to create customized
monitoring tools.


Ceph Monitoring stack
=====================

Ceph deploys an integrated monitoring stack as described
in the :ref:`Monitoring Services <mgr-cephadm-monitoring>` section of
the ``cephadm`` documentation.  Deployments with external fleetwide monitoring
and observability systems using these or other tools may choose to disable
the stack that Ceph deploys by default.


Ceph metrics
============

Many Ceph metrics are gathered from the performance counters exposed by each
Ceph daemon. These :doc:`../dev/perf_counters` are native Ceph metrics.

Performance counters are rendered into standard Prometheus metrics by the
``ceph_exporter`` daemon. This daemon runs on every Ceph cluster host and exposes
an endpoint where performance counters exposed by Ceph
daemons running on that host are presented in the form of Prometheus metrics.

In addition to the ``ceph_exporter`` the Ceph Manager ``prometheus`` module
exposes metrics relating to the Ceph cluster as a  whole.

Ceph provides a Prometheus endpoint from which one can obtain the complete list
of available metrics, or against which admins, Grafana, and Alertmanager can exeute queries.

Prometheus (and related systems) accept data queries formatted as PromQL
expressions. Expansive documentation of PromQL can be
viewed [here](https://prometheus.io/docs/prometheus/latest/querying/basics/) and
several excellent books can be found at the usual sources of digital and print books.

We will explore a number of PromQL queries below. Use the following command
to obtain the Prometheus endpoint for your cluster:

Example:

.. code-block:: bash

  # ceph orch ps --service_name prometheus
  NAME                         HOST                          PORTS   STATUS          REFRESHED  AGE  MEM USE  MEM LIM  VERSION  IMAGE ID      CONTAINER ID
  prometheus.cephtest-node-00  cephtest-node-00.cephlab.com  *:9095  running (103m)    50s ago   5w     142M        -  2.33.4   514e6a882f6e  efe3cbc2e521

With this information you can connect to
``http://cephtest-node-00.cephlab.com:9095`` to access the Prometheus server
interface, which includes a list of targets, an expression browser, and metrics
related to the Prometheus service itself.

The complete list of metrics (with descriptions) is available at the URL of the below form:
in:

``http://cephtest-node-00.cephlab.com:9095/api/v1/targets/metadata``

The Ceph Dashboard provides a rich set of graphs and other panels that display the
most important cluster and service metrics.  Many of the examples in this document
are taken from Dashboard graphics or extrapolated from metrics exposed by the
Ceph Dashboard.

Ceph daemon health metrics
==========================

The ``ceph_exporter`` provides a metric named ``ceph_daemon_socket_up`` that
indicates the health status of a Ceph daemon based on its ability to respond
via the admin socket, where a value of ``1`` means healthy, and ``0`` means
unhealthy. Although a Ceph daemon might still be "alive" when it
reports ``ceph_daemon_socket_up=0``, this status indicates a significant issue
in its functionality. As such, this metric serves as an excellent means of
detecting problems in any of the main Ceph daemons.

The ``ceph_daemon_socket_up`` Prometheus metrics also have labels as described below:
* ``ceph_daemon``: Identifier of the Ceph daemon exposing an admin socket on the host.
* ``hostname``: Name of the host where the Ceph daemon is running.

Example:

.. code-block:: bash

   ceph_daemon_socket_up{ceph_daemon="mds.a",hostname="testhost"} 1
   ceph_daemon_socket_up{ceph_daemon="osd.1",hostname="testhost"} 0

To identify any Ceph daemons that were not responsive at any point in the last
12 hours, you can use the following PromQL expression:

.. code-block:: bash

   ceph_daemon_socket_up == 0 or min_over_time(ceph_daemon_socket_up[12h]) == 0


Performance metrics
===================

Below we explore a a number of metrics that indicate Ceph cluster performance.

All of these metrics have the following labels:
* ``ceph_daemon``: Identifier of the Ceph daemon from which the metric was harvested
* ``instance``: The IP address of the exporter instance exposing the metric.
* ``job``: Prometheus scrape job name

Below is an example Prometheus query result showing these labels:

.. code-block:: bash

  ceph_osd_op_r{ceph_daemon="osd.0", instance="192.168.122.7:9283", job="ceph"} = 73981

*Cluster throughput:*
Query ``ceph_osd_op_r_out_bytes`` and ``ceph_osd_op_w_in_bytes`` to obtain cluster client throughput:

Example:

.. code-block:: bash

  # Writes (B/s):
  sum(irate(ceph_osd_op_w_in_bytes[1m]))

  # Reads (B/s):
  sum(irate(ceph_osd_op_r_out_bytes[1m]))


*Cluster I/O (operations):*
Query ``ceph_osd_op_r``, ``ceph_osd_op_w`` to obtain the rates of client operations (IOPS):

Example:

.. code-block:: bash

  # Writes (ops/s):
  sum(irate(ceph_osd_op_w[1m]))

  # Reads (ops/s):
  sum(irate(ceph_osd_op_r[1m]))

*Latency:*
Query ``ceph_osd_op_latency_sum`` to measure the delay before OSD transfers of data
begins in respose to client requests:

Example:

.. code-block:: bash

  sum(irate(ceph_osd_op_latency_sum[1m]))


OSD performance
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

  # OSD 0 write thughtput (bytes)
  irate(ceph_osd_op_w_in_bytes{ceph_daemon=~"osd.0"}[1m])

  # OSD.0 total raw capacity available
  ceph_osd_stat_bytes{ceph_daemon="osd.0", instance="cephtest-node-00.cephlab.com:9283", job="ceph"} = 536451481


Physical storage drive performance:
===================================

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

Pool metrics
============

Ceph pool metrics have the following labels:

* ``instance``: The IP address of the exporter providing the metric
* ``pool_id``: Numeric identifier of the Ceph pool
* ``job``: Prometheus scrape job name


Pool-specific metrics include:

   * ``ceph_pool_metadata``: Information about the pool that can be used
     together with other metrics to provide more information in query resultss
     and graphs.  In addition to the above three common labels this metric
     provides the following:

    * ``compression_mode``: Compression type enabled for the pool. Values are
      ``lz4``, ``snappy``, ``zlib``, ``zstd``, and ``none`). Example:
      ``compression_mode="none"``

    * ``description``: Brief description of the pool data protection strategy
      including replica number or EC profile. Example:
      ``description="replica:3"``

    * ``name``: Name of the pool. Example: ``name=".mgr"``

    * ``type``: Data protection strategy, replicated or EC. ``Example:
      type="replicated"``

    * ``ceph_pool_bytes_used``: Total raw capacity (after replication or EC)
      consumed by user data and metadata

    * ``ceph_pool_stored``: Total client data stored in the pool (before data
      protection)

    * ``ceph_pool_compress_under_bytes``: Data eligible to be compressed in
      the pool

    * ``ceph_pool_compress_bytes_used``:  Data compressed in the pool

    * ``ceph_pool_rd``: Client read operations per pool (reads per second)

    * ``ceph_pool_rd_bytes``: Client read operations in bytes per pool

    * ``ceph_pool_wr``: Client write operations per pool (writes per second)

    * ``ceph_pool_wr_bytes``: Client write operation in bytes per pool


**Useful queries**:

.. code-block:: bash

  # Total raw capacity available in the cluster:
  sum(ceph_osd_stat_bytes)

  # Total raw capacity consumed in the cluster (including metadata + redundancy):
  sum(ceph_pool_bytes_used)

  # Total client data stored in the cluster:
  sum(ceph_pool_stored)

  # Compression savings:
  sum(ceph_pool_compress_under_bytes - ceph_pool_compress_bytes_used)

  # Client IOPS for a specific pool
  reads: irate(ceph_pool_rd[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"testrbdpool"}
  writes: irate(ceph_pool_wr[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"testrbdpool"}

  # Client throughput for a specific pool
  reads: irate(ceph_pool_rd_bytes[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"testrbdpool"}
  writes: irate(ceph_pool_wr_bytes[1m]) * on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"testrbdpool"}

RGW metrics
==================

These metrics have the following labels:

* ``instance``: The IP address of the exporter providing the metric
* ``instance_id``: Identifier of the RGW daemon instance
* ``job``: Orometheus scrape job name

Example:

.. code-block:: bash

  ceph_rgw_req{instance="192.168.122.7:9283", instance_id="154247", job="ceph"} = 12345


Generic metrics
---------------

* ``ceph_rgw_metadata``: Provides generic information about an RGW daemon.
  This can be used together with other metrics to provide contextual
  information in queries and graphs. In addtion to the three common labels,
  this metric provides the following:

  * ``ceph_daemon``: Name of the RGW daemon instance. Example:
    ``ceph_daemon="rgw.rgwtest.cephtest-node-00.sxizyq"``

  * ``ceph_version``: Version of the RGW daemon. Example: ``ceph_version="ceph
    version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy
    (stable)"``

  * ``hostname``: Name of the host where the daemon runs. Example:
    ``hostname:"cephtest-node-00.cephlab.com"``

  * ``ceph_rgw_req``: Number of requests processed by the daemon
    (``GET``+``PUT``+``DELETE``).  Useful for detecting bottlenecks and
    optimizing load distribution.

  * ``ceph_rgw_qlen``: Operations queue length for the daemon.  Useful for
    detecting bottlenecks and optimizing load distribution.

  * ``ceph_rgw_failed_req``: Aborted requests.  Useful for detecting daemon
    errors.


GET operation metrics
---------------------
* ``ceph_rgw_op_global_get_obj_lat_count``: Number of ``GET`` requests

* ``ceph_rgw_op_global_get_obj_lat_sum``: Total latency for ``GET`` requests

* ``ceph_rgw_op_global_get_obj_ops``: Total number of ``GET`` requests

* ``ceph_rgw_op_global_get_obj_bytes``: Total bytes transferred for ``GET`` requests


PUT operation metrics
-------------------------------
* ``ceph_rgw_op_global_put_obj_lat_count``: Number of get operations

* ``ceph_rgw_op_global_put_obj_lat_sum``: Total latency time for ``PUT`` operations

* ``ceph_rgw_op_global_put_obj_ops``: Total number of ``PUT`` operations

* ``ceph_rgw_op_global_get_obj_bytes``: Total bytes transferred in ``PUT`` operations


Additional Useful queries
-------------------------

.. code-block:: bash

  # Average GET latency
  rate(ceph_rgw_op_global_get_obj_lat_sum[30s]) / rate(ceph_rgw_op_global_get_obj_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # Average PUT latency
  rate(ceph_rgw_op_global_put_obj_lat_sum[30s]) / rate(ceph_rgw_op_global_put_obj_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # Requests per second
  rate(ceph_rgw_req[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # Total number of "other" operations (``LIST``, ``DELETE``, etc)
  rate(ceph_rgw_req[30s]) -  (rate(ceph_rgw_op_global_get_obj_ops[30s]) + rate(ceph_rgw_op_global_put_obj_ops[30s]))

  # GET latency per RGW instance
  rate(ceph_rgw_op_global_get_obj_lat_sum[30s]) /  rate(ceph_rgw_op_global_get_obj_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # PUT latency per RGW instance
  rate(ceph_rgw_op_global_put_obj_lat_sum[30s]) /  rate(ceph_rgw_op_global_put_obj_lat_count[30s]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # Bandwidth consumed by GET operations
  sum(rate(ceph_rgw_op_global_get_obj_bytes[30s]))

  # Bandwidth consumed by PUT operations
  sum(rate(ceph_rgw_op_global_put_obj_bytes[30s]))

  # Bandwidth consumed by RGW instance (PUTs + GETs)
  sum by (instance_id) (rate(ceph_rgw_op_global_get_obj_bytes[30s]) + rate(ceph_rgw_op_global_put_obj_bytes[30s])) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata

  # HTTP errors and other request failures
  rate(ceph_rgw_failed_req[30s])


CephFS Metrics
==============

These metrics have the following labels:

* ``ceph_daemon``: The name of the MDS daemon
* ``instance``: The IP address and port of the exporter exposing the metric
* ``job``: Prometheus scrape job name

Example:

.. code-block:: bash

  ceph_mds_request{ceph_daemon="mds.test.cephtest-node-00.hmhsoh", instance="192.168.122.7:9283", job="ceph"} = 1452


Important metrics
-----------------

* ``ceph_mds_metadata``: Provides general information about the MDS daemon.  It
  can be used together with other metrics to provide contextual
  information in queries and graphs.  The following extra labels are populated:

    * ``ceph_version``: MDS daemon version
    * ``fs_id``: CephFS filesystem ID
    * ``hostname``: Name of the host where the MDS daemon runs
    * ``public_addr``: Public address of the host where the MDS daemon runs
    * ``rank``: Rank of the MDS daemon

Example:

.. code-block:: bash

 ceph_mds_metadata{ceph_daemon="mds.test.cephtest-node-00.hmhsoh", ceph_version="ceph version 17.2.6 (d7ff0d10654d2280e08f1ab989c7cdf3064446a5) quincy (stable)", fs_id="-1", hostname="cephtest-node-00.cephlab.com", instance="cephtest-node-00.cephlab.com:9283", job="ceph", public_addr="192.168.122.145:6801/118896446", rank="-1"}


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

* ``ceph_mds_root_rbytes``: Total number of bytes managed by the daemon

* ``ceph_mds_root_rfiles``: Total number of files managed by the daemon


Useful queries:
---------------

.. code-block:: bash

  # Total MDS read workload:
  sum(rate(ceph_objecter_op_r[1m]))

  # Total MDS daemons workload:
  sum(rate(ceph_objecter_op_w[1m]))

  # Read workload for a specific MDS
  sum(rate(ceph_objecter_op_r{ceph_daemon=~"mdstest"}[1m]))

  # Write workload for a specific MDS
  sum(rate(ceph_objecter_op_r{ceph_daemon=~"mdstest"}[1m]))

  # Average reply latency
  rate(ceph_mds_reply_latency_sum[30s]) / rate(ceph_mds_reply_latency_count[30s])

  # Total requests per second
  rate(ceph_mds_request[30s]) * on (instance) group_right (ceph_daemon) ceph_mds_metadata


Block metrics
=============

By default RBD metrics for images are not gathered, as their cardinality may
be high.  This helps ensure the performance of the Manager's ``prometheus`` module.

To produce metrics for RBD images, configure the
Manager option ``mgr/prometheus/rbd_stats_pools``. For more information
see :ref:`prometheus-rbd-io-statistics`

These metrics have the following labels:

* ``image``: Name of the image (volume)
* ``instance``: Node where the exporter runs
* ``job``: Name of the Prometheus scrape job
* ``pool``: RBD pool name

Example:

.. code-block:: bash

  ceph_rbd_read_bytes{image="test2", instance="cephtest-node-00.cephlab.com:9283", job="ceph", pool="testrbdpool"}


Important  metrics
------------------

* ``ceph_rbd_read_bytes``: RBD bytes read

* ``ceph_rbd_write_bytes``: RBD image bytes written

* ``ceph_rbd_read_latency_count``: RBD read operation latency count

* ``ceph_rbd_read_latency_sum``: RBD read operation latency total time

* ``ceph_rbd_read_ops``: RBD read operation count

* ``ceph_rbd_write_ops``: RBD write operation count

* ``ceph_rbd_write_latency_count``: RBD write operation latency count

* ``ceph_rbd_write_latency_sum``: RBD write operation latency total



Useful queries
--------------

.. code-block:: bash

  # Average read latency
  rate(ceph_rbd_read_latency_sum[30s]) / rate(ceph_rbd_read_latency_count[30s]) * on (instance) group_left (ceph_daemon) ceph_rgw_metadata


Hardware monitoring
===================

See :ref:`hardware-monitoring`

