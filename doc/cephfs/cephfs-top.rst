.. _cephfs-top:

==================
CephFS Top Utility
==================

CephFS provides `top(1)` like utility to display various Ceph Filesystem metrics
in realtime. `cephfs-top` is a curses based python script which makes use of `stats`
plugin in Ceph Manager to fetch (and display) metrics.

Manager Plugin
--------------

Ceph Filesystem clients periodically forward various metrics to Ceph Metadata Servers (MDS)
which in turn get forwarded to Ceph Manager by MDS rank zero. Each active MDS forward its
respective set of metrics to MDS rank zero. Metrics are aggregated and forwarded to Ceph
Manager.

Metrics are divided into two categories - global and per-mds. Global metrics represent
set of metrics for the filesystem as a whole (e.g., client read latency) whereas per-mds
metrics are for a particular MDS rank (e.g., number of subtrees handled by an MDS).

.. note:: Currently, only global metrics are tracked.

`stats` plugin is disabled by default and should be enabled via::

  $ ceph mgr module enable stats

Once enabled, Ceph Filesystem metrics can be fetched via::

  $ ceph fs perf stats
  {"version": 1, "global_counters": ["cap_hit", "read_latency", "write_latency", "metadata_latency", "dentry_lease", "opened_files", "pinned_icaps", "opened_inodes", "avg_read_latency", "stdev_read_latency", "avg_write_latency", "stdev_write_latency", "avg_metadata_latency", "stdev_metadata_latency"], "counters": [], "client_metadata": {"client.324130": {"IP": "192.168.1.100", "hostname": "ceph-host1", "root": "/", "mount_point": "/mnt/cephfs", "valid_metrics": ["cap_hit", "read_latency", "write_latency", "metadata_latency", "dentry_lease, "opened_files", "pinned_icaps", "opened_inodes", "avg_read_latency", "stdev_read_latency", "avg_write_latency", "stdev_write_latency", "avg_metadata_latency", "stdev_metadata_latency"]}}, "global_metrics": {"client.324130": [[309785, 1280], [0, 0], [197, 519015022], [88, 279074768], [12, 70147], [0, 3], [3, 3], [0, 3], [0, 0], [0, 0], [0, 11699223], [0, 88245], [0, 6596951], [0, 9539]]}, "metrics": {"delayed_ranks": [], "mds.0": {"client.324130": []}}}

Details of the JSON command output are as follows:

- `version`: Version of stats output
- `global_counters`: List of global performance metrics
- `counters`: List of per-mds performance metrics
- `client_metadata`: Ceph Filesystem client metadata
- `global_metrics`: Global performance counters
- `metrics`: Per-MDS performance counters (currently, empty) and delayed ranks

.. note:: `delayed_ranks` is the set of active MDS ranks that are reporting stale metrics.
          This can happen in cases such as (temporary) network issue between MDS rank zero
          and other active MDSs.

Metrics can be fetched for a particular client and/or for a set of active MDSs. To fetch metrics
for a particular client (e.g., for client-id: 1234)::

  $ ceph fs perf stats --client_id=1234

To fetch metrics only for a subset of active MDSs (e.g., MDS rank 1 and 2)::

  $ ceph fs perf stats --mds_rank=1,2

`cephfs-top`
------------

`cephfs-top` utility relies on `stats` plugin to fetch performance metrics and display in
`top(1)` like format. `cephfs-top` is available as part of `cephfs-top` package.

By default, `cephfs-top` uses `client.fstop` user to connect to a Ceph cluster::

  $ ceph auth get-or-create client.fstop mon 'allow r' mds 'allow r' osd 'allow r' mgr 'allow r'
  $ cephfs-top

To use a non-default user (other than `client.fstop`) use::

  $ cephfs-top --id <name>

By default, `cephfs-top` connects to cluster name `ceph`. To use a non-default cluster name::

  $ cephfs-top --cluster <cluster>

`cephfs-top` refreshes stats every second by default. To choose a different refresh interval use::

  $ cephfs-top -d <seconds>

Interval should be greater than or equal to 0.5 seconds. Fractional seconds are honoured.

Sample screenshot running `cephfs-top` with 2 clients:

.. image:: cephfs-top.png

.. note:: As of now, `cephfs-top` does not reliably work with multiple Ceph Filesystems.
