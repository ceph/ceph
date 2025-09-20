Insights Module
===============

The ``insights`` module collects and exposes system information to the
Insights Core data analysis framework. It is intended to replace explicit
interrogation of Ceph CLIs and daemon admin sockets, reducing the API surface
that Insights depends on. The insights reports contain the following:

* **Health reports**. In addition to reporting the current health of the
  cluster, the insights module reports a summary of the last 24 hours of
  health checks. This feature is important for catching cluster health issues
  that are transient and may not be present at the moment the report is
  generated. Health checks are deduplicated to avoid unbounded data growth.

* **Crash reports**. A summary of any daemon crashes in the past 24 hours is
  included in the insights report. Crashes are reported as the number of
  crashes per daemon type (e.g. ``ceph-osd``) within the time window. Full
  details of a crash may be obtained using the `crash module`_.

* Software version, storage utilization, cluster maps, placement group
  summary, monitor status, cluster configuration, and OSD metadata.

Enabling
--------

To enable the *insights* module, run the following command:

.. prompt:: bash #

   ceph mgr module enable insights

Commands
--------

To generate a full report, run the following command:

.. prompt:: bash #

   ceph insights

To remove historical health data older than ``<hours>``, run a command of the
following form. Passing ``0`` for ``<hours>`` will clear all health data.

.. prompt:: bash #

   ceph insights prune-health <hours>

The ``prune-health`` subcommand is useful for cleaning the health history
before automated nightly reports are generated. Unpruned health histories may
contain spurious health checks accumulated while performing system maintenance
and other health checks that have been resolved. There is no need to prune
health data to reclaim storage space; garbage collection is performed
regularly to remove old health data from persistent storage.

.. _crash module: ../crash
