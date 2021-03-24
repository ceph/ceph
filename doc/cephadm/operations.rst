==================
Cephadm Operations
==================

Watching cephadm log messages
=============================

Cephadm logs to the ``cephadm`` cluster log channel, meaning you can
monitor progress in realtime with::

  # ceph -W cephadm

By default it will show info-level events and above.  To see
debug-level messages too::

  # ceph config set mgr mgr/cephadm/log_to_cluster_level debug
  # ceph -W cephadm --watch-debug

Be careful: the debug messages are very verbose!

You can see recent events with::

  # ceph log last cephadm

These events are also logged to the ``ceph.cephadm.log`` file on
monitor hosts and to the monitor daemons' stderr.


.. _cephadm-logs:

Ceph daemon logs
================

Logging to stdout
-----------------

Traditionally, Ceph daemons have logged to ``/var/log/ceph``.  By
default, cephadm daemons log to stderr and the logs are
captured by the container runtime environment.  For most systems, by
default, these logs are sent to journald and accessible via
``journalctl``.

For example, to view the logs for the daemon ``mon.foo`` for a cluster
with ID ``5c5a50ae-272a-455d-99e9-32c6a013e694``, the command would be
something like::

  journalctl -u ceph-5c5a50ae-272a-455d-99e9-32c6a013e694@mon.foo

This works well for normal operations when logging levels are low.

To disable logging to stderr::

  ceph config set global log_to_stderr false
  ceph config set global mon_cluster_log_to_stderr false

Logging to files
----------------

You can also configure Ceph daemons to log to files instead of stderr,
just like they have in the past.  When logging to files, Ceph logs appear
in ``/var/log/ceph/<cluster-fsid>``.

To enable logging to files::

  ceph config set global log_to_file true
  ceph config set global mon_cluster_log_to_file true

We recommend disabling logging to stderr (see above) or else everything
will be logged twice::

  ceph config set global log_to_stderr false
  ceph config set global mon_cluster_log_to_stderr false

By default, cephadm sets up log rotation on each host to rotate these
files.  You can configure the logging retention schedule by modifying
``/etc/logrotate.d/ceph.<cluster-fsid>``.


Data location
=============

Cephadm daemon data and logs in slightly different locations than older
versions of ceph:

* ``/var/log/ceph/<cluster-fsid>`` contains all cluster logs.  Note
  that by default cephadm logs via stderr and the container runtime,
  so these logs are normally not present.
* ``/var/lib/ceph/<cluster-fsid>`` contains all cluster daemon data
  (besides logs).
* ``/var/lib/ceph/<cluster-fsid>/<daemon-name>`` contains all data for
  an individual daemon.
* ``/var/lib/ceph/<cluster-fsid>/crash`` contains crash reports for
  the cluster.
* ``/var/lib/ceph/<cluster-fsid>/removed`` contains old daemon
  data directories for stateful daemons (e.g., monitor, prometheus)
  that have been removed by cephadm.

Disk usage
----------

Because a few Ceph daemons may store a significant amount of data in
``/var/lib/ceph`` (notably, the monitors and prometheus), we recommend
moving this directory to its own disk, partition, or logical volume so
that it does not fill up the root file system.


Health checks
=============
The cephadm module provides additional healthchecks to supplement the default healthchecks
provided by the Cluster. These additional healthchecks fall into two categories;

- **cephadm operations**: Healthchecks in this category are always executed when the cephadm module is active.
- **cluster configuration**: These healthchecks are *optional*, and focus on the configuration of the hosts in
  the cluster

CEPHADM Operations
------------------

CEPHADM_PAUSED
^^^^^^^^^^^^^^

Cephadm background work has been paused with ``ceph orch pause``.  Cephadm
continues to perform passive monitoring activities (like checking
host and daemon status), but it will not make any changes (like deploying
or removing daemons).

Resume cephadm work with::

  ceph orch resume

.. _cephadm-stray-host:

CEPHADM_STRAY_HOST
^^^^^^^^^^^^^^^^^^

One or more hosts have running Ceph daemons but are not registered as
hosts managed by *cephadm*.  This means that those services cannot
currently be managed by cephadm (e.g., restarted, upgraded, included
in `ceph orch ps`).

You can manage the host(s) with::

  ceph orch host add *<hostname>*

Note that you may need to configure SSH access to the remote host
before this will work.

Alternatively, you can manually connect to the host and ensure that
services on that host are removed or migrated to a host that is
managed by *cephadm*.

You can also disable this warning entirely with::

  ceph config set mgr mgr/cephadm/warn_on_stray_hosts false

See :ref:`cephadm-fqdn` for more information about host names and
domain names.

CEPHADM_STRAY_DAEMON
^^^^^^^^^^^^^^^^^^^^

One or more Ceph daemons are running but not are not managed by
*cephadm*.  This may be because they were deployed using a different
tool, or because they were started manually.  Those
services cannot currently be managed by cephadm (e.g., restarted,
upgraded, or included in `ceph orch ps`).

If the daemon is a stateful one (monitor or OSD), it should be adopted
by cephadm; see :ref:`cephadm-adoption`.  For stateless daemons, it is
usually easiest to provision a new daemon with the ``ceph orch apply``
command and then stop the unmanaged daemon.

This warning can be disabled entirely with::

  ceph config set mgr mgr/cephadm/warn_on_stray_daemons false

CEPHADM_HOST_CHECK_FAILED
^^^^^^^^^^^^^^^^^^^^^^^^^

One or more hosts have failed the basic cephadm host check, which verifies
that (1) the host is reachable and cephadm can be executed there, and (2)
that the host satisfies basic prerequisites, like a working container
runtime (podman or docker) and working time synchronization.
If this test fails, cephadm will no be able to manage services on that host.

You can manually run this check with::

  ceph cephadm check-host *<hostname>*

You can remove a broken host from management with::

  ceph orch host rm *<hostname>*

You can disable this health warning with::

  ceph config set mgr mgr/cephadm/warn_on_failed_host_check false

Cluster Configuration Checks
----------------------------
Cephadm periodically scans each of the hosts in the cluster, to understand the state
of the OS, disks, NICs etc. These facts can then be analysed for consistency across the hosts
in the cluster to identify any configuration anomalies.

The configuration checks are an **optional** feature, enabled by the following command
::

  ceph config set mgr mgr/cephadm/config_checks_enabled true

The configuration checks are triggered after each host scan (1m). The cephadm log entries will
show the current state and outcome of the configuration checks as follows;

Disabled state (config_checks_enabled false)
::

  ALL cephadm checks are disabled, use 'ceph config set mgr mgr/cephadm/config_checks_enabled true' to enable

Enabled state (config_checks_enabled true)
::

  CEPHADM 8/8 checks enabled and executed (0 bypassed, 0 disabled). No issues detected

The configuration checks themselves are managed through several cephadm sub-commands.

To determine whether the configuration checks are enabled, you can use the following command
::

  ceph cephadm config-check status

This command will return the status of the configuration checker as either "Enabled" or "Disabled".


Listing all the configuration checks and their current state
::

  ceph cephadm config-check ls

  e.g.
    NAME             HEALTHCHECK                      STATUS   DESCRIPTION
  kernel_security  CEPHADM_CHECK_KERNEL_LSM         enabled  checks SELINUX/Apparmor profiles are consistent across cluster hosts
  os_subscription  CEPHADM_CHECK_SUBSCRIPTION       enabled  checks subscription states are consistent for all cluster hosts
  public_network   CEPHADM_CHECK_PUBLIC_MEMBERSHIP  enabled  check that all hosts have a NIC on the Ceph public_netork
  osd_mtu_size     CEPHADM_CHECK_MTU                enabled  check that OSD hosts share a common MTU setting
  osd_linkspeed    CEPHADM_CHECK_LINKSPEED          enabled  check that OSD hosts share a common linkspeed
  network_missing  CEPHADM_CHECK_NETWORK_MISSING    enabled  checks that the cluster/public networks defined exist on the Ceph hosts
  ceph_release     CEPHADM_CHECK_CEPH_RELEASE       enabled  check for Ceph version consistency - ceph daemons should be on the same release (unless upgrade is active)
  kernel_version   CEPHADM_CHECK_KERNEL_VERSION     enabled  checks that the MAJ.MIN of the kernel on Ceph hosts is consistent

The name of each configuration check, can then be used to enable or disable a specific check.
::

  ceph cephadm config-check disable <name>

  eg.
  ceph cephadm config-check disable kernel_security

CEPHADM_CHECK_KERNEL_LSM
^^^^^^^^^^^^^^^^^^^^^^^^
Each host within the cluster is expected to operate within the same Linux Security Module (LSM) state. For example,
if the majority of the hosts are running with SELINUX in enforcing mode, any host not running in this mode
would be flagged as an anomaly and a healtcheck (WARNING) state raised.

CEPHADM_CHECK_SUBSCRIPTION
^^^^^^^^^^^^^^^^^^^^^^^^^^
This check relates to the status of vendor subscription. This check is only performed for hosts using RHEL, but helps
to confirm that all your hosts are covered by an active subscription so patches and updates
are available.

CEPHADM_CHECK_PUBLIC_MEMBERSHIP
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
All members of the cluster should have NICs configured on at least one of the public network subnets. Hosts
that are not on the public network will rely on routing which may affect performance

CEPHADM_CHECK_MTU
^^^^^^^^^^^^^^^^^
The MTU of the NICs on OSDs can be a key factor in consistent performance. This check examines hosts
that are running OSD services to ensure that the MTU is configured consistently within the cluster. This is
determined by establishing the MTU setting that the majority of hosts are using, with any anomalies being
resulting in a Ceph healthcheck.

CEPHADM_CHECK_LINKSPEED
^^^^^^^^^^^^^^^^^^^^^^^
Similar to the MTU check, linkspeed consistency is also a factor in consistent cluster performance.
This check determines the linkspeed shared by the majority of "OSD hosts", resulting in a healthcheck for
any hosts that are set at a lower linkspeed rate.

CEPHADM_CHECK_NETWORK_MISSING
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The public_network and cluster_network settings support subnet definitions for IPv4 and IPv6. If these
settings are not found on any host in the cluster a healthcheck is raised.

CEPHADM_CHECK_CEPH_RELEASE
^^^^^^^^^^^^^^^^^^^^^^^^^^
Under normal operations, the ceph cluster should be running daemons under the same ceph release (i.e. all
pacific). This check looks at the active release for each daemon, and reports any anomalies as a
healthcheck. *This check is bypassed if an upgrade process is active within the cluster.*

CEPHADM_CHECK_KERNEL_VERSION
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The OS kernel version (maj.min) is checked for consistency across the hosts. Once again, the
majority of the hosts is used as the basis of identifying anomalies.

/etc/ceph/ceph.conf
===================

Cephadm distributes a minimized ``ceph.conf`` that only contains
a minimal set of information to connect to the Ceph cluster.

To update the configuration settings, instead of manually editing
the ``ceph.conf`` file, use the config database instead::

  ceph config set ...

See :ref:`ceph-conf-database` for details.

By default, cephadm does not deploy that minimized ``ceph.conf`` across the
cluster. To enable the management of ``/etc/ceph/ceph.conf`` files on all
hosts, please enable this by running::

  ceph config set mgr mgr/cephadm/manage_etc_ceph_ceph_conf true

To set up an initial configuration before bootstrapping
the cluster, create an initial ``ceph.conf`` file. For example::

  cat <<EOF > /etc/ceph/ceph.conf
  [global]
  osd crush chooseleaf type = 0
  EOF

Then, run bootstrap referencing this file::

  cephadm bootstrap -c /root/ceph.conf ...


