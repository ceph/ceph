==================
Cephadm Operations
==================

.. _watching_cephadm_logs:

Watching cephadm log messages
=============================

Cephadm writes logs to the ``cephadm`` cluster log channel. You can
monitor Ceph's activity in real time by reading the logs as they fill
up. Run the following command to see the logs in real time:

.. prompt:: bash #

  ceph -W cephadm

By default, this command shows info-level events and above.  To see
debug-level messages as well as info-level events, run the following
commands:

.. prompt:: bash #

  ceph config set mgr mgr/cephadm/log_to_cluster_level debug
  ceph -W cephadm --watch-debug

.. warning::

  The debug messages are very verbose!

You can see recent events by running the following command:

.. prompt:: bash #

  ceph log last cephadm

These events are also logged to the ``ceph.cephadm.log`` file on
monitor hosts as well as to the monitor daemons' stderr.


.. _cephadm-logs:


Ceph daemon control
===================

Starting and stopping daemons
-----------------------------

You can stop, start, or restart a daemon with:

.. prompt:: bash #

   ceph orch daemon stop <name>
   ceph orch daemon start <name>
   ceph orch daemon restart <name>

You can also do the same for all daemons for a service with:   

.. prompt:: bash #

   ceph orch stop <name>
   ceph orch start <name>
   ceph orch restart <name>


Redeploying or reconfiguring a daemon
-------------------------------------

The container for a daemon can be stopped, recreated, and restarted with
the ``redeploy`` command:

.. prompt:: bash #

   ceph orch daemon redeploy <name> [--image <image>]

A container image name can optionally be provided to force a
particular image to be used (instead of the image specified by the
``container_image`` config value).

If only the ceph configuration needs to be regenerated, you can also
issue a ``reconfig`` command, which will rewrite the ``ceph.conf``
file but will not trigger a restart of the daemon.

.. prompt:: bash #

   ceph orch daemon reconfig <name>


Rotating a daemon's authenticate key
------------------------------------

All Ceph and gateway daemons in the cluster have a secret key that is used to connect
to and authenticate with the cluster.  This key can be rotated (i.e., replaced with a
new key) with the following command:

.. prompt:: bash #

   ceph orch daemon rotate-key <name>

For MDS, OSD, and MGR daemons, this does not require a daemon restart.  For other
daemons, however (e.g., RGW), the daemon may be restarted to switch to the new key.


Ceph daemon logs
================

Logging to journald
-------------------

Ceph daemons traditionally write logs to ``/var/log/ceph``. Ceph daemons log to
journald by default and Ceph logs are captured by the container runtime
environment. They are accessible via ``journalctl``.

.. note:: Prior to Quincy, ceph daemons logged to stderr.

Example of logging to journald
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For example, to view the logs for the daemon ``mon.foo`` for a cluster
with ID ``5c5a50ae-272a-455d-99e9-32c6a013e694``, the command would be
something like:

.. prompt:: bash #

  journalctl -u ceph-5c5a50ae-272a-455d-99e9-32c6a013e694@mon.foo

This works well for normal operations when logging levels are low.

Logging to files
----------------

You can also configure Ceph daemons to log to files instead of to
journald if you prefer logs to appear in files (as they did in earlier,
pre-cephadm, pre-Octopus versions of Ceph).  When Ceph logs to files,
the logs appear in ``/var/log/ceph/<cluster-fsid>``. If you choose to
configure Ceph to log to files instead of to journald, remember to
configure Ceph so that it will not log to journald (the commands for
this are covered below).

Enabling logging to files
~~~~~~~~~~~~~~~~~~~~~~~~~

To enable logging to files, run the following commands:

.. prompt:: bash #

  ceph config set global log_to_file true
  ceph config set global mon_cluster_log_to_file true

Disabling logging to journald
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you choose to log to files, we recommend disabling logging to journald or else
everything will be logged twice. Run the following commands to disable logging
to stderr:

.. prompt:: bash #

  ceph config set global log_to_stderr false
  ceph config set global mon_cluster_log_to_stderr false
  ceph config set global log_to_journald false
  ceph config set global mon_cluster_log_to_journald false

.. note:: You can change the default by passing --log-to-file during
   bootstrapping a new cluster.

Modifying the log retention schedule
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, cephadm sets up log rotation on each host to rotate these
files.  You can configure the logging retention schedule by modifying
``/etc/logrotate.d/ceph.<cluster-fsid>``.


Data location
=============

Cephadm stores daemon data and logs in different locations than did
older, pre-cephadm (pre Octopus) versions of ceph:

* ``/var/log/ceph/<cluster-fsid>`` contains all cluster logs. By
  default, cephadm logs via stderr and the container runtime. These
  logs will not exist unless you have enabled logging to files as
  described in `cephadm-logs`_.
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

Because a few Ceph daemons (notably, the monitors and prometheus) store a
large amount of data in ``/var/lib/ceph`` , we recommend moving this
directory to its own disk, partition, or logical volume so that it does not
fill up the root file system.


Health checks
=============
The cephadm module provides additional health checks to supplement the
default health checks provided by the Cluster. These additional health
checks fall into two categories:

- **cephadm operations**: Health checks in this category are always
  executed when the cephadm module is active.
- **cluster configuration**: These health checks are *optional*, and
  focus on the configuration of the hosts in the cluster.

CEPHADM Operations
------------------

CEPHADM_PAUSED
~~~~~~~~~~~~~~

This indicates that cephadm background work has been paused with
``ceph orch pause``.  Cephadm continues to perform passive monitoring
activities (like checking host and daemon status), but it will not
make any changes (like deploying or removing daemons).

Resume cephadm work by running the following command:

.. prompt:: bash #

  ceph orch resume

.. _cephadm-stray-host:

CEPHADM_STRAY_HOST
~~~~~~~~~~~~~~~~~~

This indicates that one or more hosts have Ceph daemons that are
running, but are not registered as hosts managed by *cephadm*.  This
means that those services cannot currently be managed by cephadm
(e.g., restarted, upgraded, included in `ceph orch ps`).

* You can manage the host(s) by running the following command:

  .. prompt:: bash #

    ceph orch host add *<hostname>*

  .. note::

    You might need to configure SSH access to the remote host
    before this will work.

* See :ref:`cephadm-fqdn` for more information about host names and
  domain names.

* Alternatively, you can manually connect to the host and ensure that
  services on that host are removed or migrated to a host that is
  managed by *cephadm*.

* This warning can be disabled entirely by running the following
  command:

  .. prompt:: bash #

    ceph config set mgr mgr/cephadm/warn_on_stray_hosts false

CEPHADM_STRAY_DAEMON
~~~~~~~~~~~~~~~~~~~~

One or more Ceph daemons are running but not are not managed by
*cephadm*.  This may be because they were deployed using a different
tool, or because they were started manually.  Those
services cannot currently be managed by cephadm (e.g., restarted,
upgraded, or included in `ceph orch ps`).

* If the daemon is a stateful one (monitor or OSD), it should be adopted
  by cephadm; see :ref:`cephadm-adoption`.  For stateless daemons, it is
  usually easiest to provision a new daemon with the ``ceph orch apply``
  command and then stop the unmanaged daemon.

* If the stray daemon(s) are running on hosts not managed by cephadm, you can manage the host(s) by running the following command:

  .. prompt:: bash #

    ceph orch host add *<hostname>*

  .. note::

    You might need to configure SSH access to the remote host
    before this will work.

* See :ref:`cephadm-fqdn` for more information about host names and
  domain names.

* This warning can be disabled entirely by running the following command:

  .. prompt:: bash #

    ceph config set mgr mgr/cephadm/warn_on_stray_daemons false

CEPHADM_HOST_CHECK_FAILED
~~~~~~~~~~~~~~~~~~~~~~~~~

One or more hosts have failed the basic cephadm host check, which verifies
that (1) the host is reachable and cephadm can be executed there, and (2)
that the host satisfies basic prerequisites, like a working container
runtime (podman or docker) and working time synchronization.
If this test fails, cephadm will no be able to manage services on that host.

You can manually run this check by running the following command:

.. prompt:: bash #

  ceph cephadm check-host *<hostname>*

You can remove a broken host from management by running the following command:

.. prompt:: bash #

  ceph orch host rm *<hostname>*

You can disable this health warning by running the following command:

.. prompt:: bash #

  ceph config set mgr mgr/cephadm/warn_on_failed_host_check false

Cluster Configuration Checks
----------------------------
Cephadm periodically scans each host in the cluster in order
to understand the state of the OS, disks, network interfacess etc. This information can
then be analyzed for consistency across the hosts in the cluster to
identify any configuration anomalies.

Enabling Cluster Configuration Checks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These configuration checks are an **optional** feature, and are enabled
by running the following command:

.. prompt:: bash #

  ceph config set mgr mgr/cephadm/config_checks_enabled true

States Returned by Cluster Configuration Checks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configuration checks are triggered after each host scan. The
cephadm log entries will show the current state and outcome of the
configuration checks as follows:

Disabled state (config_checks_enabled false):

.. code-block:: bash 

  ALL cephadm checks are disabled, use 'ceph config set mgr mgr/cephadm/config_checks_enabled true' to enable

Enabled state (config_checks_enabled true):

.. code-block:: bash 

  CEPHADM 8/8 checks enabled and executed (0 bypassed, 0 disabled). No issues detected

Managing Configuration Checks (subcommands)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The configuration checks themselves are managed through several cephadm subcommands.

To determine whether the configuration checks are enabled, run the following command:

.. prompt:: bash #

  ceph cephadm config-check status

This command returns the status of the configuration checker as either "Enabled" or "Disabled".


To list all the configuration checks and their current states, run the following command:

.. code-block:: console

  # ceph cephadm config-check ls

    NAME             HEALTHCHECK                      STATUS   DESCRIPTION
  kernel_security  CEPHADM_CHECK_KERNEL_LSM         enabled  check that SELINUX/Apparmor profiles are consistent across cluster hosts
  os_subscription  CEPHADM_CHECK_SUBSCRIPTION       enabled  check that subscription states are consistent for all cluster hosts
  public_network   CEPHADM_CHECK_PUBLIC_MEMBERSHIP  enabled  check that all hosts have a network interface on the Ceph public_network
  osd_mtu_size     CEPHADM_CHECK_MTU                enabled  check that OSD hosts share a common MTU setting
  osd_linkspeed    CEPHADM_CHECK_LINKSPEED          enabled  check that OSD hosts share a common network link speed
  network_missing  CEPHADM_CHECK_NETWORK_MISSING    enabled  check that the cluster/public networks as defined exist on the Ceph hosts
  ceph_release     CEPHADM_CHECK_CEPH_RELEASE       enabled  check for Ceph version consistency: all Ceph daemons should be the same release unless upgrade is in progress
  kernel_version   CEPHADM_CHECK_KERNEL_VERSION     enabled  checks that the maj.min version of the kernel is consistent across Ceph hosts

The name of each configuration check can be used to enable or disable a specific check by running a command of the following form:
:

.. prompt:: bash #

  ceph cephadm config-check disable <name>

For example:

.. prompt:: bash #

  ceph cephadm config-check disable kernel_security

CEPHADM_CHECK_KERNEL_LSM
~~~~~~~~~~~~~~~~~~~~~~~~
Each host within the cluster is expected to operate within the same Linux
Security Module (LSM) state. For example, if the majority of the hosts are
running with SELINUX in enforcing mode, any host not running in this mode is
flagged as an anomaly and a healthcheck (WARNING) state raised.

CEPHADM_CHECK_SUBSCRIPTION
~~~~~~~~~~~~~~~~~~~~~~~~~~
This check relates to the status of OS vendor subscription. This check is
performed only for hosts using RHEL and helps to confirm that all hosts are
covered by an active subscription, which ensures that patches and updates are
available.

CEPHADM_CHECK_PUBLIC_MEMBERSHIP
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
All members of the cluster should have a network interface configured on at least one of the
public network subnets. Hosts that are not on the public network will rely on
routing, which may affect performance.

CEPHADM_CHECK_MTU
~~~~~~~~~~~~~~~~~
The MTU of the network interfaces on OSD hosts can be a key factor in consistent performance. This
check examines hosts that are running OSD services to ensure that the MTU is
configured consistently within the cluster. This is determined by determining
the MTU setting that the majority of hosts is using. Any anomalies result in a
health check.

CEPHADM_CHECK_LINKSPEED
~~~~~~~~~~~~~~~~~~~~~~~
This check is similar to the MTU check. Link speed consistency is a factor in
consistent cluster performance, as is the MTU of the OSD node network interfaces.
This check determines the link speed shared by the majority of OSD hosts, and a
health check is run for any hosts that are set at a lower link speed rate.

CEPHADM_CHECK_NETWORK_MISSING
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The `public_network` and `cluster_network` settings support subnet definitions
for IPv4 and IPv6. If these settings are not found on any host in the cluster,
a health check is raised.

CEPHADM_CHECK_CEPH_RELEASE
~~~~~~~~~~~~~~~~~~~~~~~~~~
Under normal operations, the Ceph cluster runs daemons that are of the same Ceph
release (for example, Reef).  This check determines the active release for each daemon, and
reports any anomalies as a healthcheck. *This check is bypassed if an upgrade
is in process.*

CEPHADM_CHECK_KERNEL_VERSION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The OS kernel version (maj.min) is checked for consistency across hosts.
The kernel version of the majority of the hosts is used as the basis for
identifying anomalies.

.. _client_keyrings_and_configs:

Client keyrings and configs
===========================
Cephadm can distribute copies of the ``ceph.conf`` file and client keyring
files to hosts. Starting from versions 16.2.10 (Pacific) and 17.2.1 (Quincy),
in addition to the default location ``/etc/ceph/`` cephadm also stores config
and keyring files in the ``/var/lib/ceph/<fsid>/config`` directory. It is usually
a good idea to store a copy of the config and ``client.admin`` keyring on any host
used to administer the cluster via the CLI. By default, cephadm does this for any
nodes that have the ``_admin`` label (which normally includes the bootstrap host).

.. note:: Ceph daemons will still use files on ``/etc/ceph/``. The new configuration
   location ``/var/lib/ceph/<fsid>/config`` is used by cephadm only. Having this config
   directory under the fsid helps cephadm to load the configuration associated with
   the cluster.


When a client keyring is placed under management, cephadm will:

  - build a list of target hosts based on the specified placement spec (see
    :ref:`orchestrator-cli-placement-spec`)
  - store a copy of the ``/etc/ceph/ceph.conf`` file on the specified host(s)
  - store a copy of the ``ceph.conf`` file at ``/var/lib/ceph/<fsid>/config/ceph.conf`` on the specified host(s)
  - store a copy of the ``ceph.client.admin.keyring`` file at ``/var/lib/ceph/<fsid>/config/ceph.client.admin.keyring`` on the specified host(s)
  - store a copy of the keyring file on the specified host(s)
  - update the ``ceph.conf`` file as needed (e.g., due to a change in the cluster monitors)
  - update the keyring file if the entity's key is changed (e.g., via ``ceph
    auth ...`` commands)
  - ensure that the keyring file has the specified ownership and specified mode
  - remove the keyring file when client keyring management is disabled
  - remove the keyring file from old hosts if the keyring placement spec is
    updated (as needed)

Listing Client Keyrings
-----------------------

To see the list of client keyrings are currently under management, run the following command:

.. prompt:: bash #

  ceph orch client-keyring ls

Putting a Keyring Under Management
----------------------------------

To put a keyring under management, run a command of the following form: 

.. prompt:: bash #

  ceph orch client-keyring set <entity> <placement> [--mode=<mode>] [--owner=<uid>.<gid>] [--path=<path>]

- By default, the *path* is ``/etc/ceph/client.{entity}.keyring``, which is
  where Ceph looks by default.  Be careful when specifying alternate locations,
  as existing files may be overwritten.
- A placement of ``*`` (all hosts) is common.
- The mode defaults to ``0600`` and ownership to ``0:0`` (user root, group root).

For example, to create a ``client.rbd`` key and deploy it to hosts with the
``rbd-client`` label and make it group readable by uid/gid 107 (qemu), run the
following commands:

.. prompt:: bash #

  ceph auth get-or-create-key client.rbd mon 'profile rbd' mgr 'profile rbd' osd 'profile rbd pool=my_rbd_pool'
  ceph orch client-keyring set client.rbd label:rbd-client --owner 107:107 --mode 640

The resulting keyring file is:

.. code-block:: console

  -rw-r-----. 1 qemu qemu 156 Apr 21 08:47 /etc/ceph/client.client.rbd.keyring

Disabling Management of a Keyring File
--------------------------------------

To disable management of a keyring file, run a command of the following form:

.. prompt:: bash #

  ceph orch client-keyring rm <entity>

.. note::

  This deletes any keyring files for this entity that were previously written
  to cluster nodes.

.. _etc_ceph_conf_distribution:

/etc/ceph/ceph.conf
===================

Distributing ceph.conf to hosts that have no keyrings
-----------------------------------------------------

It might be useful to distribute ``ceph.conf`` files to hosts without an
associated client keyring file.  By default, cephadm deploys only a
``ceph.conf`` file to hosts where a client keyring is also distributed (see
above).  To write config files to hosts without client keyrings, run the
following command:

.. prompt:: bash #

    ceph config set mgr mgr/cephadm/manage_etc_ceph_ceph_conf true

Using Placement Specs to specify which hosts get keyrings
---------------------------------------------------------

By default, the configs are written to all hosts (i.e., those listed by ``ceph
orch host ls``).  To specify which hosts get a ``ceph.conf``, run a command of
the following form:

.. prompt:: bash #

  ceph config set mgr mgr/cephadm/manage_etc_ceph_ceph_conf_hosts <placement spec>

For example, to distribute configs to hosts with the ``bare_config`` label, run
the following command:

Distributing ceph.conf to hosts tagged with bare_config 
-------------------------------------------------------

For example, to distribute configs to hosts with the ``bare_config`` label, run the following command:

.. prompt:: bash #

  ceph config set mgr mgr/cephadm/manage_etc_ceph_ceph_conf_hosts label:bare_config

(See :ref:`orchestrator-cli-placement-spec` for more information about placement specs.)

Purging a cluster
=================

.. danger:: THIS OPERATION WILL DESTROY ALL DATA STORED IN THIS CLUSTER

In order to destroy a cluster and delete all data stored in this cluster, disable
cephadm to stop all orchestration operations (so we avoid deploying new daemons).

.. prompt:: bash #

  ceph mgr module disable cephadm

Then verify the FSID of the cluster:

.. prompt:: bash #

  ceph fsid

Purge ceph daemons from all hosts in the cluster

.. prompt:: bash #

  # For each host:
  cephadm rm-cluster --force --zap-osds --fsid <fsid>
