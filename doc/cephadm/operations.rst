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



SSH Configuration
=================

Cephadm uses SSH to connect to remote hosts.  SSH uses a key to authenticate
with those hosts in a secure way.


Default behavior
----------------

Cephadm stores an SSH key in the monitor that is used to
connect to remote hosts.  When the cluster is bootstrapped, this SSH
key is generated automatically and no additional configuration
is necessary.

A *new* SSH key can be generated with::

  ceph cephadm generate-key

The public portion of the SSH key can be retrieved with::

  ceph cephadm get-pub-key

The currently stored SSH key can be deleted with::

  ceph cephadm clear-key

You can make use of an existing key by directly importing it with::

  ceph config-key set mgr/cephadm/ssh_identity_key -i <key>
  ceph config-key set mgr/cephadm/ssh_identity_pub -i <pub>

You will then need to restart the mgr daemon to reload the configuration with::

  ceph mgr fail


Customizing the SSH configuration
---------------------------------

Cephadm generates an appropriate ``ssh_config`` file that is
used for connecting to remote hosts.  This configuration looks
something like this::

  Host *
  User root
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null

There are two ways to customize this configuration for your environment:

#. Import a customized configuration file that will be stored
   by the monitor with::

     ceph cephadm set-ssh-config -i <ssh_config_file>

   To remove a customized SSH config and revert back to the default behavior::

     ceph cephadm clear-ssh-config

#. You can configure a file location for the SSH configuration file with::

     ceph config set mgr mgr/cephadm/ssh_config_file <path>

   We do *not recommend* this approach.  The path name must be
   visible to *any* mgr daemon, and cephadm runs all daemons as
   containers. That means that the file either need to be placed
   inside a customized container image for your deployment, or
   manually distributed to the mgr data directory
   (``/var/lib/ceph/<cluster-fsid>/mgr.<id>`` on the host, visible at
   ``/var/lib/ceph/mgr/ceph-<id>`` from inside the container).


Health checks
=============

CEPHADM_PAUSED
--------------

Cephadm background work has been paused with ``ceph orch pause``.  Cephadm
continues to perform passive monitoring activities (like checking
host and daemon status), but it will not make any changes (like deploying
or removing daemons).

Resume cephadm work with::

  ceph orch resume

.. _cephadm-stray-host:

CEPHADM_STRAY_HOST
------------------

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
--------------------

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
-------------------------

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

/etc/ceph/ceph.conf
===================

Cephadm uses a minimized ``ceph.conf`` that only contains 
a minimal set of information to connect to the Ceph cluster.

To update the configuration settings, use::

  ceph config set ...


To set up an initial configuration before calling
`bootstrap`, create an initial ``ceph.conf`` file. For example::

  cat <<EOF > /etc/ceph/ceph.conf
  [global]
  osd crush chooseleaf type = 0
  EOF
  cephadm bootstrap -c /root/ceph.conf ...
