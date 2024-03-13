=======================
Basic Ceph Client Setup
=======================
Client hosts require basic configuration to interact with
Ceph clusters. This section describes how to perform this configuration.

.. note:: 
   Most client hosts need to install only the ``ceph-common`` package
   and its dependencies. Such an installation supplies the basic ``ceph`` and
   ``rados`` commands, as well as other commands including ``mount.ceph``
   and ``rbd``.

Config File Setup
=================
Client hosts  usually require smaller configuration files (here
sometimes called "config files") than do back-end cluster hosts.
To generate a minimal config file, log into a host that has been
configured as a client or that is running a cluster daemon, then
run the following command:

.. prompt:: bash #

  ceph config generate-minimal-conf

This command generates a minimal config file that tells the client how
to reach the Ceph Monitors. This file should usually 
be copied to ``/etc/ceph/ceph.conf`` on each client host.

Keyring Setup
=============
Most Ceph clusters run with authentication enabled. This means that
the client needs keys in order to communicate with Ceph daemons.
To generate a keyring file with credentials for ``client.fs``,
log into an running cluster member and run the following command:

.. prompt:: bash $

  ceph auth get-or-create client.fs

The resulting output is directed into a keyring file, typically
``/etc/ceph/ceph.keyring``.

To gain a broader understanding of client keyring distribution and administration,
you should read :ref:`client_keyrings_and_configs`.

To see an example that explains how to distribute ``ceph.conf`` configuration
files to hosts that are tagged with the ``bare_config`` label, you should read
the subsection named "Distributing ceph.conf to hosts tagged with bare_config"
under the heading :ref:`etc_ceph_conf_distribution`.
