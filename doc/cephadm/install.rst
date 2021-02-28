============================
Deploying a new Ceph cluster
============================

Cephadm creates a new Ceph cluster by "bootstrapping" on a single
host, expanding the cluster to encompass any additional hosts, and
then deploying the needed services.

.. highlight:: console

Requirements
============

- Systemd
- Podman or Docker for running containers
- Time synchronization (such as chrony or NTP)
- LVM2 for provisioning storage devices

Any modern Linux distribution should be sufficient.  Dependencies
are installed automatically by the bootstrap process below.

.. _get-cephadm:

Install cephadm
===============

The ``cephadm`` command can 

#. bootstrap a new cluster
#. launch a containerized shell with a working Ceph CLI
#. aid in debugging containerized Ceph daemons

There are two ways to install ``cephadm``:

#. a :ref:`curl-based installation<cephadm_install_curl>` method
#. :ref:`distribution-specific installation methods<cephadm_install_distros>`

.. _cephadm_install_curl:

curl-based installation
-----------------------

* Use ``curl`` to fetch the most recent version of the
  standalone script. 
  
  .. prompt:: bash #

   curl --silent --remote-name --location https://github.com/ceph/ceph/raw/octopus/src/cephadm/cephadm

  Make the ``cephadm`` script executable:

  .. prompt:: bash #

   chmod +x cephadm

  This script can be run directly from the current directory:

  .. prompt:: bash #

   ./cephadm <arguments...>

* Although the standalone script is sufficient to get a cluster started, it is
  convenient to have the ``cephadm`` command installed on the host.  To install
  the packages that provide the ``cephadm`` command for the Octopus release,
  run the following commands:

  .. prompt:: bash #

    ./cephadm add-repo --release octopus
    ./cephadm install

  Confirm that ``cephadm`` is now in your PATH by running ``which``:

  .. prompt:: bash #

    which cephadm

  A successful ``which cephadm`` command will return this:

  .. code-block:: bash

    /usr/sbin/cephadm

.. _cephadm_install_distros:

distribution-specific installations
-----------------------------------

.. important:: The methods of installing ``cephadm`` in this section are distinct from the curl-based method above. Use either the curl-based method above or one of the methods in this section, but not both the curl-based method and one of these.

Some Linux distributions  may already include up-to-date Ceph packages.  In
that case, you can install cephadm directly. For example:

  In Ubuntu:

  .. prompt:: bash #

     dnf install -y cephadm   

  In SUSE:

  .. prompt:: bash #

     zypper install -y cephadm



Bootstrap a new cluster
=======================

You need to know which *IP address* to use for the cluster's first
monitor daemon.  This is normally just the IP for the first host.  If there
are multiple networks and interfaces, be sure to choose one that will
be accessible by any host accessing the Ceph cluster.

Run the ``ceph bootstrap`` command:

.. prompt:: bash # 

   cephadm bootstrap --mon-ip *<mon-ip>*

This command will:

* Create a monitor and manager daemon for the new cluster on the local
  host.
* Generate a new SSH key for the Ceph cluster and adds it to the root
  user's ``/root/.ssh/authorized_keys`` file.
* Write a minimal configuration file needed to communicate with the
  new cluster to ``/etc/ceph/ceph.conf``.
* Write a copy of the ``client.admin`` administrative (privileged!)
  secret key to ``/etc/ceph/ceph.client.admin.keyring``.
* Write a copy of the public key to
  ``/etc/ceph/ceph.pub``.

The default bootstrap behavior will work for the vast majority of
users.  See below for a few options that may be useful for some users,
or run ``cephadm bootstrap -h`` to see all available options:

* In larger Ceph clusters, network separation between the public
  network traffic and cluster traffic which handles replication,
  recovery and heartbeats between OSD daemons, can lead to performance
  improvements. To define the `cluster network`_ you can supply the
  ``--cluster-network`` option to the ``bootstrap`` subcommand. This
  parameter must define a subnet in CIDR notation, for example
  10.90.90.0/24 or fe80::/64.

* Bootstrap writes the files needed to access the new cluster to ``/etc/ceph``,
  so that any Ceph packages installed on the host itself (e.g., to access the
  command line interface) can easily find them.

  Daemon containers deployed with cephadm, however, do not need
  ``/etc/ceph`` at all.  Use the ``--output-dir *<directory>*`` option
  to put them in a different directory (like ``.``), avoiding any
  potential conflicts with existing Ceph configuration (cephadm or
  otherwise) on the same host.

* You can pass any initial Ceph configuration options to the new
  cluster by putting them in a standard ini-style configuration file
  and using the ``--config *<config-file>*`` option.

* You can choose the ssh user cephadm will use to connect to hosts by
  using the ``--ssh-user *<user>*`` option. The ssh key will be added
  to ``/home/*<user>*/.ssh/authorized_keys``. This user will require
  passwordless sudo access.

* If you are using a container on an authenticated registry that requires
  login you may add the three arguments ``--registry-url <url of registry>``,
  ``--registry-username <username of account on registry>``,
  ``--registry-password <password of account on registry>`` OR
  ``--registry-json <json file with login info>``. Cephadm will attempt
  to login to this registry so it may pull your container and then store
  the login info in its config database so other hosts added to the cluster
  may also make use of the authenticated registry.

.. _cephadm-enable-cli:

Enable Ceph CLI
===============

Cephadm does not require any Ceph packages to be installed on the
host.  However, we recommend enabling easy access to the ``ceph``
command.  There are several ways to do this:

* The ``cephadm shell`` command launches a bash shell in a container
  with all of the Ceph packages installed. By default, if
  configuration and keyring files are found in ``/etc/ceph`` on the
  host, they are passed into the container environment so that the
  shell is fully functional. Note that when executed on a MON host,
  ``cephadm shell`` will infer the ``config`` from the MON container
  instead of using the default configuration. If ``--mount <path>``
  is given, then the host ``<path>`` (file or directory) will appear
  under ``/mnt`` inside the container:

  .. prompt:: bash #

     cephadm shell

* To execute ``ceph`` commands, you can also run commands like this:

  .. prompt:: bash #

     cephadm shell -- ceph -s

* You can install the ``ceph-common`` package, which contains all of the
  ceph commands, including ``ceph``, ``rbd``, ``mount.ceph`` (for mounting
  CephFS file systems), etc.:

  .. prompt:: bash #

    cephadm add-repo --release octopus
    cephadm install ceph-common

Confirm that the ``ceph`` command is accessible with:

.. prompt:: bash #
 
  ceph -v


Confirm that the ``ceph`` command can connect to the cluster and also
its status with:

.. prompt:: bash #

  ceph status

Adding Hosts
============

Next, add all hosts to the cluster by following :ref:`cephadm-adding-hosts`.

Adding additional MONs
======================

A typical Ceph cluster has three or five monitor daemons spread
across different hosts.  We recommend deploying five
monitors if there are five or more nodes in your cluster.

Please follow :ref:`deploy_additional_monitors` to deploy additional MONs.

Adding Storage
==============

To add storage to the cluster, either tell Ceph to consume any
available and unused device:

  .. prompt:: bash #

    ceph orch apply osd --all-available-devices

Or See :ref:`cephadm-deploy-osds` for more detailed instructions.

Using Ceph
==========

To use the *Ceph Filesystem*, follow :ref:`orchestrator-cli-cephfs`.

To use the *Ceph Object Gateway*, follow :ref:`cephadm-deploy-rgw`.

To use *NFS*, follow :ref:`deploy-cephadm-nfs-ganesha`

To use *iSCSI*, follow :ref:`cephadm-iscsi`


.. _cluster network: ../rados/configuration/network-config-ref#cluster-network
