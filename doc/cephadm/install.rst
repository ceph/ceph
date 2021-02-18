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
#. launch a containerized shell with a working Ceph CLI, and 
#. aid in debugging containerized Ceph daemons.

There are a few ways to install cephadm:

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


* Some commercial Linux distributions (e.g., RHEL, SLE) may already
  include up-to-date Ceph packages.  In that case, you can install
  cephadm directly.  For example:

  .. prompt:: bash #

     dnf install -y cephadm   

  or

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


.. _deploy_additional_monitors:

Deploy additional monitors (optional)
=====================================

A typical Ceph cluster has three or five monitor daemons spread
across different hosts.  We recommend deploying five
monitors if there are five or more nodes in your cluster.

.. _CIDR: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation

When Ceph knows what IP subnet the monitors should use it can automatically
deploy and scale monitors as the cluster grows (or contracts).  By default,
Ceph assumes that other monitors should use the same subnet as the first
monitor's IP.

If your Ceph monitors (or the entire cluster) live on a single subnet,
then by default cephadm automatically adds up to 5 monitors as you add new
hosts to the cluster. No further steps are necessary.

* If there is a specific IP subnet that should be used by monitors, you
  can configure that in `CIDR`_ format (e.g., ``10.1.2.0/24``) with:

  .. prompt:: bash #

     ceph config set mon public_network *<mon-cidr-network>*

  For example:

  .. prompt:: bash #

     ceph config set mon public_network 10.1.2.0/24

  Cephadm deploys new monitor daemons only on hosts that have IPs
  configured in the configured subnet.

* If you want to adjust the default of 5 monitors, run this command:

  .. prompt:: bash #

     ceph orch apply mon *<number-of-monitors>*

* To deploy monitors on a specific set of hosts, run this command:

  .. prompt:: bash #

    ceph orch apply mon *<host1,host2,host3,...>*

  Be sure to include the first (bootstrap) host in this list.

* You can control which hosts the monitors run on by making use of
  host labels.  To set the ``mon`` label to the appropriate
  hosts, run this command:
  
  .. prompt:: bash #

    ceph orch host label add *<hostname>* mon

  To view the current hosts and labels, run this command:

  .. prompt:: bash #

    ceph orch host ls

  For example:

  .. prompt:: bash #

    ceph orch host label add host1 mon
    ceph orch host label add host2 mon
    ceph orch host label add host3 mon
    ceph orch host ls

  .. code-block:: bash

    HOST   ADDR   LABELS  STATUS
    host1         mon
    host2         mon
    host3         mon
    host4
    host5

  Tell cephadm to deploy monitors based on the label by running this command:

  .. prompt:: bash #

    ceph orch apply mon label:mon

* You can explicitly specify the IP address or CIDR network for each monitor
  and control where it is placed.  To disable automated monitor deployment, run
  this command:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged

  To deploy each additional monitor:

  .. prompt:: bash #

    ceph orch daemon add mon *<host1:ip-or-network1> [<host1:ip-or-network-2>...]*

  For example, to deploy a second monitor on ``newhost1`` using an IP
  address ``10.1.2.123`` and a third monitor on ``newhost2`` in
  network ``10.1.2.0/24``, run the following commands:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged
    ceph orch daemon add mon newhost1:10.1.2.123
    ceph orch daemon add mon newhost2:10.1.2.0/24

  .. note::
     The **apply** command can be confusing. For this reason, we recommend using
     YAML specifications. 

     Each ``ceph orch apply mon`` command supersedes the one before it. 
     This means that you must use the proper comma-separated list-based 
     syntax when you want to apply monitors to more than one host. 
     If you do not use the proper syntax, you will clobber your work 
     as you go.

     For example:

     .. prompt:: bash #
        
          ceph orch apply mon host1
          ceph orch apply mon host2
          ceph orch apply mon host3

     This results in only one host having a monitor applied to it: host 3.

     (The first command creates a monitor on host1. Then the second command
     clobbers the monitor on host1 and creates a monitor on host2. Then the
     third command clobbers the monitor on host2 and creates a monitor on 
     host3. In this scenario, at this point, there is a monitor ONLY on
     host3.)

     To make certain that a monitor is applied to each of these three hosts,
     run a command like this:
     
     .. prompt:: bash #
       
       ceph orch apply mon "host1,host2,host3"

     There is another way to apply monitors to multiple hosts: a ``yaml`` file
     can be used. Instead of using the "ceph orch apply mon" commands, run a
     command of this form:
     
     .. prompt:: bash #

        ceph orch apply -i file.yaml

     Here is a sample **file.yaml** file::

          service_type: mon
          placement:
            hosts:
             - host1
             - host2
             - host3

Adding Storage
==============

To add storage to the cluster, either tell Ceph to consume any
available and unused device:

  .. prompt:: bash #

    ceph orch apply osd --all-available-devices

Or See :ref:`cephadm-deploy-osds` for more detailed instructions.


Deploy CephFS
=============

One or more MDS daemons is required to use the CephFS file system.
These are created automatically if the newer ``ceph fs volume``
interface is used to create a new file system. For more information,
see :ref:`fs-volumes-and-subvolumes`.

For example:

.. prompt:: bash #

  ceph fs volume create <fs_name> --placement=""<placement spec>""

See :ref:`orchestrator-cli-stateless-services` for manually deploying
MDS daemons.


To use the *Ceph Object Gateway*, follow :ref:`cephadm-deploy-rgw`.


.. _deploy-cephadm-nfs-ganesha:

Deploying NFS ganesha
=====================

Cephadm deploys NFS Ganesha using a pre-defined RADOS *pool*
and optional *namespace*

To deploy a NFS Ganesha gateway, run the following command:

.. prompt:: bash #

    ceph orch apply nfs *<svc_id>* *<pool>* *<namespace>* --placement="*<num-daemons>* [*<host1>* ...]"

For example, to deploy NFS with a service id of *foo*, that will use the RADOS
pool *nfs-ganesha* and namespace *nfs-ns*:

.. prompt:: bash #

   ceph orch apply nfs foo nfs-ganesha nfs-ns

.. note::
   Create the *nfs-ganesha* pool first if it doesn't exist.

See :ref:`orchestrator-cli-placement-spec` for details of the placement specification.

Deploying custom containers
===========================
It is also possible to choose different containers than the default containers to deploy Ceph. See :ref:`containers` for information about your options in this regard.

.. _cluster network: ../rados/configuration/network-config-ref#cluster-network
