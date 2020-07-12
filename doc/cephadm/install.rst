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

The ``cephadm`` command can (1) bootstrap a new cluster, (2)
launch a containerized shell with a working Ceph CLI, and (3) aid in
debugging containerized Ceph daemons.

There are a few ways to install cephadm:

* Use ``curl`` to fetch the most recent version of the
  standalone script::

    # curl --silent --remote-name --location https://github.com/ceph/ceph/raw/octopus/src/cephadm/cephadm
    # chmod +x cephadm

  This script can be run directly from the current directory with::

    # ./cephadm <arguments...>

* Although the standalone script is sufficient to get a cluster started, it is
  convenient to have the ``cephadm`` command installed on the host.  To install
  these packages for the current Octopus release::

    # ./cephadm add-repo --release octopus
    # ./cephadm install

  Confirm that ``cephadm`` is now in your PATH with::

    # which cephadm

* Some commercial Linux distributions (e.g., RHEL, SLE) may already
  include up-to-date Ceph packages.  In that case, you can install
  cephadm directly.  For example::

    # dnf install -y cephadm     # or
    # zypper install -y cephadm



Bootstrap a new cluster
=======================

You need to know which *IP address* to use for the cluster's first
monitor daemon.  This is normally just the IP for the first host.  If there
are multiple networks and interfaces, be sure to choose one that will
be accessible by any host accessing the Ceph cluster.

To bootstrap the cluster::

  # mkdir -p /etc/ceph
  # cephadm bootstrap --mon-ip *<mon-ip>*

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

* Bootstrap writes the files needed to access the new cluster to
  ``/etc/ceph`` for convenience, so that any Ceph packages installed
  on the host itself (e.g., to access the command line interface) can
  easily find them.

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
  under ``/mnt`` inside the container::

    # cephadm shell

* It may be helpful to create an alias::

    # alias ceph='cephadm shell -- ceph'

* You can install the ``ceph-common`` package, which contains all of the
  ceph commands, including ``ceph``, ``rbd``, ``mount.ceph`` (for mounting
  CephFS file systems), etc.::

    # cephadm add-repo --release octopus
    # cephadm install ceph-common

Confirm that the ``ceph`` command is accessible with::

  # ceph -v

Confirm that the ``ceph`` command can connect to the cluster and also
its status with::

  # ceph status


Add hosts to the cluster
========================

To add each new host to the cluster, perform two steps:

#. Install the cluster's public SSH key in the new host's root user's
   ``authorized_keys`` file::

     # ssh-copy-id -f -i /etc/ceph/ceph.pub root@*<new-host>*

   For example::

     # ssh-copy-id -f -i /etc/ceph/ceph.pub root@host2
     # ssh-copy-id -f -i /etc/ceph/ceph.pub root@host3

#. Tell Ceph that the new node is part of the cluster::

     # ceph orch host add *newhost*

   For example::

     # ceph orch host add host2
     # ceph orch host add host3


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
  can configure that in `CIDR`_ format (e.g., ``10.1.2.0/24``) with::

    # ceph config set mon public_network *<mon-cidr-network>*

  For example::

    # ceph config set mon public_network 10.1.2.0/24

  Cephadm only deploys new monitor daemons on hosts that have IPs
  configured in the configured subnet.

* If you want to adjust the default of 5 monitors::

    # ceph orch apply mon *<number-of-monitors>*

* To deploy monitors on a specific set of hosts::

    # ceph orch apply mon *<host1,host2,host3,...>*

  Be sure to include the first (bootstrap) host in this list.

* You can control which hosts the monitors run on by making use of
  host labels.  To set the ``mon`` label to the appropriate
  hosts::

    # ceph orch host label add *<hostname>* mon

  To view the current hosts and labels::

    # ceph orch host ls

  For example::

    # ceph orch host label add host1 mon
    # ceph orch host label add host2 mon
    # ceph orch host label add host3 mon
    # ceph orch host ls
    HOST   ADDR   LABELS  STATUS
    host1         mon
    host2         mon
    host3         mon
    host4
    host5

  Tell cephadm to deploy monitors based on the label::

    # ceph orch apply mon label:mon

* You can explicitly specify the IP address or CIDR network for each monitor
  and control where it is placed.  To disable automated monitor deployment::

    # ceph orch apply mon --unmanaged

  To deploy each additional monitor::

    # ceph orch daemon add mon *<host1:ip-or-network1> [<host1:ip-or-network-2>...]*

  For example, to deploy a second monitor on ``newhost1`` using an IP
  address ``10.1.2.123`` and a third monitor on ``newhost2`` in
  network ``10.1.2.0/24``::

    # ceph orch apply mon --unmanaged
    # ceph orch daemon add mon newhost1:10.1.2.123
    # ceph orch daemon add mon newhost2:10.1.2.0/24


Deploy OSDs
===========

An inventory of storage devices on all cluster hosts can be displayed with::

  # ceph orch device ls

A storage device is considered *available* if all of the following
conditions are met:

* The device must have no partitions.
* The device must not have any LVM state.
* The device must not be mounted.
* The device must not contain a file system.
* The device must not contain a Ceph BlueStore OSD.
* The device must be larger than 5 GB.

Ceph refuses to provision an OSD on a device that is not available.

There are a few ways to create new OSDs:

* Tell Ceph to consume any available and unused storage device::

    # ceph orch apply osd --all-available-devices

* Create an OSD from a specific device on a specific host::

    # ceph orch daemon add osd *<host>*:*<device-path>*

  For example::

    # ceph orch daemon add osd host1:/dev/sdb

* Use :ref:`drivegroups` to describe device(s) to consume
  based on their properties, such device type (SSD or HDD), device
  model names, size, or the hosts on which the devices exist::

    # ceph orch apply osd -i spec.yml


Deploy MDSs
===========

One or more MDS daemons is required to use the CephFS file system.
These are created automatically if the newer ``ceph fs volume``
interface is used to create a new file system.  For more information,
see :ref:`fs-volumes-and-subvolumes`.

To deploy metadata servers::

  # ceph orch apply mds *<fs-name>* --placement="*<num-daemons>* [*<host1>* ...]"

See :ref:`orchestrator-cli-placement-spec` for details of the placement specification.

Deploy RGWs
===========

Cephadm deploys radosgw as a collection of daemons that manage a
particular *realm* and *zone*.  (For more information about realms and
zones, see :ref:`multisite`.)

Note that with cephadm, radosgw daemons are configured via the monitor
configuration database instead of via a `ceph.conf` or the command line.  If
that configuration isn't already in place (usually in the
``client.rgw.<realmname>.<zonename>`` section), then the radosgw
daemons will start up with default settings (e.g., binding to port
80).

If a realm has not been created yet, first create a realm::

  # radosgw-admin realm create --rgw-realm=<realm-name> --default

Next create a new zonegroup::

  # radosgw-admin zonegroup create --rgw-zonegroup=<zonegroup-name>  --master --default

Next create a zone::

  # radosgw-admin zone create --rgw-zonegroup=<zonegroup-name> --rgw-zone=<zone-name> --master --default

To deploy a set of radosgw daemons for a particular realm and zone::

  # ceph orch apply rgw *<realm-name>* *<zone-name>* --placement="*<num-daemons>* [*<host1>* ...]"

For example, to deploy 2 rgw daemons serving the *myorg* realm and the *us-east-1*
zone on *myhost1* and *myhost2*::

  # radosgw-admin realm create --rgw-realm=myorg --default
  # radosgw-admin zonegroup create --rgw-zonegroup=default --master --default
  # radosgw-admin zone create --rgw-zonegroup=default --rgw-zone=us-east-1 --master --default
  # ceph orch apply rgw myorg us-east-1 --placement="2 myhost1 myhost2"

See :ref:`orchestrator-cli-placement-spec` for details of the placement specification.

Deploying NFS ganesha
=====================

Cephadm deploys NFS Ganesha using a pre-defined RADOS *pool*
and optional *namespace*

To deploy a NFS Ganesha gateway,::

  # ceph orch apply nfs *<svc_id>* *<pool>* *<namespace>* --placement="*<num-daemons>* [*<host1>* ...]"

For example, to deploy NFS with a service id of *foo*, that will use the
RADOS pool *nfs-ganesha* and namespace *nfs-ns*,::

  # ceph orch apply nfs foo nfs-ganesha nfs-ns

.. note::
   Create the *nfs-ganesha* pool first if it doesn't exist.

See :ref:`orchestrator-cli-placement-spec` for details of the placement specification.

Deploying custom containers
===========================
It is also possible to choose different containers than the default containers to deploy Ceph. See :ref:`containers` for information about your options in this regard. 
