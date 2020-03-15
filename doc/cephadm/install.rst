============================
Deploying a new Ceph cluster
============================

Cephadm can create a new Ceph cluster by "bootstrapping" on a single
host, expanding the cluster to encompass any additional
hosts, and deploying the needed services.

The following instructions install a basic multi-node cluster.  Commands
may be prefixed by the host that they need to be run on. For example,
``host1`` identifies the first host, ``host2`` identifies the second
host, and so on. This information is provided in order to make clear
which commands should be issued on which systems.  If there is no
explicit prefix, then the command be run anywhere the ``ceph``
command is available.

.. highlight:: console


Requirements
============

- Systemd
- Podman or Docker for running containers.
- Time synchronization (such as chrony or NTP)
- LVM2 for provisioning storage devices

Any modern Linux distribution should be sufficient.  Dependencies
are installed automatically by the bootstrap process below.


Get cephadm
===========

The ``cephadm`` command is used (1) to bootstrap a new cluster, (2) to
access a containerized shell with a working Ceph CLI, and (3) to work
with containerized Ceph daemons when debugging issues.

You can use ``curl`` to fetch the most recent version of the standalone script::

  host1$ curl --silent --remote-name --location https://github.com/ceph/ceph/raw/octopus/src/cephadm/cephadm
  host1$ chmod +x cephadm

You may also be able to get cephadm by installing a package
provided by your Linux distribution::

  host1$ sudo apt install -y cephadm     # or
  host1$ sudo dnf install -y cephadm     # or
  host1$ sudo yum install -y cephadm     # or
  host1$ sudo zypper install -y cephadm



Bootstrap a new cluster
=======================

You need to know which *IP address* to use for the cluster's first
monitor.  This is normally just the IP for the first cluster node.  If
there are multiple networks and interfaces, be sure to choose one that
will be accessible by any hosts accessing the Ceph cluster.

To bootstrap the cluster run the following commands::

  host1$ sudo ./cephadm bootstrap --mon-ip *<mon-ip>*

This command does a few things:

* A monitor and manager daemon for the new cluster are created on the
  local host.  A minimal configuration file needed to communicate with
  the new cluster is written to ``ceph.conf`` in the current directory.
* A copy of the ``client.admin`` administrative (privileged!) secret
  key is written to ``ceph.client.admin.keyring`` in the current directory.
* A new SSH key is generated for the Ceph cluster and is added to the
  root user's ``/root/.ssh/authorized_keys`` file.  A copy of the
  public key is written to ``ceph.pub`` in the current directory.

.. tip::

   If you run the bootstrap command from ``/etc/ceph``, the cluster's new
   keys are written to a standard location.  For example,::

     host1$ sudo mkdir -p /etc/ceph
     host1$ cd /etc/ceph
     host1$ sudo /path/to/cephadm bootstrap --mon-ip *<mon-ip>*


Interacting with the cluster
============================

To interact with your cluster via the command-line interface, start up
a container that has all of the Ceph packages (including the ``ceph``
command) installed::

  host1$ sudo ./cephadm shell --config ceph.conf --keyring ceph.client.admin.keyring

Inside the container, you can check the cluster status with::

  [ceph: root@host1 /]# ceph status

In order to interact with the Ceph cluster outside of a container
(that is, from the host's command line), install the Ceph
client packages and install the configuration and privileged
administrator key in a global location::

  host1$ sudo apt install -y ceph-common   # or,
  host1$ sudo dnf install -y ceph-common   # or,
  host1$ sudo yum install -y ceph-common

  host1$ sudo install -m 0644 ceph.conf /etc/ceph/ceph.conf
  host1$ sudo install -m 0600 ceph.keyring /etc/ceph/ceph.keyring


Adding hosts to the cluster
===========================

For each new host you'd like to add to the cluster, you need to do two things:

#. Install the cluster's public SSH key in the new host's root user's
   ``authorized_keys`` file::

     host1$ sudo ssh-copy-id -f -i ceph.pu root@*<new-host>*

   For example::

     host1$ sudo ssh-copy-id -f -i ceph.pu root@host2
     host1$ sudo ssh-copy-id -f -i ceph.pu root@host3

#. Tell Ceph that the new node is part of the cluster::

     # ceph orch host add *newhost*

   For example::

     # ceph orch host add host2
     # ceph orch host add host3

Deploying additional monitors
=============================

Normally a Ceph cluster has three or five monitor daemons spread
across different hosts.  As a rule of thumb, you should deploy five
monitors if there are five or more nodes in your cluster.

.. _CIDR: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation

If all of your monitors will exist on the same IP subnet, cephadm can
automatically scale the number of monitors.  This subnet should be
specified in `CIDR`_ format (e.g., ``10.1.2.0/24``).  (If you do not
specify a subnet, you will need to manually specify an IP or subnet
when creating each monitor.)::

  # ceph config set mon public_network *<mon-cidr-network>*

For example::

  # ceph config set mon public_network 10.1.2.0/24

There are several ways to add additional monitors:

* You can simply tell cephadm how many monitors you want, and it will pick the
  hosts (randomly)::

    # ceph orch apply mon *<number-of-monitors>*

  For example, if you have 5 or more hosts added to the cluster,::

    # ceph orch apply mon 5

* You can explicitly specify which hosts to deploy on.  Be sure to include
  the first monitor host in this list.::

    # ceph orch apply mon *<host1,host2,host3,...>*

  For example,::

    # ceph orch apply mon host1,host2,host3

* You can control which hosts the monitors run on by adding the ``mon`` label
  to the appropriate hosts::

    # ceph orch host label add *<hostname>* mon

  To view the current hosts and labels,::

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

  Then tell cephadm to deploy monitors based on the label::

    # ceph orch apply mon label:mon

* You can explicitly specify the IP address or CIDR for each monitor
  and control where it is placed.  This is the only supported method
  if you did not specify the CIDR monitor network above.

  To deploy additional monitors,::

    # ceph orch daemon add mon *<host1:ip-or-network1> [<host1:ip-or-network-2>...]*

  For example, to deploy a second monitor on ``newhost1`` using an IP
  address ``10.1.2.123`` and a third monitor on ``newhost2`` in
  network ``10.1.2.0/24``,::

    # ceph orch daemon add mon newhost1:10.1.2.123
    # ceph orch daemon add mon newhost2:10.1.2.0/24

Deploying OSDs
==============

To add OSDs to the cluster, you have two options:

#. You need to know the device name for the block device (hard disk or
SSD) that will be used.  Then,::

     # ceph orch osd create *<host>*:*<path-to-device>*

   For example, to deploy an OSD on host *newhost*'s SSD,::

     # ceph orch osd create newhost:/dev/disk/by-id/ata-WDC_WDS200T2B0A-00SM50_182294800028


#. You need to describe your disk setup by it's properties (Drive Groups)

   Link to DriveGroup docs.::

    # ceph orch osd create -i my_drivegroups.yml


.. _drivegroups: drivegroups::

Deploying manager daemons
=========================

It is a good idea to have at least one backup manager daemon.  To
deploy one or more new manager daemons,::

  # ceph orch apply mgr *<new-num-mgrs>* [*<host1>* ...]

Deploying MDSs
==============

One or more MDS daemons is required to use the CephFS file system.
These are created automatically if the newer ``ceph fs volume``
interface is used to create a new file system.  For more information,
see :ref:`fs-volumes-and-subvolumes`.

To deploy metadata servers,::

  # ceph orch apply mds *<fs-name>* *<num-daemons>* [*<host1>* ...]

Deploying RGWs
==============

Cephadm deploys radosgw as a collection of daemons that manage a
particular *realm* and *zone*.  (For more information about realms and
zones, see :ref:`multisite`.)  To deploy a set of radosgw daemons for
a particular realm and zone,::

  # ceph orch apply rgw *<realm-name>* *<zone-name>* *<num-daemons>* [*<host1>* ...]

Note that with cephadm, radosgw daemons are configured via the monitor
configuration database instead of via a `ceph.conf` or the command line.  If
that confiruation isn't already in place (usually in the
``client.rgw.<realmname>.<zonename>`` section), then the radosgw
daemons will start up with default settings (e.g., binding to port
80).
