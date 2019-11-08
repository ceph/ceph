============================
 Installation (ceph-daemon)
============================

A new Ceph cluster is deployed by bootstrapping a cluster on a single
node, and then adding additional nodes and daemons via the CLI or GUI
dashboard.

Get ceph-daemon
===============

The ``ceph-daemon`` utility is used to bootstrap a new Ceph Cluster.
You can get the utility by either installing a package provided by
your Linux distribution::

  sudo apt install -y ceph-daemon   # or,
  sudo dnf install -y ceph-daemon   # or,
  sudo yum install -y ceph-daemon

or by simply downloading the standalone script manually::

  curl --silent --remote-name --location https://github.com/ceph/ceph/raw/master/src/ceph-daemon/ceph-daemon
  chmod +x ceph-daemon
  sudo install -m 0755 ceph-daemon /usr/sbin    # optional!

Bootstrap a new cluster
=======================

To create a new cluster, you need to know:

* Which *IP address* to use for the cluster's first monitor.  This is
  normally just the IP for the first cluster node.  If there are
  multiple networks and interfaces, be sure to choose one that will be
  accessible by any hosts accessing the Ceph cluster.

To bootstrap the cluster,::

  sudo ceph-daemon bootstrap --mon-ip *<mon-ip>* --output-config ceph.conf --output-keyring ceph.keyring --output-pub-ssh-key ceph.pub

This command does a few things:

* Creates a monitor and manager daemon for the new cluster on the
  local host.  A minimal configuration file needed to communicate with
  the new cluster is written to ``ceph.conf`` in the local directory.
* A copy of the ``client.admin`` administrative (privileged!) secret
  key is written to ``ceph.keyring`` in the local directory.
* Generates a new SSH key, and adds the public key to the local root user's
  ``/root/.ssh/authorized_keys`` file.  A copy of the public key is written
  to ``ceph.pub`` in the local directory.

Interacting with the cluster
============================

You can easily start up a container that has all of the Ceph packages
installed to interact with your cluster::

  sudo ceph-daemon shell --config ceph.conf --keyring ceph.keyring

The ``--config`` and ``--keyring`` arguments will bind those local
files to the default locations in ``/etc/ceph`` inside the container
to allow the ``ceph`` CLI utility to work without additional
arguments.  Inside the container, you can check the cluster status with::

  ceph status

In order to interact with the Ceph cluster outside of a container, you
need to install the Ceph client packages and install the configuration
and privileged administrator key in a global location::

  sudo apt install -y ceph-common   # or,
  sudo dnf install -y ceph-common   # or,
  sudo yum install -y ceph-common

  sudo install -m 0644 ceph.conf /etc/ceph/ceph.conf
  sudo install -m 0600 ceph.keyring /etc/ceph/ceph.keyring

Adding hosts to the cluster
===========================

For each new host you'd like to add to the cluster, you need to do two things:

#. Install the cluster's public SSH key in the new host's root user's
   ``authorized_keys`` file.  For example,::

     cat ceph.pub | ssh root@*newhost* tee -a /root/.ssh/authorized_keys

#. Tell Ceph that the new node is part of the cluster::

     ceph orchestrator host add *newhost*

Deploying additional monitors
=============================

Normally a Ceph cluster has at least three (or, preferably, five)
monitor daemons spread across different hosts.  Since we are deploying
a monitor, we again need to specify what IP address it will use,
either as a simple IP address or as a CIDR network name.

To deploy additional monitors,::

  ceph orchestrator mon update *<new-num-monitors>* *<host1:network1> [<host1:network2>...]*

For example, to deploy a second monitor on ``newhost`` using an IP
address in network ``10.1.2.0/24``,::

  ceph orchestrator mon update 2 newhost:10.1.2.0/24

Deploying OSDs
==============

To add an OSD to the cluster, you need to know the device name for the
block device (hard disk or SSD) that will be used.  Then,::

  ceph orchestrator osd create *<host>*:*<path-to-device>*

For example, to deploy an OSD on host *newhost*'s SSD,::

  ceph orchestrator osd create newhost:/dev/disk/by-id/ata-WDC_WDS200T2B0A-00SM50_182294800028

Deploying manager daemons
=========================

It is a good idea to have at least one backup manager daemon.  To
deploy one or more new manager daemons,::

  ceph orchestrator mgr update *<new-num-mgrs>* [*<host1>* ...]

Deploying MDSs
==============

In order to use the CephFS file system, one or more MDS daemons is needed.

TBD
