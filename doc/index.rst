=================
 Welcome to Ceph
=================

Ceph is a storage platform. 

Ceph makes possible object storage, block storage, and file storage. It can be used to build cloud infrastructure and web-scale object storage. 

The procedure on this page explains how to set up a three-node Ceph cluster, the most basic of setups.  

Basic Three-Node Installation Procedure
=======================================
.. highlight:: console 


Installing the First Node
-------------------------

1. Install a recent, supported Linux distribution on a computer.
2. Install podman or docker. On Fedora or Centos::

   $ sudo dnf install podman

  on Ubuntu or Debian::
  
   $ sudo apt install docker.io 

3.  Fetch the cephadm utility from github to the computer that will be the Ceph manager::

    $ curl --silent --remote-name --location https://github.com/ceph/ceph/raw/master/src/cephadm/cephadm
   

4. Make the cephadm utility executable::

   $ sudo chmod +x cephadm

#. Find the ip address of the node that will become the first Ceph monitor::

   $ ip addr

#. Using the ip address that you discovered in the step immediately prior to this step, run the following command::

   $ sudo ./cephadm bootstrap --mon-ip 192.168.1.101 --output-pub-ssh-key ceph.pub 
   
  The output of a successful execution of this command is shown here::

   INFO:root:Cluster fsid: 335b6dac-064c-11ea-8243-48f17fe53909
   INFO:cephadm:Verifying we can ping mon IP 192.168.1.101...
   INFO:cephadm:Pulling latest ceph/daemon-base:latest-master-devel container...
   INFO:cephadm:Extracting ceph user uid/gid from container image...
   INFO:cephadm:Creating initial keys...
   INFO:cephadm:Creating initial monmap...
   INFO:cephadm:Creating mon...
   INFO:cephadm:Waiting for mon to start...
   INFO:cephadm:Assimilating anything we can from ceph.conf...
   INFO:cephadm:Generating new minimal ceph.conf...
   INFO:cephadm:Restarting the monitor...
   INFO:cephadm:Creating mgr...
   INFO:cephadm:Creating crash agent...
   Created symlink /etc/systemd/system/ceph-335b6dac-064c-11ea-8243-48f17fe53909.target.wants/
   ceph-335b6dac-064c-11ea-8243-48f17fe53909-crash.service → /etc/systemd/system/ceph-335b6dac-
   064c-11ea-8243-48f17fe53909-crash.service.
   INFO:cephadm:Wrote keyring to ceph.client.admin.keyring
   INFO:cephadm:Wrote config to ceph.conf
   INFO:cephadm:Waiting for mgr to start...
   INFO:cephadm:mgr is still not available yet, waiting...
   INFO:cephadm:mgr is still not available yet, waiting...
   INFO:cephadm:Generating ssh key...
   INFO:cephadm:Wrote public SSH key to to ceph.pub
   INFO:cephadm:Adding key to root@localhost's authorized_keys...
   INFO:cephadm:Enabling ssh module...
   INFO:cephadm:Setting orchestrator backend to ssh...
   INFO:cephadm:Adding host 192-168-1-101.tpgi.com.au...
   INFO:cephadm:Enabling the dashboard module...
   INFO:cephadm:Waiting for the module to be available...
   INFO:cephadm:Generating a dashboard self-signed certificate...
   INFO:cephadm:Creating initial admin user...
   INFO:cephadm:Fetching dashboard port number...
   INFO:cephadm:Ceph Dashboard is now available at:
   URL: https://192-168-1-101.tpgi.com.au:8443/
   User: admin
   Password: oflamlrtna
   INFO:cephadm:You can access the Ceph CLI with:
   sudo ./cephadm shell -c ceph.conf -k ceph.client.admin.keyring
   INFO:cephadm:Bootstrap complete.


Second Node
-----------

#. Install a recent, supported Linux distribution on a second computer.
#. Install docker. On Fedora or Centos::

   $ sudo dnf install podman

  on Ubuntu or Debian::

   $ sudo apt install docker.io

3. Turn on ssh on node 2::

    $ sudo systemctl start sshd
    $ sudo systemctl enable sshd
#. Create a file on node 2 that will hold the ceph public key::

    $ sudo mkdir -p /root/.ssh
    $ sudo touch /root/.ssh/authorized_keys
#. Copy the public key from node 1 to node 2::

    [node 1] $ sudo cat ceph.pub | ssh root@192.168.1.102 tee -a /root/.ssh/authorized_keys

#. On node 1, issue the command that adds node 2 to the cluster::

    [node 1] $ sudo ./cephadm shell ceph orchestrator host add 192.168.1.102

Third Node
----------
#. Install a recent, supported Linux distribution on a third computer.

#. Install docker. On Fedora or Centos::

   $ sudo dnf install podman

  on Ubuntu or Debian::

   $ sudo apt install docker.io

3. Turn on ssh on node 3::

    $ sudo systemctl start sshd
    $ sudo systemctl enable sshd
#. Create a file on node 3 that will hold the ceph public key::

    $ sudo mkdir -p /root/.ssh
    $ sudo touch /root/.ssh/authorized_keys

#. Copy the public key from node 1 to node 3::

    [node 1] $ sudo cat ceph.pub | ssh root@192.168.1.103 tee -a /root/.ssh/authorized_keys

#. On node 1, issue the command that adds node 3 to the cluster::

    [node 1] $ sudo ./cephadm shell ceph orchestrator host add 192.168.1.103


Creating Two More Monitors
--------------------------

#. Set up a Ceph monitor on node 2 by issuing the following command on node 1.  ::

   [node 1] $ sudo ./cephadm shell ceph orchestrator mon update 2 192.168.1.102:192.168.1.0/24
   ["(Re)deployed mon 192.168.1.102 on host '192.168.1.102'"]

#. Set up a Ceph monitor on node 3 by issuing the following command on node 1::

   [node 1] $ sudo ./cephadm shell ceph orchestrator mon update 3 192.168.1.103:192.168.1.0/24
   ["(Re)deployed mon 192.168.1.103 on host '192.168.1.103'"]



Creating OSDs
-------------

Creating an OSD on the First Node
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Use a command of the following form to create an OSD on node 1::

   [node 1@192-168-1-101]$ sudo ./cephadm shell ceph orchestrator osd create 192-168-1-101:/dev/by-id/ata-WDC+WDS_300T2C0A-00SM50_123405928343
   ["Created osd(s) on host '192-168-1-101'"]
   [node 1@192-168-1-101]$


Creating an OSD on the Second Node
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Use a command of the following form ON NODE 1 to create an OSD on node 2::

   [node 1@192-168-1-101]$ sudo ./cephadm shell ceph orchestrator osd create 192-168-1-102:/dev/by-id/ata-WDC+WDS_300T2C0A-00SM50_123405928383
   ["Created osd(s) on host '192-168-1-102'"]
   [node 1@192-168-1-101]$


Creating an OSD on the Third Node
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Use a command of the following form ON NODE 1 to create an OSD on node 3::

   [node 1@192-168-1-101]$ sudo ./cephadm shell ceph orchestrator osd create 192-168-1-103:/dev//dev/by-id/ata-WDC+WDS_300T2C0A-00SM50_123405928384
   ["Created osd(s) on host '192-168-1-103'"]
   [node 1@192-168-1-101]$


Confirming Successful Installation
----------------------------------

#. Run the following command on node 1 in order to enter the Ceph shell::

   [node 1]$ sudo cephadm shell
#. From within the Ceph shell, run "ceph status". Confirm that the following exist:

  1) a cluster
  2) three monitors
  3) 3 osds 

  ::

   [ceph: root@192-168-1-101 /]# ceph status
   cluster:
   id: 335b6dac-064c-11ea-8243-48f17fe53909
   health: HEALTH_OK
   services:
   mon: 3 daemons, quorum 192-168-1-101,192.168.1.102,192.168.1.103 (age 29h)
   mgr: 192-168-1-101(active, since 2d)
   osd: 3 osds: 3 up (since 67s), 3 in (since 67s)
   data:
   pools: 0 pools, 0 pgs
   objects: 0 objects, 0 B
   usage: 3.0 GiB used, 82 GiB / 85 GiB avail
   pgs:
   [ceph: root@192-168-1-101 /]#



Ceph uniquely delivers **object, block, and file storage in one unified
system**.

.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Ceph Object Store</h3>

- RESTful Interface
- S3- and Swift-compliant APIs
- S3-style subdomains
- Unified S3/Swift namespace
- User management
- Usage tracking
- Striped objects
- Cloud solution integration
- Multi-site deployment
- Multi-site replication

.. raw:: html

	</td><td><h3>Ceph Block Device</h3>


- Thin-provisioned
- Images up to 16 exabytes
- Configurable striping
- In-memory caching
- Snapshots
- Copy-on-write cloning
- Kernel driver support
- KVM/libvirt support
- Back-end for cloud solutions
- Incremental backup
- Disaster recovery (multisite asynchronous replication)

.. raw:: html

	</td><td><h3>Ceph File System</h3>

- POSIX-compliant semantics
- Separates metadata from data
- Dynamic rebalancing
- Subdirectory snapshots
- Configurable striping
- Kernel driver support
- FUSE support
- NFS/CIFS deployable
- Use with Hadoop (replace HDFS)

.. raw:: html

	</td></tr><tr><td>

See `Ceph Object Store`_ for additional details.

.. raw:: html

	</td><td>

See `Ceph Block Device`_ for additional details.

.. raw:: html

	</td><td>

See `Ceph File System`_ for additional details.

.. raw::	html

	</td></tr></tbody></table>

Ceph is highly reliable, easy to manage, and free. The power of Ceph
can transform your company's IT infrastructure and your ability to manage vast
amounts of data. To try Ceph, see our `Getting Started`_ guides. To learn more
about Ceph, see our `Architecture`_ section.



.. _Ceph Object Store: radosgw
.. _Ceph Block Device: rbd
.. _Ceph File System: cephfs
.. _Getting Started: start
.. _Architecture: architecture

.. toctree::
   :maxdepth: 3
   :hidden:

   start/intro
   bootstrap
   start/index
   install/index
   rados/index
   cephfs/index
   rbd/index
   radosgw/index
   mgr/index
   mgr/dashboard
   api/index
   architecture
   Developer Guide <dev/developer_guide/index>
   dev/internals
   governance
   foundation
   ceph-volume/index
   releases/general
   releases/index
   Glossary <glossary>
