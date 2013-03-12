======================
 5-minute Quick Start
======================

Thank you for trying Ceph! Petabyte-scale data clusters are quite an
undertaking. Before delving deeper into Ceph, we recommend setting up a two-node
demo cluster to explore some of the functionality. The Ceph **5-Minute Quick
Start** deploys a Ceph object store cluster on one server machine and a Ceph
client on a separate machine, each with a recent Debian/Ubuntu operating system.
The intent of this **Quick Start** is to help you exercise Ceph object store
functionality without the configuration and deployment overhead associated with
a production-ready object store cluster. Once you complete this quick start, you
may exercise Ceph commands on the command line. You may also proceed to the
quick start guides for block devices, CephFS filesystems, and the RESTful
gateway.

.. ditaa:: 
           /----------------\         /----------------\
           | Client Machine |<------->| Server Machine |
           | cCCC           |         | cCCC           |
           +----------------+         +----------------+
           |  Ceph Commands |         |   ceph - mon   |
           +----------------+         +----------------+
           |  Block Device  |         |   ceph - osd   |
           +----------------+         +----------------+
           |    Ceph FS     |         |   ceph - mds   |  
           \----------------/         \----------------/


Install Debian/Ubuntu
=====================

Install a recent release of Debian or Ubuntu (e.g., 12.04 precise) on your
Ceph server machine and your client machine. 


Add Ceph Packages
=================

To get the latest Ceph packages, add a release key to :abbr:`APT (Advanced
Package Tool)`, add a source location to the ``/etc/apt/sources.list`` on your
Ceph server and client machines, update your systems and install Ceph. :: 

	wget -q -O- 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc' | sudo apt-key add -	
	echo deb http://ceph.com/debian/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list
	sudo apt-get update && sudo apt-get install ceph
	
Check the Ceph version you are using and make a note of it so that you have the 
correct settings in your configuration file::

   ceph -v
   
If ``ceph -v`` reflects an earlier version from what you installed, your
``ceph-common`` library may be using the version distributed with the kernel.
Once you've installed Ceph, you may also update and upgrade your packages to 
ensure you have the latest ``ceph-common`` library installed. :: 

	sudo apt-get update && sudo apt-get upgrade

If you want to use a version other than the current release, see `Installing 
Debian/Ubuntu Packages`_ for further details.

.. _Installing Debian/Ubuntu Packages: ../../install/debian
	

Add a Configuration File
========================

The example configuration file will configure Ceph to operate a monitor, two OSD
daemons and one metadata server on your Ceph server machine. To add a
configuration file to Ceph, we suggest copying the contents of the example file
below to an editor. Then, follow the steps below to modify it.

.. literalinclude:: ceph.conf
   :language: ini

#. Open a command line on your Ceph server machine and execute ``hostname -s``
   to retrieve the name of your Ceph server machine. 

#. Replace ``{hostname}`` in the sample configuration file with your host name. 

#. Execute ``ifconfig`` on the command line of your Ceph server machine to 
   retrieve the IP address of your Ceph server machine.

#. Replace ``{ip-address}`` in the sample configuration file with the IP 
   address of your Ceph server host. 

#. Save the contents to ``/etc/ceph/ceph.conf`` on Ceph server host.

#. Copy the configuration file to ``/etc/ceph/ceph.conf`` on your client host. ::

	sudo scp {user}@{server-machine}:/etc/ceph/ceph.conf /etc/ceph/ceph.conf
	
.. tip:: Ensure the ``ceph.conf`` file has appropriate permissions set 
   (e.g. ``chmod 644``) on your client machine.

.. versionadded:: 0.55

Ceph v0.55 and above have authentication enabled by default. You should 
explicitly enable or disable authentication with version 0.55 and above.
The example configuration provides ``auth`` entries for authentication.
For details on Ceph authentication see `Cephx Configuration Reference`_ 
and `Cephx Guide`_.

.. _Cephx Guide: ../../rados/operations/authentication
.. _Cephx Configuration Reference: ../../rados/configuration/auth-config-ref


Deploy the Configuration
========================

You must perform the following steps to deploy the configuration. 

#. On your Ceph server host, create a directory for each daemon. For the 
   example configuration, execute the following::

	sudo mkdir -p /var/lib/ceph/osd/ceph-0
	sudo mkdir -p /var/lib/ceph/osd/ceph-1
	sudo mkdir -p /var/lib/ceph/mon/ceph-a
	sudo mkdir -p /var/lib/ceph/mds/ceph-a


#. Execute the following on the Ceph server host:: 

	cd /etc/ceph
	sudo mkcephfs -a -c /etc/ceph/ceph.conf -k ceph.keyring
	

Among other things, ``mkcephfs`` will deploy Ceph and generate a
``client.admin`` user and key. For Bobtail and subsequent versions (v 0.56 and
after), the ``mkcephfs`` script will create and mount the filesystem for you
provided you specify ``osd mkfs`` ``osd mount`` and ``devs`` settings in your
Ceph configuration file. 


Start Ceph
==========

Once you have deployed the configuration, start Ceph from the command line of
your server machine. :: 

	sudo service ceph -a start
	
Check the health of your Ceph cluster to ensure it is ready. :: 

	sudo ceph health
	
When your cluster echoes back ``HEALTH_OK``, you may begin using Ceph.


Copy The Keyring to The Client
==============================

The next step you must perform is to copy ``/etc/ceph/ceph.keyring``, which
contains the ``client.admin`` key, from the server machine to the client
machine. If you don't perform this step, you will not be able to use the Ceph
command line, as the example Ceph configuration requires authentication. ::

	sudo scp {user}@{server-machine}:/etc/ceph/ceph.keyring /etc/ceph/ceph.keyring

.. tip:: Ensure the ``ceph.keyring`` file has appropriate permissions set 
   (e.g., ``chmod 644``) on your client machine.
   

Proceed to Other Quick Starts
=============================

Once you have Ceph running with both a client and a server, you 
may proceed to the other Quick Start guides. 

#. For Ceph block devices, proceed to `Block Device Quick Start`_.

#. For the CephFS filesystem, proceed to `CephFS Quick Start`_.

#. For the RESTful Gateway, proceed to `Gateway Quick Start`_.

.. _Block Device Quick Start: ../quick-rbd
.. _CephFS Quick Start: ../quick-cephfs
.. _Gateway Quick Start: ../quick-rgw