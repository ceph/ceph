=============================
 Deploying with ``mkcephfs``
=============================

To deploy a test or development cluster, you can use the ``mkcephfs`` tool.
We do not recommend using this tool for production environments.


Enable Login to Cluster Hosts as ``root``
=========================================

To deploy with ``mkcephfs``, you will need to be able to login as ``root``
on each host without a password. For each host, perform the following:: 

	sudo passwd root

Enter a password for the root user. 

On the admin host, generate an ``ssh`` key without specifying a passphrase
and use the default locations. :: 

	ssh-keygen
	Generating public/private key pair.
	Enter file in which to save the key (/ceph-admin/.ssh/id_rsa): 
	Enter passphrase (empty for no passphrase): 
	Enter same passphrase again: 
	Your identification has been saved in /ceph-admin/.ssh/id_rsa.
	Your public key has been saved in /ceph-admin/.ssh/id_rsa.pub.

You may use RSA or DSA keys. Once you generate your keys, copy them to each 
OSD host. For example:: 

	ssh-copy-id root@myserver01
	ssh-copy-id root@myserver02	
	
Modify your ``~/.ssh/config`` file to login as ``root``, as follows:: 

	Host myserver01
		Hostname myserver01.fully-qualified-domain.com
		User root
	Host myserver02
		Hostname myserver02.fully-qualified-domain.com
		User root


Copy Configuration File to All Hosts
====================================

Ceph's ``mkcephfs`` deployment script does not copy the configuration file you
created from the Administration host to the OSD Cluster hosts. Copy the
configuration file you created (*i.e.,* ``mycluster.conf`` in the example below)
from the Administration host to ``etc/ceph/ceph.conf`` on each OSD Cluster host
if you are using ``mkcephfs`` to deploy Ceph.

::

	ssh myserver01 sudo tee /etc/ceph/ceph.conf < /etc/ceph/ceph.conf
	ssh myserver02 sudo tee /etc/ceph/ceph.conf < /etc/ceph/ceph.conf
	ssh myserver03 sudo tee /etc/ceph/ceph.conf < /etc/ceph/ceph.conf


Create the Default Directories
==============================

The ``mkcephfs`` deployment script does not create the default server
directories.  Create server directories for each instance of a Ceph daemon (if
you haven't done so already). The ``host``  variables in the ``ceph.conf`` file
determine which host runs each instance of  a Ceph daemon. Using the exemplary
``ceph.conf`` file, you would perform  the following:

On ``myserver01``::

	sudo mkdir /var/lib/ceph/osd/ceph-0
	sudo mkdir /var/lib/ceph/mon/ceph-a

On ``myserver02``::

	sudo mkdir /var/lib/ceph/osd/ceph-1
	sudo mkdir /var/lib/ceph/mon/ceph-b

On ``myserver03``::

	sudo mkdir /var/lib/ceph/osd/ceph-2
	sudo mkdir /var/lib/ceph/mon/ceph-c
	sudo mkdir /var/lib/ceph/mds/ceph-a


Mount Disks to the Data Directories
===================================

If you are running multiple OSDs per host and one hard disk per OSD,  you should
mount the disk under the OSD data directory (if you haven't done so already).


Run ``mkcephfs``
================

Once you have copied your Ceph Configuration to the OSD Cluster hosts
and created the default directories, you may deploy Ceph with the 
``mkcephfs`` script.

.. note::  ``mkcephfs`` is a quick bootstrapping tool. It does not handle more 
           complex operations, such as upgrades.

For production environments, deploy Ceph using Chef cookbooks. To run 
``mkcephfs``, execute the following:: 

   cd /etc/ceph
   sudo mkcephfs -a -c /etc/ceph/ceph.conf -k ceph.keyring
	
The script adds an admin key to the ``ceph.keyring``, which is analogous to a 
root password. See `Authentication`_ when running with ``cephx`` enabled.

When you start or stop your cluster, you will not have to use ``sudo`` or
provide passwords. For example:: 

	service ceph -a start

See `Operating a Cluster`_ for details.


.. _Authentication: ../authentication
.. _Operating a Cluster: ../../init/