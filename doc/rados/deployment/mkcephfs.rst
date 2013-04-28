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

	sudo -i 
	ssh-keygen
	Generating public/private key pair.
	Enter file in which to save the key (/root/.ssh/id_rsa): 
	Enter passphrase (empty for no passphrase): 
	Enter same passphrase again: 
	Your identification has been saved in /root/.ssh/id_rsa.
	Your public key has been saved in /root/.ssh/id_rsa.pub.
	
Modify your ``/root/.ssh/config`` file to login as ``root``, as follows:: 

	Host myserver01
		Hostname myserver01.fully-qualified-domain.com
		User root
	Host myserver02
		Hostname myserver02.fully-qualified-domain.com
		User root

You may use RSA or DSA keys. Once you generate your keys, copy them to each 
OSD host. For example:: 

	ssh-copy-id root@myserver01
	ssh-copy-id root@myserver02

Copy Configuration File to All Hosts
====================================

Ceph's ``mkcephfs`` deployment script does not copy the configuration file you
created from the Administration host to the OSD Cluster hosts. Copy the
configuration file you created (*i.e.,* ``mycluster.conf`` in the example below)
from the Administration host to ``etc/ceph/ceph.conf`` on each OSD Cluster host
if you are using ``mkcephfs`` to deploy Ceph.

::

	sudo ssh myserver01 tee /etc/ceph/ceph.conf < /etc/ceph/ceph.conf
	sudo ssh myserver02 tee /etc/ceph/ceph.conf < /etc/ceph/ceph.conf
	sudo ssh myserver03 tee /etc/ceph/ceph.conf < /etc/ceph/ceph.conf


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
When mounting disks in this manner, there is no need for an entry in
``/etc/fstab``.

.. versionadded:: 0.56

For Bobtail (v 0.56) and beyond, you may specify the file system type, filesystem
options, and mount options. Add the following to the ``[global]`` section of your
Ceph configuration file, and replace the values in braces with appropriate values:: 

	osd mkfs type = {fs-type}
	osd mkfs options {fs-type} = {mkfs options}   # default for xfs is "-f"
	osd mount options {fs-type} = {mount options} # default mount option is "rw, noatime"

For example:: 

	osd mkfs type = btrfs
	osd mkfs options btrfs = -m raid0
	osd mount options btrfs = rw, noatime
	
For each ``[osd.n]`` section of your configuration file, specify the storage device. 
For example:: 

	[osd.1]
		devs = /dev/sda
	[osd.2]
		devs = /dev/sdb


Run ``mkcephfs``
================

Once you have copied your Ceph Configuration to the OSD Cluster hosts
and created the default directories, you may deploy Ceph with the 
``mkcephfs`` script.

.. note::  ``mkcephfs`` is a quick bootstrapping tool. It does not handle more 
   complex operations, such as upgrades.

To run ``mkcephfs``, execute the following:: 

   cd /etc/ceph
   sudo mkcephfs -a -c /etc/ceph/ceph.conf -k ceph.keyring
	
.. note:: For ``mkcephfs`` to use the ``mkfs`` configuration options, you MUST
   specify a ``devs`` entry for each OSD.

The script adds an admin key to the ``ceph.keyring``, which is analogous to a 
root password. See `Authentication`_ when running with ``cephx`` enabled. To
start the cluster, execute the following::  

	sudo service ceph -a start

See `Operating a Cluster`_ for details. Also see `man mkcephfs`_.

.. _Authentication: ../authentication
.. _Operating a Cluster: ../../operations/
.. _man mkcephfs: ../../../man/8/mkcephfs

.. toctree:: 
   :hidden: 
   
   ../../../man/8/mkcephfs