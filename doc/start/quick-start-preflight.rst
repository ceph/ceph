=====================
 Preflight Checklist
=====================

.. versionadded:: 0.60

Thank you for trying Ceph! We recommend setting up a ``ceph-deploy`` admin node
and a 3-node :term:`Ceph Storage Cluster` to explore the basics of Ceph. This
**Preflight Checklist** will help you prepare a ``ceph-deploy`` admin node and
three Ceph Nodes (or virtual machines) that will host your Ceph Storage Cluster.


.. ditaa:: 
           /------------------\         /----------------\
           |    Admin Node    |         |   ceph–node1   |
           |                  +-------->+                |
           |    ceph–deploy   |         | cCCC           |
           \---------+--------/         \----------------/
                     |
                     |                  /----------------\
                     |                  |   ceph–node2   |
                     +----------------->+                |
                     |                  | cCCC           |
                     |                  \----------------/
                     |
                     |                  /----------------\
                     |                  |   ceph–node3   |
                     +----------------->|                |
                                        | cCCC           |
                                        \----------------/


Ceph Node Setup
===============

Perform the following steps:

#. Create a user on each Ceph Node. :: 

	ssh user@ceph-server
	sudo useradd -d /home/ceph -m ceph
	sudo passwd ceph

#. Add ``root`` privileges for the user on each Ceph Node. :: 

	echo "ceph ALL = (root) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/ceph
	sudo chmod 0440 /etc/sudoers.d/ceph


#. Install an SSH server (if necessary):: 

	sudo apt-get install openssh-server
	sudo yum install openssh-server
	
	
#. Configure your ``ceph-deploy`` admin node with password-less SSH access to
   each Ceph Node. Leave the passphrase empty::

	ssh-keygen
	Generating public/private key pair.
	Enter file in which to save the key (/ceph-client/.ssh/id_rsa):
	Enter passphrase (empty for no passphrase):
	Enter same passphrase again:
	Your identification has been saved in /ceph-client/.ssh/id_rsa.
	Your public key has been saved in /ceph-client/.ssh/id_rsa.pub.

#. Copy the key to each Ceph Node. ::

	ssh-copy-id ceph@ceph-server


#. Modify the ``~/.ssh/config`` file of your ``ceph-deploy`` admin node so that
   it logs in to Ceph Nodes as the user you created (e.g., ``ceph``). ::

	Host ceph-server
	   Hostname ceph-server.fqdn-or-ip-address.com
	   User ceph


#. Ensure connectivity using ``ping`` with hostnames (i.e., not IP addresses). 
   Address hostname resolution issues and firewall issues as necessary.


Ceph Deploy Setup
=================

Add Ceph repositories to the ``ceph-deploy`` admin node. Then, install
``ceph-deploy``.

.. important:: Do not call ``ceph-deploy`` with ``sudo`` or run it as ``root`` 
   if you are logged in as a different user, because it will not issue ``sudo`` 
   commands needed on the remote host.


Advanced Package Tool (APT)
---------------------------

For Debian and Ubuntu distributions, perform the following steps:

#. Add the release key::

	wget -q -O- 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc' | sudo apt-key add -

#. Add the Ceph packages to your repository. Replace ``{ceph-stable-release}``
   with a stable Ceph release (e.g., ``cuttlefish``, ``dumpling``, etc.). 
   For example::
	
	echo deb http://ceph.com/debian-{ceph-stable-release}/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

#. Update your repository and install ``ceph-deploy``:: 

	sudo apt-get update && sudo apt-get install ceph-deploy


Red Hat Package Manager (RPM)
-----------------------------

For Red Hat(rhel6), CentOS (el6), and Fedora 17-19 (f17-f19) perform the
following steps:

#. Add the package to your repository. Open a text editor and create a 
   Yellowdog Updater, Modified (YUM) entry. Use the file path
   ``/etc/yum.repos.d/ceph.repo``. For example:: 

	sudo vim /etc/yum.repos.d/ceph.repo

   Paste the following example code. Replace ``{ceph-stable-release}`` with 
   the recent stable release of Ceph (e.g., ``dumpling``). Replace ``{distro}``
   with your Linux distribution (e.g., ``el6`` for CentOS 6, ``rhel6`` for 
   Red Hat 6, ``fc18`` or ``fc19`` for Fedora 18 or Fedora 19, and ``sles11`` 
   for SLES 11). Finally, save the contents to the 
   ``/etc/yum.repos.d/ceph.repo`` file. ::

	[ceph-noarch]
	name=Ceph noarch packages
	baseurl=http://ceph.com/rpm-{ceph-stable-release}/{distro}/noarch
	enabled=1
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc 


#. Update your repository and install ``ceph-deploy``:: 

	sudo yum update && sudo yum install ceph-deploy


Summary
=======

This completes the Quick Start Preflight. Proceed to the `Storage Cluster
Quick Start`_.

.. _Storage Cluster Quick Start: ../quick-ceph-deploy
.. _OS Recommendations: ../../install/os-recommendations
