=====================
 Preflight Checklist
=====================

.. versionadded:: 0.60

Thank you for trying Ceph! We recommend setting up a ``ceph-deploy`` node and a
3-node :term:`Ceph Storage Cluster` to explore the basics of Ceph. This
**Preflight Checklist** will help you prepare a ``ceph-deploy`` node and three
Ceph Nodes (or virtual machines) that will host your Ceph Storage Cluster.


.. ditaa:: 
           /------------------\         /----------------\
           | ceph deploy Node |         |    Ceph Node   |
           |                  +-------->+                |
           |                  |         | cCCC           |
           \---------+--------/         \----------------/
                     |
                     |                  /----------------\
                     |                  |    Ceph Node   |
                     +----------------->+                |
                     |                  | cCCC           |
                     |                  \----------------/
                     |
                     |                  /----------------\
                     |                  |    Ceph Node   |
                     +----------------->|                |
                                        | cCCC           |
                                        \----------------/

Ceph runs on recent distributions of Linux (e.g., Ubuntu, Red Hat, CentOS,
etc.).


Ceph Node Setup
===============

A :term:`Ceph Node` requires the following: 

- A recent Linux distribution.
- A user with ``root`` privileges.
- An SSH server.
- Network connectivity and access to/for other Ceph Nodes.


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
	
	
#. Configure your ``ceph-deploy`` node with password-less SSH access to each 
   Ceph Node. Leave the passphrase empty::

	ssh-keygen
	Generating public/private key pair.
	Enter file in which to save the key (/ceph-client/.ssh/id_rsa):
	Enter passphrase (empty for no passphrase):
	Enter same passphrase again:
	Your identification has been saved in /ceph-client/.ssh/id_rsa.
	Your public key has been saved in /ceph-client/.ssh/id_rsa.pub.

#. Copy the key to each Ceph Node. ::

	ssh-copy-id ceph@ceph-server


#. Modify the ``~/.ssh/config`` file of your ``ceph-deploy`` node so that it
   logs in to Ceph Nodes as the user you created (e.g., ``ceph``). ::

	Host ceph-server
	   Hostname ceph-server.fqdn-or-ip-address.com
	   User ceph


#. Ensure connectivity using ``ping`` with hostnames (i.e., not IP addresses). 
   Address hostname resolution issues and firewall issues as necessary.


Ceph Deploy Setup
=================

Add Ceph repositories to the ``ceph-deploy`` node. Then, install
``ceph-deploy``.


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

For Red Hat(rhel6), CentOS (el6), Fedora 17-19 (f17-f19), OpenSUSE 12
(opensuse12), and SLES (sles11) perform the following steps:


#. Add the package to your repository. Open a text editor and create a 
   Yellowdog Updater, Modified (YUM) entry under ``/etc/yum.repos.d``:: 

	[ceph]
	   name=Ceph Packages $basearch
	   baseurl=http://ceph.com/rpm-{ceph-stable-release}/{distro}/$basearch
	   enabled=1
	   gpgcheck=1
	   type=rpm-md
	   gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc 

   Ensure that you use the correct path for your Linux distribution. Replace 
   ``{ceph-stable-release}`` with the recent stable release of Ceph 
   (e.g., ``dumpling``). Replace ``{distro}`` with your Linux distribution
   (e.g., CentOS (``el6``), Red Hat (``rhel6``), Fedora (``fc18`` or ``fc19``),
   SLES (``sles11``)).

#. Update your repository and install ``ceph-deploy``:: 

	sudo yum update && sudo yum install ceph-deploy python-pushy



.. _Storage Cluster Quick Start: ../quick-ceph-deploy
.. _OS Recommendations: ../../install/os-recommendations
