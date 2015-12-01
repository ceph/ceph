=====================
 Preflight Checklist
=====================

.. versionadded:: 0.60

This **Preflight Checklist** will help you prepare an admin node for use with
``ceph-deploy``,  and server nodes for use with passwordless ``ssh`` and
``sudo``.

Before you can deploy Ceph using ``ceph-deploy``, you need to ensure that you
have a few things set up first on your admin node and on nodes running Ceph
daemons.
 

Install an Operating System
===========================

Install a recent release of Debian or Ubuntu (e.g., 12.04 LTS, 14.04 LTS) on
your nodes. For additional details on operating systems or to use other
operating systems other than Debian or Ubuntu, see `OS Recommendations`_.


Install an SSH Server
=====================

The ``ceph-deploy`` utility requires ``ssh``, so your server node(s) require an
SSH server. ::

	sudo apt-get install openssh-server


Create a User
=============

Create a user on nodes running Ceph daemons. 

.. tip:: We recommend a username that brute force attackers won't
   guess easily (e.g., something other than ``root``, ``ceph``, etc).

::

	ssh user@ceph-server
	sudo useradd -d /home/ceph -m ceph
	sudo passwd ceph


``ceph-deploy`` installs packages onto your nodes. This means that
the user you create requires passwordless ``sudo`` privileges. 

.. note:: We **DO NOT** recommend enabling the ``root`` password 
   for security reasons. 

To provide full privileges to the user, add the following to 
``/etc/sudoers.d/ceph``. ::

	echo "ceph ALL = (root) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/ceph
	sudo chmod 0440 /etc/sudoers.d/ceph


Configure SSH
=============

Configure your admin machine with password-less SSH access to each node
running Ceph daemons (leave the passphrase empty). ::

	ssh-keygen
	Generating public/private key pair.
	Enter file in which to save the key (/ceph-client/.ssh/id_rsa):
	Enter passphrase (empty for no passphrase):
	Enter same passphrase again:
	Your identification has been saved in /ceph-client/.ssh/id_rsa.
	Your public key has been saved in /ceph-client/.ssh/id_rsa.pub.

Copy the key to each node running Ceph daemons:: 

	ssh-copy-id ceph@ceph-server

Modify your ~/.ssh/config file of your admin node so that it defaults 
to logging in as the user you created when no username is specified. ::

	Host ceph-server
		Hostname ceph-server.fqdn-or-ip-address.com
		User ceph


Install ceph-deploy
===================

To install ``ceph-deploy``, execute the following:: 

	wget -q -O- 'https://download.ceph.com/keys/release.asc' | sudo apt-key add -
	echo deb http://ceph.com/debian-dumpling/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list
	sudo apt-get update	
	sudo apt-get install ceph-deploy


Ensure Connectivity
===================

Ensure that your Admin node has connectivity to the network and to your Server
node (e.g., ensure ``iptables``, ``ufw`` or other tools that may prevent
connections, traffic forwarding, etc. to allow what you need).


Once you have completed this pre-flight checklist, you are ready to begin using
``ceph-deploy``.

.. _OS Recommendations: ../../../start/os-recommendations
