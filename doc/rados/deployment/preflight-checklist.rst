=====================
 Preflight Checklist
=====================

.. versionadded:: 0.60

This **Preflight Checklist** will help you prepare an admin host for use with
``ceph-deploy``,  and server hosts for use with passwordless ``ssh`` and
``sudo``.

Before you can deploy Ceph using ``ceph-deploy``, you need to ensure that you
have a few things set up first on your admin host and on hosts running Ceph
daemons.
 

Install an Operating System
===========================

Install a recent release of Debian or Ubuntu (e.g., 12.04, 12.10) on your
hosts. For additional details on operating systems or to use other operating
systems other than Debian or Ubuntu, see `OS Recommendations`_.


Install an SSH Server
=====================

The ``ceph-deploy`` utility requires ``ssh``, so your server host(s) require an
SSH server. ::

	sudo apt-get install openssh-server


Create a User
=============

Create a user on hosts running Ceph daemons. 

.. tip:: We recommend a username that brute force attackers won't
   guess easily (e.g., something other than ``root``, ``ceph``, etc).

::

	ssh user@ceph-server
	sudo useradd -d /home/ceph -m ceph
	sudo passwd ceph


``ceph-deploy`` installs packages onto your hosts. This means that
the user you create requires passwordless ``sudo`` priveleges. 

.. note:: We **DO NOT** recommmend enabling the ``root`` password 
   for security reasons. 

To provide full privileges to the user, add the following to 
``/etc/sudoers.d/chef``. ::

	echo "ceph ALL = (root) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/ceph
	sudo chmod 0440 /etc/sudoers.d/ceph


Configure SSH
=============

Configure your admin machine with password-less SSH access to each host
running Ceph daemons (leave the passphrase empty). ::

	ssh-keygen
	Generating public/private key pair.
	Enter file in which to save the key (/ceph-client/.ssh/id_rsa):
	Enter passphrase (empty for no passphrase):
	Enter same passphrase again:
	Your identification has been saved in /ceph-client/.ssh/id_rsa.
	Your public key has been saved in /ceph-client/.ssh/id_rsa.pub.

Copy the key to each host running Ceph daemons:: 

	ssh-copy-id ceph@ceph-server

Modify your ~/.ssh/config file of your admin host so that it defaults 
to logging in as the user you created when no username is specified. ::

	Host ceph-server
		Hostname ceph-server.fqdn-or-ip-address.com
		User ceph


Install git
===========

To clone the ``ceph-deploy`` repository, you will need install ``git``
on your admin host. ::

	sudo apt-get install git
	

Clone ceph-deploy
=================

To begin working with ``ceph-deploy``, clone its repository. :: 

	git clone https://github.com/ceph/ceph-deploy.git ceph-deploy


Install python-virtualenv
=========================

To bootstrap ``ceph-deploy`` and run it, you must install the
``python-virtualenv`` package. :: 

	sudo apt-get install python-virtualenv


Bootstrap ceph-deploy
=====================

After you clone the repository, bootstrap ``ceph-deploy``. :: 

	cd ceph-deploy
	./bootstrap

Add ``ceph-deploy`` to your path so that so that you can execute it without
remaining in ``ceph-deploy``  directory (e.g., ``/etc/environment``,
``~/.pam_environment``). Once you have completed this pre-flight checklist, you
are ready to begin using ``ceph-deploy``.

.. _OS Recommendations: ../../../install/os-recommendations