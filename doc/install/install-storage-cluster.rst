==============================
 Install Ceph Storage Cluster
==============================

This guide describes installing Ceph packages manually. This procedure
is only for users who are not installing with a deployment tool such as
``ceph-deploy``, ``chef``, ``juju``, etc. 

.. tip:: You can also use ``ceph-deploy`` to install Ceph packages, which may
   be more convenient since you can install ``ceph`` on multiple hosts with
   a single command.


Installing with APT
===================

Once you have added either release or development packages to APT, you should
update APT's database and install Ceph::

	sudo apt-get update && sudo apt-get install ceph ceph-mds


Installing with RPM
===================

To install Ceph with RPMs, execute the following steps:


#. Install ``yum-plugin-priorities``. ::

	sudo yum install yum-plugin-priorities

#. Ensure ``/etc/yum/pluginconf.d/priorities.conf`` exists.

#. Ensure ``priorities.conf`` enables the plugin. :: 

	[main]
	enabled = 1

#. Ensure your YUM ``ceph.repo`` entry includes ``priority=2``. See
   `Get Packages`_ for details::

	[ceph]
	name=Ceph packages for $basearch
	baseurl=http://download.ceph.com/rpm-{ceph-release}/{distro}/$basearch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://download.ceph.com/keys/release.asc

	[ceph-noarch]
	name=Ceph noarch packages
	baseurl=http://download.ceph.com/rpm-{ceph-release}/{distro}/noarch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://download.ceph.com/keys/release.asc

	[ceph-source]
	name=Ceph source packages
	baseurl=http://download.ceph.com/rpm-{ceph-release}/{distro}/SRPMS
	enabled=0
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://download.ceph.com/keys/release.asc


#. Install pre-requisite packages::  

	sudo yum install snappy leveldb gdisk python-argparse gperftools-libs


Once you have added either release or development packages, or added a
``ceph.repo`` file to ``/etc/yum.repos.d``, you can install Ceph packages. :: 

	sudo yum install ceph


Installing a Build
==================

If you build Ceph from source code, you may install Ceph in user space
by executing the following:: 

	sudo make install

If you install Ceph locally, ``make`` will place the executables in
``usr/local/bin``. You may add the Ceph configuration file to the
``usr/local/bin`` directory to run Ceph from a single directory.

.. _Get Packages: ../get-packages
