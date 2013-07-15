=======================
 Installing on Calxeda
=======================

The Calxeda partnership with Inktank brings the Ceph Distributed Storage System
to Calxeda hardware. This document describes how to install Ceph development
packages on Calxeda hardware.

Ceph on Calxeda uses Debian/Ubuntu Linux. At this time, gitbuilder builds
development packages for Calxeda on the Quantal Quetzal (i.e., 12.10) version of
Ubuntu. The installation process for Ceph on Calxeda is almost identical to the
process for installing Ceph packages on `Debian/Ubuntu`_.


Install Key
===========

Packages are cryptographically signed with the ``autobuild.asc`` key. Add the 
Ceph autobuild key to your system's list of trusted keys to avoid a security 
warning::

	wget -q -O- 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc' | sudo apt-key add -


Add Package
===========

Add the Ceph package repository to your system's list of APT sources, but
replace ``{BRANCH}`` with the branch you'd like to use (e.g., ``master``,
``stable``, ``next``, ``wip-hack``). ::

	echo deb http://gitbuilder.ceph.com/ceph-deb-$(lsb_release -sc)-armv7l-basic/ref/{BRANCH} $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list
	sudo apt-get update


Prerequisites
=============

Ceph on Calxeda requires Google's memory profiling tools (``google-perftools``).
The Ceph repository should have a copy at
http://ceph.com/packages/google-perftools/debian. ::

	echo deb http://ceph.com/packages/google-perftools/debian  $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/google-perftools.list
	sudo apt-get update
	sudo apt-get install google-perftools


Install Ceph
============

Once you have added development packages to APT and installed Google
memory profiling tools, you should update APT's database and install Ceph::

	sudo apt-get update
	sudo apt-get install ceph

You may also use ceph-deploy to install Ceph, but you must add the key and 
packages and install ``google-perftools`` first.	


Ceph Object Storage Packages
============================

If you intend to run Ceph Object Storage on Calxeda hardware, you should add the
``apache2`` and ``fastcgi`` packages **before** installing Ceph Object Storage
components. ::

       echo deb http://gitbuilder.ceph.com/libapache-mod-fastcgi-deb-quantal-arm7l-basic | sudo tee /etc/apt/sources.list.d/fastcgi.list
       echo deb http://gitbuilder.ceph.com/apache2-deb-$(lsb_release -sc)-arm7l-basic/ref/ master $(lsb_release -sc) | sudo tee /etc/apt/sources.list.d/apache2.list

Once you have added these packages, you may install Ceph Object Storage on Calxeda 
hardware.

.. _Debian/Ubuntu: ../debian