.. _install-manual:

=======================
 Installation (Manual)
=======================


Get Software
============

There are several methods for getting Ceph software. The easiest and most common
method is to `get packages`_ by adding repositories for use with package
management tools such as the Advanced Package Tool (APT) or Yellowdog Updater,
Modified (YUM). You may also retrieve pre-compiled packages from the Ceph
repository. Finally, you can retrieve tarballs or clone the Ceph source code
repository and build Ceph yourself.


.. toctree::
   :maxdepth: 1

	Get Packages <get-packages>
	Get Tarballs <get-tarballs>
	Clone Source <clone-source>
	Build Ceph <build-ceph>
    	Ceph Mirrors <mirrors>
	Ceph Containers <containers>


Install Software
================

Once you have the Ceph software (or added repositories), installing the software
is easy. To install packages on each :term:`Ceph Node` in your cluster, use package
management tools. You should install Yum Priorities for RHEL/CentOS and other
distributions that use Yum if you intend to install the Ceph Object Gateway or
QEMU.

.. toctree::
   :maxdepth: 1

	Install Ceph Storage Cluster <install-storage-cluster>
	Install Virtualization for Block <install-vm-cloud>


Deploy a Cluster Manually
=========================

Once you have Ceph installed on your nodes, you can deploy a cluster manually.
The manual procedure is primarily for exemplary purposes for those developing
deployment scripts with Chef, Juju, Puppet, etc.

.. toctree::

	Manual Deployment <manual-deployment>
	Manual Deployment on FreeBSD <manual-freebsd-deployment>

Upgrade Software
================

As new versions of Ceph become available, you may upgrade your cluster to take
advantage of new functionality. Read the upgrade documentation before you
upgrade your cluster. Sometimes upgrading Ceph requires you to follow an upgrade
sequence.

.. toctree::
   :maxdepth: 2

.. _get packages: ../get-packages
