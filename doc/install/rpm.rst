========================
 Installing RPM Packages
========================

You may install stable release packages (for stable deployments),
development release packages (for the latest features), or development
testing packages (for development and QA only).  Do not add multiple
package sources at the same time.


Install Release Key
===================

Packages are cryptographically signed with the ``release.asc`` key. Add our
release key to your system's list of trusted keys to avoid a security warning::

    sudo rpm --import 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc'


Install Prerequisites
=====================

Ceph may require additional additional third party libraries. 
To add the EPEL repository, execute the following:: 

   su -c 'rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm'

Some releases of Ceph require the following packages:

- snappy
- leveldb
- gdisk
- python-argparse
- gperftools-libs

To install these packages, execute the following::  

	sudo yum install snappy leveldb gdisk python-argparse gperftools-libs


Add Release Packages
====================


Dumpling
--------

Dumpling is the most recent stable release of Ceph.  These packages are
recommended for anyone deploying Ceph in a production environment.
Critical bug fixes are backported and point releases are made as necessary.

Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 18 and 19
(``f18`` and ``f19``), OpenSUSE 12.2 (``opensuse12.2``), and SLES (``sles11``)
platforms. The repository package installs the repository details on your local
system for use with ``yum`` or ``up2date``.

For example, for CentOS 6 or other RHEL6 derivatives (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-dumpling/el6/noarch/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-dumpling


Cuttlefish
----------

Cuttlefish is the previous recent major release of Ceph.  These packages are
recommended for those who have already deployed bobtail in production and are
not yet ready to upgrade.

Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 17
(``f17``), OpenSUSE 12 (``opensuse12``), and SLES (``sles11``)
platforms. The repository package installs the repository details on
your local system for use with ``yum`` or ``up2date``.

For example, for CentOS 6 or other RHEL6 derivatives (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-cuttlefish/el6/noarch/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-cuttlefish


Bobtail
-------

Bobtail is the second major release of Ceph.  These packages are
recommended for those who have already deployed bobtail in production and
are not yet ready to upgrade.

Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 17
(``f17``), OpenSUSE 12 (``opensuse12``), and SLES (``sles11``)
platforms. The repository package installs the repository details on
your local system for use with ``yum`` or ``up2date``.

Replace the``{DISTRO}`` below with the distro codename::

    su -c 'rpm -Uvh http://ceph.com/rpm-bobtail/{DISTRO}/x86_64/ceph-release-1-0.el6.noarch.rpm'

For example, for CentOS 6 or other RHEL6 derivatives (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-bobtail/el6/x86_64/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-bobtail


Development Release Packages
----------------------------

Our development process generates a new release of Ceph every 3-4 weeks. These
packages are faster-moving than the stable releases. Development packages have
new features integrated quickly, while still undergoing several weeks of QA
prior to release.

Packages are cryptographically signed with the ``release.asc`` key. Add our
release key to your system's list of trusted keys to avoid a security warning::

    sudo rpm --import 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc'

Packages are currently built for the CentOS-6 and Fedora 17 platforms. The
repository package installs the repository details on your local system for use
with ``yum`` or ``up2date``.

For CentOS-6::

    su -c 'rpm -Uvh http://ceph.com/rpms/el6/x86_64/ceph-release-1-0.el6.noarch.rpm'

For Fedora 17:: 

    su -c 'rpm -Uvh http://ceph.com/rpms/fc17/x86_64/ceph-release-1-0.fc17.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-testing



Adding Ceph to YUM
==================

You may also add Ceph to the ``/etc/yum.repos.d`` directory. Create a
``ceph.repo`` file. In the example below, replace ``{ceph-stable}`` with 
a stable release of Ceph (e.g., ``cuttlefish``, ``dumpling``, etc.) and
``{distro}`` with your Linux distribution (e.g., ``el6``, ``rhel6``, etc.). ::

	[ceph]
	name=Ceph packages for $basearch
	baseurl=http://ceph.com/rpm-{ceph-stable}/{distro}/$basearch
	enabled=1
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-noarch]
	name=Ceph noarch packages
	baseurl=http://ceph.com/rpm-{ceph-stable}/{distro}/noarch
	enabled=1
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-source]
	name=Ceph source packages
	baseurl=http://ceph.com/rpm-{ceph-stable}/{distro}/SRPMS
	enabled=0
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc


Installing Ceph Deploy
======================

Once you have added either release or development packages, or added a
``ceph.repo`` file to ``/etc/yum.repos.d``, you can install ``ceph-deploy``. ::

	sudo yum install ceph-deploy python-pushy


Installing Ceph Packages
========================

Once you have added either release or development packages, or added a
``ceph.repo`` file to ``/etc/yum.repos.d``, you can install Ceph packages. :: 

	sudo yum install ceph

.. note:: You can also use ``ceph-deploy`` to install Ceph packages.
