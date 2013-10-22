==================
 Add Repositories
==================

Adding repositories is the easiest way to install Ceph, because package
management tools will handle the installation of dependencies for you
in most cases.

Follow this guide to add repositories; then, proceed to the 
`Install Ceph Object Storage`_.


Add Keys
========

Add a key to your system's list of trusted keys to avoid a security warning. For
stable releases (e.g., ``cuttlefish``, ``emperor``) and development releases
(``release-name-rc1``, ``release-name-rc2``), use the ``release.asc`` key. For
development testing packages, use the ``autobuild.asc`` key (developers and QA).


APT
---

To install the ``release.asc`` key, execute the following::

	wget -q -O- 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc' | sudo apt-key add -


To install the ``autobuild.asc`` key, execute the following 
(QA and developers only):: 

	wget -q -O- 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc' | sudo apt-key add -


RPM
---

To install the ``release.asc`` key, execute the following::

	sudo rpm --import 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc'

To install the ``autobuild.asc`` key, execute the following
(QA and developers only):: 

	sudo rpm --import 'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc'


Add Ceph Release Repos
======================

Release repositories use the ``release.asc`` key to verify packages.
To install Ceph packages with the Advanced Package Tool (APT) or
Yellowdog Updater, Modified (YUM), you must add Ceph repositories.

You may find releases for Debian/Ubuntu (installed with APT) at:: 

	http://ceph.com/debian-{release-name}

You may find releases for CentOS/RHEL and others (installed with YUM) at:: 

	http://ceph.com/rpm-{release-name}

The stable releases of Ceph include: 

- **Emperor:** Emperor is the most recent major release of Ceph. These packages
  are recommended for anyone deploying Ceph in a production environment. 
  Critical bug fixes are backported and point releases are made as necessary.
  
- **Dumpling:** Dumpling is the fourth major release of Ceph. These packages
  are recommended for anyone deploying Ceph in a production environment.
  Critical bug fixes are backported as necessary.

- **Cuttlefish:** Cuttlefish is the third major release of Ceph. These packages
  are recommended for those who have already deployed bobtail in production and
  are not yet ready to upgrade.

- **Bobtail:** Bobtail is the second major release of Ceph. These packages are
  recommended for those who have already deployed bobtail in production and
  are not yet ready to upgrade.

- **Argonaut:** Argonaut is the first major release of Ceph.  These packages
  are recommended for those who have already deployed Argonaut in production
  and are not yet ready to upgrade.

.. tip:: For European users, there is also a mirror in the Netherlands at 
   http://eu.ceph.com/ ::


Debian Packages
---------------

Add a Ceph package repository to your system's list of APT sources. For newer
versions of Debian/Ubuntu, call ``lsb_release -sc`` on the command line to 
get the short codename, and replace ``{codename}`` in the following command. :: 

	sudo apt-add-repository 'deb http://ceph.com/debian-emperor/ {codename} main'

For early Linux distributions, you may execute the following command:: 

	echo deb http://ceph.com/debian-emperor/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

For earlier Ceph releases, replace ``{release-name}`` with the name  with the
name of the Ceph release. You may call ``lsb_release -sc`` on the command  line
to get the short codename, and replace ``{codename}`` in the following command.
::

	sudo apt-add-repository 'deb http://ceph.com/debian-{release-name}/ {codename} main'

For older Linux distributions, replace ``{release-name}`` with the name of the
release:: 

	echo deb http://ceph.com/debian-{release-name}/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

Ceph on ARM processors requires Google's memory profiling tools (``google-perftools``).
The Ceph repository should have a copy at
http://ceph.com/packages/google-perftools/debian. ::

	echo deb http://ceph.com/packages/google-perftools/debian  $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/google-perftools.list


For development release packages, add our package repository to your system's
list of APT sources.  See `the testing Debian repository`_ for a complete list
of Debian and Ubuntu releases supported. ::

	echo deb http://ceph.com/debian-testing/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list


RPM Packages
------------

For stable releases, you may add a Ceph entry to the ``/etc/yum.repos.d``
directory. Create a ``ceph.repo`` file. In the example below, replace
``{ceph-release}`` with  a stable release of Ceph (e.g., ``dumpling``,
``emperor``, etc.) and ``{distro}`` with your Linux distribution (e.g., ``el6``,
``rhel6``, etc.).  You may view http://ceph.com/rpm-{ceph-release}/ directory to
see which  distributions Ceph supports. ::

	[ceph]
	name=Ceph packages for $basearch
	baseurl=http://ceph.com/rpm-{ceph-release}/{distro}/$basearch
	enabled=1
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-noarch]
	name=Ceph noarch packages
	baseurl=http://ceph.com/rpm-{ceph-release}/{distro}/noarch
	enabled=1
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-source]
	name=Ceph source packages
	baseurl=http://ceph.com/rpm-{ceph-release}/{distro}/SRPMS
	enabled=0
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc


For development release packages, you may specify the repository
for development releases instead. ::

	[ceph]
	name=Ceph packages for $basearch/$releasever
	baseurl=http://ceph.com/rpm-testing/{distro}/$basearch
	enabled=1
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-noarch]
	name=Ceph noarch packages
	baseurl=http://ceph.com/rpm-testing/{distro}/noarch
	enabled=1
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-source]
	name=Ceph source packages
	baseurl=http://ceph.com/rpm-testing/{distro}/SRPMS
	enabled=0
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc


For specific packages, you may retrieve them by specifically downloading the
release package by name. Our development process generates a new release of Ceph
every 3-4 weeks. These packages are faster-moving than the stable releases.
Development packages have new features integrated quickly, while still
undergoing several weeks of QA prior to release.

The repository package installs the repository details on your local system for
use with ``yum`` or ``up2date``. Replace ``{distro}`` with your Linux distribution, 
and ``{release}`` with the specific release of Ceph::

    su -c 'rpm -Uvh http://ceph.com/rpms/{distro}/x86_64/ceph-{release}.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-testing


Add Ceph Development Repos
==========================

Development repositories use the ``autobuild.asc`` key to verify packages.
If you are developing Ceph and need to deploy and test specific Ceph branches,
ensure that you remove stable repository entries first.

Debian Packages
--------------- 

We automatically build Debian and Ubuntu packages for current
development branches in the Ceph source code repository.  These
packages are intended for developers and QA only.

Add our package repository to your system's list of APT sources, but
replace ``{BRANCH}`` with the branch you'd like to use (e.g., chef-3,
wip-hack, master, stable).  See `the gitbuilder page`_ for a complete
list of distributions we build. ::

	echo deb http://gitbuilder.ceph.com/ceph-deb-$(lsb_release -sc)-x86_64-basic/ref/{BRANCH} $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list


RPM Packages
------------

For current development branches, you may add a Ceph entry to the
``/etc/yum.repos.d`` directory. Create a ``ceph.repo`` file. In the example
below, replace ``{distro}`` with your Linux distribution (e.g., ``centos6``,
``rhel6``, etc.), and ``{branch}`` with the name of the branch you want to
install. ::


	[ceph-source]
	name=Ceph source packages
	baseurl=http://gitbuilder.ceph.com/ceph-rpm-{distro}-x86_64-basic/ref/{branch}/SRPMS
	enabled=0
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc


You may view http://gitbuilder.ceph.com directory to see which distributions 
Ceph supports.


Add Ceph Object Gateway Repos
=============================

Ceph Object Gateway works with ordinary Apache and FastCGI libraries. However,
Ceph builds Apache and FastCGI packages that support 100-continue. To use the
Ceph Apache and FastCGI packages, add them to your repository.


Debian Packages
---------------

Add our Apache and FastCGI packages to your system's list of APT sources if you intend to
use 100-continue. :: 

	echo deb http://gitbuilder.ceph.com/apache2-deb-$(lsb_release -sc)-x86_64-basic/ref/master $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph-apache.list
	echo deb http://gitbuilder.ceph.com/libapache-mod-fastcgi-deb-$(lsb_release -sc)-x86_64-basic/ref/master $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph-fastcgi.list


RPM Packages
------------

You may add a Ceph entry to the ``/etc/yum.repos.d`` directory. Create a
``ceph-apache.repo`` file. In the example below, replace ``{distro}`` with your
Linux distribution (e.g., ``el6``, ``rhel6``, etc.).  You may view
http://gitbuilder.ceph.com directory to see which distributions Ceph supports.
::


	[apache2-ceph-noarch]
	name=Apache noarch packages for Ceph
	baseurl=http://gitbuilder.ceph.com/apache2-rpm-{distro}-x86_64-basic/ref/master
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc

	[apache2-ceph-source]
	name=Apache source packages for Ceph
	baseurl=http://gitbuilder.ceph.com/apache2-rpm-{distro}-x86_64-basic/ref/master
	enabled=0
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc


Repeat the forgoing process by creating a ``ceph-fastcgi.repo`` file. ::

	[fastcgi-ceph-basearch]
	name=FastCGI basearch packages for Ceph
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-{distro}-x86_64-basic/ref/master
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc

	[fastcgi-ceph-noarch]
	name=FastCGI noarch packages for Ceph
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-{distro}-x86_64-basic/ref/master
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc

	[fastcgi-ceph-source]
	name=FastCGI source packages for Ceph
	baseurl=http://gitbuilder.ceph.com/mod_fastcgi-rpm-{distro}-x86_64-basic/ref/master
	enabled=0
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc


Add Ceph Extras
===============

Some Ceph deployments require additional Ceph packages. Ceph Extras contains
packages for ``curl``, ``leveldb``, ``ceph-deploy``, the SCSI target framework
and even some QEMU packages for RPMs. Add the Ceph Extras repository to ensure
you obtain these additional packages from the Ceph repository.


Debian Packages
---------------

Add our Ceph Extras package repository to your system's list of APT sources. ::

	echo deb http://ceph.com/packages/ceph-extras/debian $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph-extras.list


RPM Packages
------------

For RPM packages, add our package repository to your ``/etc/yum.repos.d`` repos (e.g.,
``ceph-extras.repo``). Some Ceph packages (e.g., QEMU) must take priority over standard 
packages, so you must ensure that you set ``priority=2``. ::

	[ceph-extras]
	name=Ceph Extras Packages
	baseurl=http://ceph.com/packages/ceph-extras/rpm/{distro}/$basearch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-extras-noarch]
	name=Ceph Extras noarch
	baseurl=http://ceph.com/packages/ceph-extras/rpm/{distro}/noarch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-extras-source]
	name=Ceph Extras Sources
	baseurl=http://ceph.com/packages/ceph-extras/rpm/c{distro}/SRPMS
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc



.. _Install Ceph Object Storage: ../install-storage-cluster
.. _the testing Debian repository: http://ceph.com/debian-testing/dists
.. _the gitbuilder page: http://gitbuilder.ceph.com