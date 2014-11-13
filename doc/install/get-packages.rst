==============
 Get Packages
==============

To install Ceph and other enabling software, you need to retrieve packages from
the Ceph repository. Follow this guide to get packages; then, proceed to the 
`Install Ceph Object Storage`_.


Getting Packages
================

There are two ways to get packages: 

- **Add Repositories:** Adding repositories is the easiest way to get packages, 
  because package management tools will retrieve the packages and all enabling
  software for you in most cases. However, to use this approach, each 
  :term:`Ceph Node` in your cluster must have internet access.
  
- **Download Packages Manually:** Downloading packages manually is a convenient
  way to install Ceph if your environment does not allow a :term:`Ceph Node` to
  access the internet.


Requirements
============

All Ceph deployments require Ceph packages (except for development). You should 
also add keys and recommended packages. 

- **Keys: (Recommended)** Whether you add repositories or download packages 
  manually, you should download keys to verify the packages. If you do not get
  the keys, you may encounter security warnings. There are two keys: one for 
  releases (common) and one for development (programmers and QA only). Choose 
  the key that suits your needs. See `Add Keys`_ for details.

- **Ceph Extras: (Required)** The Ceph Extras repository provides newer 
  Ceph-enabled versions of packages which are already provided in your Linux 
  distribution, but where newer versions are required to support Ceph. Examples 
  of newer versions of available packages include QEMU for CentOS/RHEL 
  distribution and iSCSI among others. If you intend to use any of the 
  foregoing packages, you must add the Ceph Extras repository or download the 
  packages manually. This repository also contains Ceph dependencies for those
  who intend to install Ceph manually.  See `Add Ceph Extras`_ for details.

- **Ceph: (Required)** All Ceph deployments require Ceph release packages, 
  except for deployments that use development packages (development, QA, and 
  bleeding edge deployments only). See `Add Ceph`_ for details.

- **Ceph Development: (Optional)** If you are developing for Ceph, testing Ceph 
  development builds, or if you want features from the bleeding edge of Ceph 
  development, you may get Ceph development packages. See 
  `Add Ceph Development`_ for details.

- **Apache/FastCGI: (Optional)** If you are deploying a 
  :term:`Ceph Object Storage` service, you must install Apache and FastCGI. 
  Ceph provides Apache and FastCGI builds that are identical to those available 
  from Apache, but with 100-continue support. If you want to enable 
  :term:`Ceph Object Gateway` daemons with 100-continue support, you must 
  retrieve Apache/FastCGI packages from the Ceph repository. 
  See `Add Apache/FastCGI`_ for details.


If you intend to download packages manually, see Section `Download Packages`_.


Add Keys
========

Add a key to your system's list of trusted keys to avoid a security warning. For
major releases (e.g., ``dumpling``, ``emperor``, ``firefly``) and development
releases (``release-name-rc1``, ``release-name-rc2``), use the ``release.asc``
key. For development testing packages, use the ``autobuild.asc`` key (developers
and QA).


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



Add Ceph Extras
===============

Some Ceph deployments require newer Ceph-enabled versions of packages that are
already available in your Linux distribution. For example, Ceph Extras contains
newer Ceph-enabled packages for the SCSI target framework and QEMU packages for
RPMs. The repository also contains ``curl``, ``leveldb`` and other Ceph
dependencies. Add the Ceph Extras repository to ensure you obtain these
additional packages from the Ceph repository.


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
	baseurl=http://ceph.com/packages/ceph-extras/rpm/{distro}/SRPMS
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc


Add Ceph
========

Release repositories use the ``release.asc`` key to verify packages.
To install Ceph packages with the Advanced Package Tool (APT) or
Yellowdog Updater, Modified (YUM), you must add Ceph repositories.

You may find releases for Debian/Ubuntu (installed with APT) at:: 

	http://ceph.com/debian-{release-name}

You may find releases for CentOS/RHEL and others (installed with YUM) at:: 

	http://ceph.com/rpm-{release-name}

The major releases of Ceph include:

- **Giant:** Giant is the most recent major release of Ceph. These packages
  are recommended for anyone deploying Ceph in a production environment. 
  Critical bug fixes are backported and point releases are made as necessary.

- **Firefly:** Firefly is the sixth major release of Ceph. These packages
  are recommended for anyone deploying Ceph in a production environment. 
  Firefly is a long-term stable release, so critical bug fixes are backported
  and point releases are made as necessary.

- **Emperor:** Emperor is the fifth major release of Ceph. These packages
  are are old and no longer supported, so we recommend that users upgrade to
  Firefly immediately.
  
- **Dumpling:** Dumpling is the fourth major release of Ceph. These packages
  are older and not recommended for new users, but critical bug fixes are
  still backported as necessary. We encourage all Dumpling users to update to
  Firefly as soon as they are able to do so.

- **Argonaut, Bobtail, Cuttlefish:** These are the first three releases of
  Ceph. These packages are old and no longer supported, so we recommend that
  users upgrade to a supported version.

.. tip:: For European users, there is also a mirror in the Netherlands at:
   http://eu.ceph.com/ 


Debian Packages
---------------

Add a Ceph package repository to your system's list of APT sources. For newer
versions of Debian/Ubuntu, call ``lsb_release -sc`` on the command line to 
get the short codename, and replace ``{codename}`` in the following command. :: 

	sudo apt-add-repository 'deb http://ceph.com/debian-firefly/ {codename} main'

For early Linux distributions, you may execute the following command:: 

	echo deb http://ceph.com/debian-firefly/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

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

For major releases, you may add a Ceph entry to the ``/etc/yum.repos.d``
directory. Create a ``ceph.repo`` file. In the example below, replace
``{ceph-release}`` with  a major release of Ceph (e.g., ``dumpling``,
``emperor``, etc.) and ``{distro}`` with your Linux distribution (e.g., ``el6``,
``rhel6``, etc.).  You may view http://ceph.com/rpm-{ceph-release}/ directory to
see which  distributions Ceph supports. Some Ceph packages (e.g., EPEL) must
take priority over standard packages, so you must ensure that you set
``priority=2``. ::

	[ceph]
	name=Ceph packages for $basearch
	baseurl=http://ceph.com/rpm-{ceph-release}/{distro}/$basearch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-noarch]
	name=Ceph noarch packages
	baseurl=http://ceph.com/rpm-{ceph-release}/{distro}/noarch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-source]
	name=Ceph source packages
	baseurl=http://ceph.com/rpm-{ceph-release}/{distro}/SRPMS
	enabled=0
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc


For development release packages, you may specify the repository
for development releases instead. ::

	[ceph]
	name=Ceph packages for $basearch/$releasever
	baseurl=http://ceph.com/rpm-testing/{distro}/$basearch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-noarch]
	name=Ceph noarch packages
	baseurl=http://ceph.com/rpm-testing/{distro}/noarch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

	[ceph-source]
	name=Ceph source packages
	baseurl=http://ceph.com/rpm-testing/{distro}/SRPMS
	enabled=0
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc


For specific packages, you may retrieve them by specifically downloading the
release package by name. Our development process generates a new release of Ceph
every 3-4 weeks. These packages are faster-moving than the major releases.
Development packages have new features integrated quickly, while still
undergoing several weeks of QA prior to release.

The repository package installs the repository details on your local system for
use with ``yum`` or ``up2date``. Replace ``{distro}`` with your Linux distribution, 
and ``{release}`` with the specific release of Ceph::

    su -c 'rpm -Uvh http://ceph.com/rpms/{distro}/x86_64/ceph-{release}.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-testing


Add Ceph Development
====================

Development repositories use the ``autobuild.asc`` key to verify packages.
If you are developing Ceph and need to deploy and test specific Ceph branches,
ensure that you remove repository entries for major releases first.


Debian Packages
--------------- 

We automatically build Debian and Ubuntu packages for current
development branches in the Ceph source code repository.  These
packages are intended for developers and QA only.

Add our package repository to your system's list of APT sources, but
replace ``{BRANCH}`` with the branch you'd like to use (e.g., chef-3,
wip-hack, master).  See `the gitbuilder page`_ for a complete
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


Add Apache/FastCGI
==================

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


Download Packages
=================

If you are attempting to install behind a firewall in an environment without internet 
access, you must retrieve the packages (mirrored with all the necessary dependencies) 
before attempting an install.

Debian Packages
---------------

Ceph requires additional additional third party libraries.  

- libaio1
- libsnappy1
- libcurl3
- curl
- libgoogle-perftools4
- google-perftools
- libleveldb1


The repository package installs the repository details on your local system for
use with ``apt``. Replace ``{release}`` with the latest Ceph release. Replace
``{version}`` with the latest Ceph version number. Replace ``{distro}`` with
your Linux distribution codename. Replace ``{arch}`` with the CPU architecture.

::

	wget -q http://ceph.com/debian-{release}/pool/main/c/ceph/ceph_{version}{distro}_{arch}.deb


RPM Packages
------------

Ceph requires additional additional third party libraries.  
To add the EPEL repository, execute the following:: 

   su -c 'rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm'

Ceph requires the following packages:

- snappy
- leveldb
- gdisk
- python-argparse
- gperftools-libs


Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 18 and 19
(``f18`` and ``f19``), OpenSUSE 12.2 (``opensuse12.2``), and SLES (``sles11``)
platforms. The repository package installs the repository details on your local
system for use with ``yum`` or ``up2date``. Replace ``{distro}`` with your
distribution. ::

    su -c 'rpm -Uvh http://ceph.com/rpm-firefly/{distro}/noarch/ceph-{version}.{distro}.noarch.rpm'

For example, for CentOS 6  (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-firefly/el6/noarch/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

	http://ceph.com/rpm-firefly


For earlier Ceph releases, replace ``{release-name}`` with the name 
with the name of the Ceph release. You may call ``lsb_release -sc`` on the command 
line to get the short codename. ::

	su -c 'rpm -Uvh http://ceph.com/rpm-{release-name}/{distro}/noarch/ceph-{version}.{distro}.noarch.rpm'




.. _Install Ceph Object Storage: ../install-storage-cluster
.. _the testing Debian repository: http://ceph.com/debian-testing/dists
.. _the gitbuilder page: http://gitbuilder.ceph.com