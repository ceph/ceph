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
  the keys, you may encounter security warnings. See `Add Keys`_ for details.

- **Ceph: (Required)** All Ceph deployments require Ceph release packages,
  except for deployments that use development packages (development, QA, and
  bleeding edge deployments only). See `Add Ceph`_ for details.

- **Ceph Development: (Optional)** If you are developing for Ceph, testing Ceph
  development builds, or if you want features from the bleeding edge of Ceph
  development, you may get Ceph development packages. See
  `Add Ceph Development`_ for details.


If you intend to download packages manually, see Section `Download Packages`_.


Add Keys
========

Add a key to your system's list of trusted keys to avoid a security warning. For
major releases (e.g., ``hammer``, ``jewel``, ``luminous``) and development releases
(``release-name-rc1``, ``release-name-rc2``), use the ``release.asc`` key.


APT
---

To install the ``release.asc`` key, execute the following::

	wget -q -O- 'https://download.ceph.com/keys/release.asc' | sudo apt-key add -


RPM
---

To install the ``release.asc`` key, execute the following::

	sudo rpm --import 'https://download.ceph.com/keys/release.asc'

Add Ceph
========

Release repositories use the ``release.asc`` key to verify packages.
To install Ceph packages with the Advanced Package Tool (APT) or
Yellowdog Updater, Modified (YUM), you must add Ceph repositories.

You may find releases for Debian/Ubuntu (installed with APT) at::

	https://download.ceph.com/debian-{release-name}

You may find releases for CentOS/RHEL and others (installed with YUM) at::

	https://download.ceph.com/rpm-{release-name}

The major releases of Ceph are summarized at: :ref:`ceph-releases`

Every second major release is considered Long Term Stable (LTS). Critical
bugfixes are backported to LTS releases until their retirement. Since retired
releases are no longer maintained, we recommend that users upgrade their
clusters regularly - preferably to the latest LTS release.

.. tip:: For non-US users: There might be a mirror close to you where
         to download Ceph from. For more information see: `Ceph Mirrors`_.

Debian Packages
---------------

Add a Ceph package repository to your system's list of APT sources. For newer
versions of Debian/Ubuntu, call ``lsb_release -sc`` on the command line to
get the short codename, and replace ``{codename}`` in the following command. ::

	sudo apt-add-repository 'deb https://download.ceph.com/debian-luminous/ {codename} main'

For early Linux distributions, you may execute the following command::

	echo deb https://download.ceph.com/debian-luminous/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

For earlier Ceph releases, replace ``{release-name}`` with the name  with the
name of the Ceph release. You may call ``lsb_release -sc`` on the command  line
to get the short codename, and replace ``{codename}`` in the following command.

::

	sudo apt-add-repository 'deb https://download.ceph.com/debian-{release-name}/ {codename} main'

For older Linux distributions, replace ``{release-name}`` with the name of the
release::

	echo deb https://download.ceph.com/debian-{release-name}/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

Ceph on ARM processors requires Google's memory profiling tools (``google-perftools``).
The Ceph repository should have a copy at
https://download.ceph.com/packages/google-perftools/debian. ::

	echo deb https://download.ceph.com/packages/google-perftools/debian  $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/google-perftools.list


For development release packages, add our package repository to your system's
list of APT sources.  See `the testing Debian repository`_ for a complete list
of Debian and Ubuntu releases supported. ::

	echo deb https://download.ceph.com/debian-testing/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

.. tip:: For non-US users: There might be a mirror close to you where
         to download Ceph from. For more information see: `Ceph Mirrors`_.


RPM Packages
------------

For major releases, you may add a Ceph entry to the ``/etc/yum.repos.d``
directory. Create a ``ceph.repo`` file. In the example below, replace
``{ceph-release}`` with  a major release of Ceph (e.g., ``hammer``, ``jewel``, ``luminous``,
etc.) and ``{distro}`` with your Linux distribution (e.g., ``el7``, etc.).  You
may view https://download.ceph.com/rpm-{ceph-release}/ directory to see which
distributions Ceph supports. Some Ceph packages (e.g., EPEL) must take priority
over standard packages, so you must ensure that you set
``priority=2``. ::

	[ceph]
	name=Ceph packages for $basearch
	baseurl=https://download.ceph.com/rpm-{ceph-release}/{distro}/$basearch
	enabled=1
	priority=2
	gpgcheck=1
	gpgkey=https://download.ceph.com/keys/release.asc

	[ceph-noarch]
	name=Ceph noarch packages
	baseurl=https://download.ceph.com/rpm-{ceph-release}/{distro}/noarch
	enabled=1
	priority=2
	gpgcheck=1
	gpgkey=https://download.ceph.com/keys/release.asc

	[ceph-source]
	name=Ceph source packages
	baseurl=https://download.ceph.com/rpm-{ceph-release}/{distro}/SRPMS
	enabled=0
	priority=2
	gpgcheck=1
	gpgkey=https://download.ceph.com/keys/release.asc


For specific packages, you may retrieve them by downloading the release package
by name. Our development process generates a new release of Ceph every 3-4
weeks. These packages are faster-moving than the major releases.  Development
packages have new features integrated quickly, while still undergoing several
weeks of QA prior to release.

The repository package installs the repository details on your local system for
use with ``yum``. Replace ``{distro}`` with your Linux distribution, and
``{release}`` with the specific release of Ceph::

    su -c 'rpm -Uvh https://download.ceph.com/rpms/{distro}/x86_64/ceph-{release}.el7.noarch.rpm'

You can download the RPMs directly from::

     https://download.ceph.com/rpm-testing

.. tip:: For non-US users: There might be a mirror close to you where
         to download Ceph from. For more information see: `Ceph Mirrors`_.


Add Ceph Development
====================

If you are developing Ceph and need to deploy and test specific Ceph branches,
ensure that you remove repository entries for major releases first.


DEB Packages
------------

We automatically build Ubuntu packages for current development branches in the
Ceph source code repository.  These packages are intended for developers and QA
only.

Add the package repository to your system's list of APT sources, but
replace ``{BRANCH}`` with the branch you'd like to use (e.g.,
wip-hack, master).  See `the shaman page`_ for a complete
list of distributions we build. ::

    curl -L https://shaman.ceph.com/api/repos/ceph/{BRANCH}/latest/ubuntu/$(lsb_release -sc)/repo/ | sudo tee /etc/apt/sources.list.d/shaman.list

.. note:: If the repository is not ready an HTTP 504 will be returned

The use of ``latest`` in the url, means it will figure out which is the last
commit that has been built. Alternatively, a specific sha1 can be specified.
For Ubuntu Xenial and the master branch of Ceph, it would look like::

    curl -L https://shaman.ceph.com/api/repos/ceph/master/53e772a45fdf2d211c0c383106a66e1feedec8fd/ubuntu/xenial/repo/ | sudo tee /etc/apt/sources.list.d/shaman.list


.. warning:: Development repositories are no longer available after two weeks.

RPM Packages
------------

For current development branches, you may add a Ceph entry to the
``/etc/yum.repos.d`` directory. The `the shaman page`_ can be used to retrieve the full details
of a repo file. It can be retrieved via an HTTP request, for example::

    curl -L https://shaman.ceph.com/api/repos/ceph/{BRANCH}/latest/centos/7/repo/ | sudo tee /etc/yum.repos.d/shaman.repo

The use of ``latest`` in the url, means it will figure out which is the last
commit that has been built. Alternatively, a specific sha1 can be specified.
For CentOS 7 and the master branch of Ceph, it would look like::

    curl -L https://shaman.ceph.com/api/repos/ceph/master/53e772a45fdf2d211c0c383106a66e1feedec8fd/centos/7/repo/ | sudo tee /etc/apt/sources.list.d/shaman.list


.. warning:: Development repositories are no longer available after two weeks.

.. note:: If the repository is not ready an HTTP 504 will be returned

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

	wget -q https://download.ceph.com/debian-{release}/pool/main/c/ceph/ceph_{version}{distro}_{arch}.deb


RPM Packages
------------

Ceph requires additional additional third party libraries.
To add the EPEL repository, execute the following::

   sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

Ceph requires the following packages:

- snappy
- leveldb
- gdisk
- python-argparse
- gperftools-libs


Packages are currently built for the RHEL/CentOS7 (``el7``) platforms.  The
repository package installs the repository details on your local system for use
with ``yum``. Replace ``{distro}`` with your distribution. ::

    su -c 'rpm -Uvh https://download.ceph.com/rpm-luminous/{distro}/noarch/ceph-{version}.{distro}.noarch.rpm'

For example, for CentOS 7  (``el7``)::

    su -c 'rpm -Uvh https://download.ceph.com/rpm-luminous/el7/noarch/ceph-release-1-0.el7.noarch.rpm'

You can download the RPMs directly from::

	https://download.ceph.com/rpm-luminous


For earlier Ceph releases, replace ``{release-name}`` with the name
with the name of the Ceph release. You may call ``lsb_release -sc`` on the command
line to get the short codename. ::

	su -c 'rpm -Uvh https://download.ceph.com/rpm-{release-name}/{distro}/noarch/ceph-{version}.{distro}.noarch.rpm'



.. _Install Ceph Object Storage: ../install-storage-cluster
.. _the testing Debian repository: https://download.ceph.com/debian-testing/dists
.. _the shaman page: https://shaman.ceph.com
.. _Ceph Mirrors: ../mirrors
