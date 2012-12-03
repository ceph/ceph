=====================
 Build Ceph Packages
=====================

To build packages, you must clone the `Ceph`_ repository. You can create 
installation packages from the latest code using ``dpkg-buildpackage`` for 
Debian/Ubuntu or ``rpmbuild`` for the RPM Package Manager.

.. tip:: When building on a multi-core CPU, use the ``-j`` and the number of 
   cores * 2. For example, use ``-j4`` for a dual-core processor to accelerate 
   the build.

Advanced Package Tool (APT)
===========================

To create ``.deb`` packages for Debian/Ubuntu, ensure that you have cloned the 
`Ceph`_ repository, installed the `build prerequisites`_ and installed 
``debhelper``::

	sudo apt-get install debhelper

Once you have installed debhelper, you can build the packages:

	sudo dpkg-buildpackage

For multi-processor CPUs use the ``-j`` option to accelerate the build.

RPM Package Manager
===================

To create ``.rpm`` packages, ensure that you have cloned the `Ceph`_ repository,
installed the `build prerequisites`_ and installed ``rpm-build`` and 
``rpmdevtools``::

	yum install rpm-build rpmdevtools

Once you have installed the tools, setup an RPM compilation environment::

	rpmdev-setuptree

Fetch the source tarball for the RPM compilation environment::

	wget -P ~/rpmbuild/SOURCES/ http://ceph.com/download/ceph-<version>.tar.gz

Or from the EU mirror::

	wget -P ~/rpmbuild/SOURCES/ http://eu.ceph.com/download/ceph-<version>.tar.gz

Build the RPM packages::

	rpmbuild -tb ~/rpmbuild/SOURCES/ceph-<version>.tar.gz

For multi-processor CPUs use the ``-j`` option to accelerate the build.

.. _build prerequisites: ../build-prerequisites
.. _Ceph: ../clone-source
