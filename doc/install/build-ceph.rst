============
 Build Ceph
============

You can get Ceph software by retrieving Ceph source code and building it yourself.
To build Ceph, you need to set up a development environment, compile Ceph,
and then either install in user space or build packages and install the packages.

Build Prerequisites
===================


.. tip:: Check this section to see if there are specific prerequisites for your
   Linux/Unix distribution.

A debug build of Ceph may take around 40 gigabytes. If you want to build Ceph in
a virtual machine (VM) please make sure total disk space on the VM is at least
60 gigabytes.

Please also be aware that some distributions of Linux, like CentOS, use Linux
Volume Manager (LVM) for the default installation. LVM may reserve a large
portion of disk space of a typical sized virtual disk for the operating system.

Before you can build Ceph source code, you need to install several libraries
and tools::

	./install-deps.sh

.. note:: Some distributions that support Google's memory profiler tool may use
   a different package name (e.g., ``libgoogle-perftools4``).

.. _build-ceph:

Build Ceph
==========

Ceph is built using cmake. To build Ceph, navigate to your cloned Ceph
repository and execute the following::

    cd ceph
    ./do_cmake.sh
    cd build
    ninja

See `Installing a Build`_ to install a build in user space and `Ceph README.md`_
doc for more details on build.

Build Ceph Packages
===================

To build packages, you must clone the `Ceph`_ repository. You can create 
installation packages from the latest code using ``dpkg-buildpackage`` for 
Debian/Ubuntu or ``rpmbuild`` for the RPM Package Manager.

.. tip:: When building on a multi-core CPU, use the ``-j`` and the number of 
   cores * 2. For example, use ``-j4`` for a dual-core processor to accelerate 
   the build.


Advanced Package Tool (APT)
---------------------------

To create ``.deb`` packages for Debian/Ubuntu, ensure that you have cloned the 
`Ceph`_ repository, installed the `Build Prerequisites`_ and installed 
``debhelper``::

	sudo apt-get install debhelper

Once you have installed debhelper, you can build the packages::

	sudo dpkg-buildpackage

For multi-processor CPUs use the ``-j`` option to accelerate the build.


RPM Package Manager
-------------------

To create ``.rpm`` packages, ensure that you have cloned the `Ceph`_ repository,
installed the `Build Prerequisites`_ and installed ``rpm-build`` and 
``rpmdevtools``::

	yum install rpm-build rpmdevtools

Once you have installed the tools, setup an RPM compilation environment::

	rpmdev-setuptree

Fetch the source tarball for the RPM compilation environment::

	wget -P ~/rpmbuild/SOURCES/ https://download.ceph.com/tarballs/ceph-<version>.tar.bz2

Or from the EU mirror::

	wget -P ~/rpmbuild/SOURCES/ http://eu.ceph.com/tarballs/ceph-<version>.tar.bz2

Extract the specfile::

    tar --strip-components=1 -C ~/rpmbuild/SPECS/ --no-anchored -xvjf ~/rpmbuild/SOURCES/ceph-<version>.tar.bz2 "ceph.spec"

Build the RPM packages::

	rpmbuild -ba ~/rpmbuild/SPECS/ceph.spec

For multi-processor CPUs use the ``-j`` option to accelerate the build.

.. _Ceph: ../clone-source
.. _Installing a Build: ../install-storage-cluster#installing-a-build
.. _Ceph README.md: https://github.com/ceph/ceph#building-ceph
