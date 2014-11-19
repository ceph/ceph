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

Before you can build Ceph source code, you need to install several libraries
and tools. Ceph provides ``autoconf`` and ``automake`` scripts to get you
started quickly. Ceph build scripts depend on the following:

- ``autotools-dev``
- ``autoconf``
- ``automake``
- ``cdbs``
- ``gcc``
- ``g++``
- ``git``
- ``libboost-dev``
- ``libedit-dev``
- ``libssl-dev``
- ``libtool``
- ``libfcgi``
- ``libfcgi-dev``
- ``libfuse-dev``
- ``linux-kernel-headers``
- ``libcrypto++-dev``
- ``libcrypto++``
- ``libexpat1-dev``
- ``pkg-config``
- ``libcurl4-gnutls-dev``

On Ubuntu, execute ``sudo apt-get install`` for each dependency that isn't 
installed on your host. ::

	sudo apt-get install autoconf automake autotools-dev libbz2-dev debhelper default-jdk git javahelper junit4 libaio-dev libatomic-ops-dev libbabeltrace-ctf-dev libbabeltrace-dev libblkid-dev libboost-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev libcurl4-gnutls-dev libedit-dev libexpat1-dev libfcgi-dev libfuse-dev libgoogle-perftools-dev libkeyutils-dev libleveldb-dev libnss3-dev libsnappy-dev liblttng-ust-dev libtool libudev-dev libxml2-dev pkg-config python python-argparse python-nose uuid-dev uuid-runtime xfslibs-dev yasm

On Debian, execute ``aptitude install`` for each dependency that isn't 
installed on your host. ::

	aptitude install autoconf automake autotools-dev libbz2-dev debhelper default-jdk git javahelper junit4 libaio-dev libatomic-ops-dev libbabeltrace-ctf-dev libbabeltrace-dev libblkid-dev libboost-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev libcurl4-gnutls-dev libedit-dev libexpat1-dev libfcgi-dev libfuse-dev libgoogle-perftools-dev libkeyutils-dev libleveldb-dev libnss3-dev libsnappy-dev liblttng-ust-dev libtool libudev-dev libxml2-dev pkg-config python python-argparse python-nose uuid-dev uuid-runtime xfslibs-dev yasm
	
.. note:: Some distributions that support Google's memory profiler tool may use
   a different package name (e.g., ``libgoogle-perftools4``).

Ubuntu
------

- ``uuid-dev``
- ``libkeyutils-dev``
- ``libgoogle-perftools-dev``
- ``libatomic-ops-dev``
- ``libaio-dev``
- ``libgdata-common``
- ``libgdata13``
- ``libsnappy-dev`` 
- ``libleveldb-dev``

Execute ``sudo apt-get install`` for each dependency that isn't installed on 
your host. ::

	sudo apt-get install uuid-dev libkeyutils-dev libgoogle-perftools-dev libatomic-ops-dev libaio-dev libgdata-common libgdata13 libsnappy-dev libleveldb-dev


Debian
------

Alternatively, you may also install::

	aptitude install fakeroot dpkg-dev
	aptitude install debhelper cdbs libexpat1-dev libatomic-ops-dev

openSUSE 11.2 (and later)
-------------------------

- ``boost-devel``
- ``gcc-c++``
- ``libedit-devel``
- ``libopenssl-devel``
- ``fuse-devel`` (optional)

Execute ``zypper install`` for each dependency that isn't installed on your 
host. ::

	zypper install boost-devel gcc-c++ libedit-devel libopenssl-devel fuse-devel

Fedora 20
---------

As root, run:

::

    yum install make automake autoconf  boost-devel fuse-devel gcc-c++ libtool libuuid-devel libblkid-devel keyutils-libs-devel cryptopp-devel fcgi-devel libcurl-devel expat-devel gperftools-devel libedit-devel libatomic_ops-devel snappy-devel leveldb-devel libaio-devel xfsprogs-devel git libudev-devel


Build Ceph
==========

Ceph provides ``automake`` and ``configure`` scripts to streamline the build 
process. To build Ceph, navigate to your cloned Ceph repository and execute the 
following::

	cd ceph
	./autogen.sh
	./configure
	make

.. topic:: Hyperthreading

	You can use ``make -j`` to execute multiple jobs depending upon your system. For 
	example, ``make -j4`` for a dual core processor may build faster.

See `Installing a Build`_ to install a build in user space.

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

	wget -P ~/rpmbuild/SOURCES/ http://ceph.com/download/ceph-<version>.tar.bz2

Or from the EU mirror::

	wget -P ~/rpmbuild/SOURCES/ http://eu.ceph.com/download/ceph-<version>.tar.bz2

Extract the specfile::

    tar --strip-components=1 -C ~/rpmbuild/SPECS/ --no-anchored -xvjf ~/rpmbuild/SOURCES/ceph-<version>.tar.bz2 "ceph.spec"

Build the RPM packages::

	rpmbuild -ba ~/rpmbuild/SPECS/ceph.spec

For multi-processor CPUs use the ``-j`` option to accelerate the build.

.. _Ceph: ../clone-source
.. _Installing a Build: ../install-storage-cluster#installing-a-build
