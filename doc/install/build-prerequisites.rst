=====================
 Build Prerequisites
=====================

.. tip:: Check this section to see if there are specific prerequisites for your 
   Linux/Unix distribution.

Before you can build Ceph source code, you need to install  several libraries
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

	sudo apt-get install autotools-dev autoconf automake cdbs gcc g++ git libboost-dev libedit-dev libssl-dev libtool libfcgi libfcgi-dev libfuse-dev linux-kernel-headers libcrypto++-dev libcrypto++ libexpat1-dev

On Debian/Squeeze, execute ``aptitude install`` for each dependency that isn't 
installed on your host. ::

	aptitude install autotools-dev autoconf automake cdbs gcc g++ git libboost-dev libedit-dev libssl-dev libtool libfcgi libfcgi-dev libfuse-dev linux-kernel-headers libcrypto++-dev libcrypto++ libexpat1-dev pkg-config libcurl4-gnutls-dev
	
On Debian/Wheezy, you may also need:: 

	keyutils-dev libaio and libboost-thread-dev

.. note:: Some distributions that support Google's memory profiler tool may use
   a different package name (e.g., ``libgoogle-perftools4``).

Ubuntu
======

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
======

Alternatively, you may also install::

	aptitude install fakeroot dpkg-dev
	aptitude install debhelper cdbs libexpat1-dev libatomic-ops-dev

openSUSE 11.2 (and later)
=========================

- ``boost-devel``
- ``gcc-c++``
- ``libedit-devel``
- ``libopenssl-devel``
- ``fuse-devel`` (optional)

Execute ``zypper install`` for each dependency that isn't installed on your 
host. ::

	zypper install boost-devel gcc-c++ libedit-devel libopenssl-devel fuse-devel

