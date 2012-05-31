=====================
 Build Prerequisites
=====================
Before you can build Ceph source code or Ceph documentation, you need to install 
several libraries and tools.

.. tip:: Check this section to see if there are specific prerequisites for your 
   Linux/Unix distribution.

Prerequisites for Building Ceph Source Code
===========================================
Ceph provides ``autoconf`` and ``automake`` scripts to get you started quickly. 
Ceph build scripts depend on the following:

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

	aptitude install autotools-dev autoconf automake cdbs gcc g++ git libboost-dev libedit-dev libssl-dev libtool libfcgi libfcgi-dev libfuse-dev linux-kernel-headers libcrypto++-dev libcrypto++ libexpat1-dev


Ubuntu Requirements
-------------------

- ``uuid-dev``
- ``libkeyutils-dev``
- ``libgoogle-perftools-dev``
- ``libatomic-ops-dev``
- ``libaio-dev``
- ``libgdata-common``
- ``libgdata13``

Execute ``sudo apt-get install`` for each dependency that isn't installed on 
your host. ::

	sudo apt-get install uuid-dev libkeyutils-dev libgoogle-perftools-dev libatomic-ops-dev libaio-dev libgdata-common libgdata13

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

Prerequisites for Building Ceph Documentation
=============================================
Ceph utilizes Python's Sphinx documentation tool. For details on
the Sphinx documentation tool, refer to: `Sphinx`_
Follow the directions at `Sphinx 1.1.3`_
to install Sphinx. To run Sphinx, with ``admin/build-doc``, at least the 
following are required:

- ``python-dev``
- ``python-pip``
- ``python-virtualenv``
- ``libxml2-dev``
- ``libxslt-dev``
- ``doxygen``
- ``ditaa``
- ``graphviz``

Execute ``sudo apt-get install`` for each dependency that isn't installed on 
your host. ::

	sudo apt-get install python-dev python-pip python-virtualenv libxml2-dev libxslt-dev doxygen ditaa graphviz

.. _Sphinx: http://sphinx.pocoo.org
.. _Sphinx 1.1.3: http://pypi.python.org/pypi/Sphinx
