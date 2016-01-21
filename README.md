============================================
Ceph - a scalable distributed storage system
============================================

Please see http://ceph.com/ for current info.

Contributing Code
=================

Most of Ceph is licensed under the LGPL version 2.1.  Some
miscellaneous code is under BSD-style license or is public domain.
The documentation is licensed under Creative Commons
Attribution-ShareAlike (CC BY-SA).  There are a handful of headers
included here that are licensed under the GPL.  Please see the file
COPYING for a full inventory of licenses by file.

Code contributions must include a valid "Signed-off-by" acknowledging
the license for the modified or contributed file.  Please see the file
SubmittingPatches.rst for details on what that means and on how to
generate and submit patches.

We do not require assignment of copyright to contribute code; code is
contributed under the terms of the applicable license.


Build Prerequisites
===================

The list of Debian or RPM packages dependencies can be installed with:

	./install-deps.sh

Note: libsnappy-dev and libleveldb-dev are not available upstream for
Debian Squeeze.  Backports for Ceph can be found at ceph.com/debian-leveldb.

Building Ceph
=============

Autotools
---------

Developers, please refer to the [Developer
Guide](doc/dev/quick_guide.rst) for more information, otherwise, you
can build the server daemons, and FUSE client, by executing the
following:

	./autogen.sh
	./configure
	make

(Note that the FUSE client will only be built if libfuse is present.)

CMake
-----

Prerequisite:
        CMake 2.8.11

Build instructions:

	mkdir build
	cd build
	cmake [options] /path/to/ceph/src/dir
	make

(Note that /path/to/ceph/src/dir can be in the tree and out of the tree)

Dependencies
------------

The configure script will complain about any missing dependencies as
it goes.  You can also refer to debian/control or ceph.spec.in for the
package build dependencies on those platforms.  In many cases,
dependencies can be avoided with --with-foo or --without-bar switches.
For example,

	./configure --with-nss         # use libnss instead of libcrypto++
	./configure --without-radosgw  # do not build radosgw
	./configure --without-tcmalloc # avoid google-perftools dependency


Building packages
-----------------

You can build packages for Debian or Debian-derived (e.g., Ubuntu)
systems with

	sudo apt-get install dpkg-dev
	dpkg-checkbuilddeps        # make sure we have all dependencies
	dpkg-buildpackage

For RPM-based systems (Red Hat, SUSE, etc.),

	rpmbuild

Building the Documentation
==========================

Prerequisites
-------------

The list of package dependencies for building the documentation can be
found in doc_deps.deb.txt:

	sudo apt-get install `cat doc_deps.deb.txt`

Building the Documentation
--------------------------

To build the documentation, ensure that you are in the top-level
`/ceph directory, and execute the build script. For example:

	admin/build-doc

