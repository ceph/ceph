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


Checking out the source
=======================

You can clone from github with

        git clone git@github.com:ceph/ceph

or, if you are not a github user,

        git clone git://github.com/ceph/ceph

Ceph contains many git submodules that need to be checked out with

        git submodule update --init --recursive


Build Prerequisites
===================

The list of Debian or RPM packages dependencies can be installed with:

	./install-deps.sh


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
	cmake [options] ..
	make

This assumes you make your build dir a subdirectory of the ceph.git
checkout.  If you put it elsewhere, just replace .. above with a
correct path to the checkout.


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


Running a test cluster
======================

Autotools
---------

To run a functional test cluster,

	cd src
	./vstart.sh -d -n -x -l
	./ceph -s

Almost all of the usual commands are available in the src/ directory.
For example,

	./rados -p rbd bench 30 write
	./rbd create foo --size 1000

To shut down the test cluster,

	./stop.sh

To start or stop individual daemons, the sysvinit script should work:

	./init-ceph restart osd.0
	./init-ceph stop

CMake
-----

???


Running unit tests
==================

Autotools
---------

To run all tests, a simple

	cd src
	make check

will suffice.  Each test generates a log file that is the name of the
test with .log appended.  For example, unittest_addrs generates a
unittest_addrs.log and test/osd/osd-config.sh puts its output in
test/osd/osd-config.sh.log.

To run an individual test manually, you may want to clean up with

	rm -rf testdir /tmp/*virtualenv
	./stop.sh

and then run a given test like so:

	./unittest_addrs

Many tests are bash scripts that spin up small test clusters, and must be run
like so:

	CEPH_DIR=. test/osd/osd-bench.sh   # or whatever the test is

CMake
-----

???


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

