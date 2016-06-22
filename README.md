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


Prerequisite:
        CMake 2.8.11

Build instructions:

        ./do_cmake.sh
	cd build
	make

This assumes you make your build dir a subdirectory of the ceph.git
checkout.  If you put it elsewhere, just replace .. above with a
correct path to the checkout.


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

To run a functional test cluster,

	cd build
	../src/vstart.sh -d -n -x -l
	./bin/ceph -s

Almost all of the usual commands are available in the bin/ directory.
For example,

	./bin/rados -p rbd bench 30 write
	./bin/rbd create foo --size 1000

To shut down the test cluster,

	../src/stop.sh

To start or stop individual daemons, the sysvinit script can be used:

	./bin/init-ceph restart osd.0
	./bin/init-ceph stop


Running unit tests
==================

To run build and run all tests, use ctest:

	cd build
	ctest -j$(nproc)

To run an individual test manually, run the ctest command with -R
(regex matching):

        ctest -R [test name]

To run an individual test manually and see all the tests output, run
the ctest command with the -V (verbose) flag:

        ctest -V -R [test name]

To run an tests manually and run the jobs in parallel, run the ctest
command with the -j flag:

        ctest -j [number of jobs]

There are many other flags you can give the ctest command for better control
over manual test execution. To view these options run:

        man ctest


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

