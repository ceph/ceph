=============
Building Ceph
=============

Ceph provides build scripts for source code and for documentation.

Building Ceph
=============
Ceph provides ``automake`` and ``configure`` scripts to streamline the build process. To build Ceph, navigate to your cloned Ceph repository and execute the following::

	$ cd ceph
	$ ./autogen.sh
	$ ./configure
	$ make

You can use ``make -j`` to execute multiple jobs depending upon your system. For example:: 

	$ make -j4
	
Building Ceph Documentation
===========================
Ceph utilizes Python’s Sphinx documentation tool. For details on the Sphinx documentation tool, refer to: `Sphinx <http://sphinx.pocoo.org>`_. To build the Ceph documentaiton, navigate to the Ceph repository and execute the build script::

	$ cd ceph
	$ admin/build-doc
	
Once you build the documentation set, you may navigate to the source directory to view it::

	$ cd build-doc/output
	
There should be an ``/html`` directory and a ``/man`` directory containing documentation in HTML and manpage formats respectively.
