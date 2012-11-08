===============
 Building Ceph
===============

Ceph provides ``automake`` and ``configure`` scripts to streamline the build 
process. To build Ceph, navigate to your cloned Ceph repository and execute the 
following::

	cd ceph
	./autogen.sh
	./configure
	make

You can use ``make -j`` to execute multiple jobs depending upon your system. For 
example::

	make -j4

To install Ceph locally, you may also use::

	sudo make install

If you install Ceph locally, ``make`` will place the executables in 
``usr/local/bin``. You may add the ``ceph.conf`` file to the ``usr/local/bin`` 
directory to run an evaluation environment of Ceph from a single directory.

