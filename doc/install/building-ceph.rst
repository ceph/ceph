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

.. topic:: Memory Profiling

	If Google's memory profile tool isn't supported for your distribution, you may
	use ``./configure`` with the ``--without-tcmalloc`` option. See 
	`Memory Profiling`_ for details on using memory profiling.


.. topic:: Hyperthreading

	You can use ``make -j`` to execute multiple jobs depending upon your system. For 
	example, ``make -j4`` for a dual core processor may build faster.


To install Ceph locally, you may also use::

	sudo make install

If you install Ceph locally, ``make`` will place the executables in
``usr/local/bin``. You may add the Ceph configuration file to the
``usr/local/bin`` directory to run an evaluation environment of Ceph from a
single directory.

.. _Memory Profiling: ../../rados/operations/memory-profiling