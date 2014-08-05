=================================
 Developer Guide (Quick)
=================================

This guide will describe how to build and test Ceph for development.

Development
-----------

After installing the dependencies described in the ``README``,
prepare the git source tree by updating the submodules

.. code::

	git submodule update --init

To build the server daemons, and FUSE client, execute the following:

.. code::

	./do_autogen.sh -d 1
	make -j [number of cpus]

Running a development deployment
--------------------------------
Ceph contains a script called ``vstart.sh`` which allows developers to quickly test their code using
a simple deployment on your development system. Once the build finishes successfully, start the ceph
deployment using the following command:

.. code::

	$ cd src
	$ ./vstart.sh -d -n -x

You can also configure ``vstart.sh`` to use only one monitor and one metadata server by using the following:

.. code::

	$ MON=1 MDS=1 ./vstart.sh -d -n -x

The system creates three pools on startup: `data`, `metadata`, and `rbd`.  Let's get some stats on
the current pools:

.. code::

	$ ./ceph osd pool stats
	*** DEVELOPER MODE: setting PATH, PYTHONPATH and LD_LIBRARY_PATH ***
	pool data id 0
	  nothing is going on
	
	pool metadata id 1
	  nothing is going on
	
	pool rbd id 2
	  nothing is going on
	
	$ ./ceph osd pool stats data
	*** DEVELOPER MODE: setting PATH, PYTHONPATH and LD_LIBRARY_PATH ***
	pool data id 0
	  nothing is going on

	$ ./rados df
	pool name       category                 KB      objects       clones     degraded      unfound     rd        rd KB           wr        wr KB
	data            -                          0            0            0            0     0            0            0            0            0
	metadata        -                          2           20            0           40     0            0            0           21            8
	rbd             -                          0            0            0            0     0            0            0            0            0
	  total used        12771536           20
	  total avail     3697045460
	  total space     3709816996


Make a pool and run some benchmarks against it:

.. code::

	$ ./rados mkpool mypool
	$ ./rados -p mypool bench 10 write -b 123

Place a file into the new pool:

.. code::

	$ ./rados -p mypool put objectone <somefile>
	$ ./rados -p mypool put objecttwo <anotherfile>

List the objects in the pool:

.. code::

	$ ./rados -p mypool ls

Once you are done, type the following to stop the development ceph deployment:

.. code::

	$ ./stop.sh

Run unit tests
--------------

The tests are located in `src/tests`.  To run them type:

	$ make check

