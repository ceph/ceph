=================================
 Developer Guide (Quick)
=================================

This guide will describe how to build and test Ceph for development.

Development
-----------

The ``run-make-check.sh`` script will install Ceph dependencies,
compile everything in debug mode and run a number of tests to verify
the result behaves as expected.

.. prompt:: bash $

   ./run-make-check.sh

Optionally if you want to work on a specific component of Ceph,
install the dependencies and build Ceph in debug mode with required cmake flags.

Example:

.. prompt:: bash $

   ./install-deps.sh
   ./do_cmake.sh -DWITH_MANPAGE=OFF -DWITH_BABELTRACE=OFF -DWITH_MGR_DASHBOARD_FRONTEND=OFF

You can also turn off building of some core components that are not relevant to
your development:

.. prompt:: bash $

   ./do_cmake.sh ... -DWITH_RBD=OFF -DWITH_KRBD=OFF -DWITH_RADOSGW=OFF

Finally, build ceph:

.. prompt:: bash $

   cmake --build build [--target <target>...]

Omit ``--target...`` if you want to do a full build.


Running a development deployment
--------------------------------

Ceph contains a script called ``vstart.sh`` (see also
:doc:`/dev/dev_cluster_deployment`) which allows developers to quickly test
their code using a simple deployment on your development system. Once the build
finishes successfully, start the ceph deployment using the following command:

.. prompt:: bash $

   cd build
   ../src/vstart.sh -d -n

You can also configure ``vstart.sh`` to use only one monitor and one metadata server by using the following:

.. prompt:: bash $

   env MON=1 MDS=1 ../src/vstart.sh -d -n -x

Most logs from the cluster can be found in ``build/out``.

The system creates two pools on startup: `cephfs_data_a` and `cephfs_metadata_a`.  Let's get some stats on
the current pools:

.. code-block:: console

  $ bin/ceph osd pool stats
  *** DEVELOPER MODE: setting PATH, PYTHONPATH and LD_LIBRARY_PATH ***
  pool cephfs_data_a id 1
    nothing is going on
	
  pool cephfs_metadata_a id 2
    nothing is going on
	
  $ bin/ceph osd pool stats cephfs_data_a
  *** DEVELOPER MODE: setting PATH, PYTHONPATH and LD_LIBRARY_PATH ***
  pool cephfs_data_a id 1
    nothing is going on

  $ bin/rados df
  POOL_NAME         USED OBJECTS CLONES COPIES MISSING_ON_PRIMARY UNFOUND DEGRADED RD_OPS RD WR_OPS WR
  cephfs_data_a        0       0      0      0                  0       0        0      0  0      0    0
  cephfs_metadata_a 2246      21      0     63                  0       0        0      0  0     42 8192

  total_objects    21
  total_used       244G
  total_space      1180G


Make a pool and run some benchmarks against it:

.. prompt:: bash $

   bin/ceph osd pool create mypool
   bin/rados -p mypool bench 10 write -b 123

Place a file into the new pool:

.. prompt:: bash $

   bin/rados -p mypool put objectone <somefile>
   bin/rados -p mypool put objecttwo <anotherfile>

List the objects in the pool:

.. prompt:: bash $

   bin/rados -p mypool ls

Once you are done, type the following to stop the development ceph deployment:

.. prompt:: bash $

   ../src/stop.sh

Resetting your vstart environment
---------------------------------

The vstart script creates out/ and dev/ directories which contain
the cluster's state.  If you want to quickly reset your environment,
you might do something like this:

.. prompt:: bash [build]$

   ../src/stop.sh
   rm -rf out dev
   env MDS=1 MON=1 OSD=3 ../src/vstart.sh -n -d

Running a RadosGW development environment
-----------------------------------------

Set the ``RGW`` environment variable when running vstart.sh to enable the RadosGW.

.. prompt:: bash $

   cd build
   RGW=1 ../src/vstart.sh -d -n -x

You can now use the swift python client to communicate with the RadosGW.

.. prompt:: bash $

   swift -A http://localhost:8000/auth -U test:tester -K testing list
   swift -A http://localhost:8000/auth -U test:tester -K testing upload mycontainer ceph
   swift -A http://localhost:8000/auth -U test:tester -K testing list


Run unit tests
--------------

The tests are located in `src/tests`.  To run them type:

.. prompt:: bash $

   (cd build && ninja check)
