Running Unit Tests
==================

How to run s3-tests locally
---------------------------

RGW code can be tested by building Ceph locally from source, starting a vstart
cluster, and running the "s3-tests" suite against it.

The following instructions should work on jewel and above.

Step 1 - build Ceph
^^^^^^^^^^^^^^^^^^^

Refer to :doc:`/install/build-ceph`.

You can do step 2 separately while it is building.

Step 2 - vstart
^^^^^^^^^^^^^^^

When the build completes, and still in the top-level directory of the git
clone where you built Ceph, do the following, for cmake builds::

    cd build/
    RGW=1 ../src/vstart.sh -n

This will produce a lot of output as the vstart cluster is started up. At the
end you should see a message like::

    started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output.

This means the cluster is running.


Step 3 - run s3-tests
^^^^^^^^^^^^^^^^^^^^^

.. highlight:: console

To run the s3tests suite do the following::

   $ ../qa/workunits/rgw/run-s3tests.sh


Running test using vstart_runner.py
-----------------------------------
CephFS and Ceph Manager code is be tested using `vstart_runner.py`_.

Running your first test
^^^^^^^^^^^^^^^^^^^^^^^^^^
The Python tests in Ceph repository can be executed on your local machine
using `vstart_runner.py`_. To do that, you'd need `teuthology`_ installed::

    $ virtualenv --python=python3 venv
    $ source venv/bin/activate
    $ pip install 'setuptools >= 12'
    $ pip install teuthology[test]@git+https://github.com/ceph/teuthology
    $ deactivate

The above steps installs teuthology in a virtual environment. Before running
a test locally, build Ceph successfully from the source (refer
:doc:`/install/build-ceph`) and do::

    $ cd build
    $ ../src/vstart.sh -n -d -l
    $ source ~/path/to/teuthology/venv/bin/activate

To run a specific test, say `test_reconnect_timeout`_ from
`TestClientRecovery`_ in ``qa/tasks/cephfs/test_client_recovery``, you can
do::

    $ python ../qa/tasks/vstart_runner.py tasks.cephfs.test_client_recovery.TestClientRecovery.test_reconnect_timeout

The above command runs vstart_runner.py and passes the test to be executed as
an argument to vstart_runner.py. In a similar way, you can also run the group
of tests in the following manner::

    $ # run all tests in class TestClientRecovery
    $ python ../qa/tasks/vstart_runner.py tasks.cephfs.test_client_recovery.TestClientRecovery
    $ # run  all tests in test_client_recovery.py
    $ python ../qa/tasks/vstart_runner.py tasks.cephfs.test_client_recovery

Based on the argument passed, vstart_runner.py collects tests and executes as
it would execute a single test.

vstart_runner.py can take the following options -

--clear-old-log             deletes old log file before running the test
--create                    create Ceph cluster before running a test
--create-cluster-only       creates the cluster and quits; tests can be issued
                            later
--interactive               drops a Python shell when a test fails
--log-ps-output             logs ps output; might be useful while debugging
--teardown                  tears Ceph cluster down after test(s) has finished
                            running
--kclient                   use the kernel cephfs client instead of FUSE
--brxnet=<net/mask>         specify a new net/mask for the mount clients' network
                            namespace container (Default: 192.168.0.0/16)

.. note:: If using the FUSE client, ensure that the fuse package is installed
          and enabled on the system and that ``user_allow_other`` is added
          to ``/etc/fuse.conf``.

.. note:: If using the kernel client, the user must have the ability to run
          commands with passwordless sudo access.

.. note:: A failure on the kernel client may crash the host, so it's
          recommended to use this functionality within a virtual machine.

Internal working of vstart_runner.py -
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
vstart_runner.py primarily does three things -

* collects and runs the tests
    vstart_runner.py setups/teardowns the cluster and collects and runs the
    test. This is implemented using methods ``scan_tests()``, ``load_tests()``
    and ``exec_test()``. This is where all the options that vstart_runner.py
    takes are implemented along with other features like logging and copying
    the traceback to the bottom of the log.

* provides an interface for issuing and testing shell commands
    The tests are written assuming that the cluster exists on remote machines.
    vstart_runner.py provides an interface to run the same tests with the
    cluster that exists within the local machine. This is done using the class
    ``LocalRemote``. Class ``LocalRemoteProcess`` can manage the process that
    executes the commands from ``LocalRemote``, class ``LocalDaemon`` provides
    an interface to handle Ceph daemons and class ``LocalFuseMount`` can
    create and handle FUSE mounts.

* provides an interface to operate Ceph cluster
    ``LocalCephManager`` provides methods to run Ceph cluster commands with
    and without admin socket and ``LocalCephCluster`` provides methods to set
    or clear ``ceph.conf``.

.. note:: vstart_runner.py deletes "adjust-ulimits" and "ceph-coverage" from
          the command arguments unconditionally since they are not applicable
          when tests are run on a developer's machine.

.. note:: "omit_sudo" is re-set to False unconditionally in cases of commands
          "passwd" and "chown".

.. note:: The presence of binary file named after the first argument is
          checked in ``<ceph-repo-root>/build/bin/``. If present, the first
          argument is replaced with the path to binary file.

Running Workunits Using vstart_enviroment.sh
--------------------------------------------

Code can be tested by building Ceph locally from source, starting a vstart
cluster, and running any suite against it.
Similar to S3-Tests, other workunits can be run against by configuring your environment.

Set up the environment
^^^^^^^^^^^^^^^^^^^^^^

Configure your environment::

    $ . ./build/vstart_enviroment.sh

Running a test
^^^^^^^^^^^^^^

To run a workunit (e.g ``mon/osd.sh``) do the following::

    $ ./qa/workunits/mon/osd.sh

.. _test_reconnect_timeout: https://github.com/ceph/ceph/blob/master/qa/tasks/cephfs/test_client_recovery.py#L133
.. _TestClientRecovery: https://github.com/ceph/ceph/blob/master/qa/tasks/cephfs/test_client_recovery.py#L86
.. _teuthology: https://github.com/ceph/teuthology
.. _vstart_runner.py: https://github.com/ceph/ceph/blob/master/qa/tasks/vstart_runner.py
