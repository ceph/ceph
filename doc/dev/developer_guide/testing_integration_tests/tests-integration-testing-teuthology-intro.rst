.. _tests-integration-testing-teuthology-intro:

Testing - Integration Tests - Introduction
==========================================

Ceph has two types of tests: :ref:`make check <make-check>` tests and
integration tests. When a test requires multiple machines, root access, or lasts
for a long time (for example, to simulate a realistic Ceph workload), it is
deemed to be an integration test. Integration tests are organized into "suites",
which are defined in the `ceph/qa sub-directory`_ and run with the
``teuthology-suite`` command.

The ``teuthology-suite`` command is part of the `teuthology framework`_.
In the sections that follow we attempt to provide a detailed introduction
to that framework from the perspective of a beginning Ceph developer.

Teuthology consumes packages
----------------------------

It may take some time to understand the significance of this fact, but it
is `very` significant. It means that automated tests can be conducted on
multiple platforms using the same packages (RPM, DEB) that can be
installed on any machine running those platforms.

Teuthology has a `list of platforms that it supports
<https://github.com/ceph/ceph/tree/master/qa/distros/supported>`_ (as of
September 2020 the list consisted of "RHEL/CentOS 8" and "Ubuntu 18.04"). It
expects to be provided pre-built Ceph packages for these platforms.  Teuthology
deploys these platforms on machines (bare-metal or cloud-provisioned), installs
the packages on them, and deploys Ceph clusters on them - all as called for by
the test.

The Nightlies
-------------

A number of integration tests are run on a regular basis in the `Sepia
lab`_ against the official Ceph repositories (on the ``master`` development
branch and the stable branches). Traditionally, these tests are called "the
nightlies" because the Ceph core developers used to live and work in
the same time zone and from their perspective the tests were run overnight.

The results of nightly test runs are published at http://pulpito.ceph.com/
under the user ``teuthology``. The developer nick appears in URL of the the
test results and in the first column of the Pulpito dashboard.  The results are
also reported on the `ceph-qa mailing list <https://ceph.com/irc/>`_.

Testing Priority
----------------

In brief: in the ``teuthology-suite`` command option ``-p <N>``, set the value of ``<N>`` to a number lower than 1000. An explanation of why follows.

The ``teuthology-suite`` command includes an option ``-p <N>``. This option specifies the priority of the jobs submitted to the queue. The lower the value of ``N``, the higher the priority.

The default value of ``N`` is ``1000``. This is the same priority value given to the nightly tests (the nightlies). Often, the volume of testing done during the nightly tests is so great that the full number of nightly tests do not get run during the time allotted for their run.

Set the value of ``N`` lower than ``1000``, or your tests will not have priority over the nightly tests. This means that they might never run.

Select your job's priority (the value of ``N``) in accordance with the following guidelines:

.. list-table::
   :widths: 30 30
   :header-rows: 1

   * - Priority
     - Explanation
   * - **N < 10**
     - Use this if the sky is falling and some group of tests must be run ASAP.
   * - **10 <= N < 50**
     - Use this if your tests are urgent and blocking other important
       development.
   * - **50 <= N < 75**
     - Use this if you are testing a particular feature/fix and running fewer
       than about 25 jobs. This range is also used for urgent release testing.
   * - **75 <= N < 100**
     - Tech Leads regularly schedule integration tests with this priority to
       verify pull requests against master.
   * - **100 <= N < 150**
     - This priority is used for QE validation of point releases.
   * - **150 <= N < 200**
     - Use this priority for 100 jobs or fewer that test a particular feature
       or fix.  Results are available in about 24 hours.
   * - **200 <= N < 1000**
     - Use this priority for large test runs.  Results are available in about a
       week.

To see how many jobs the ``teuthology-suite`` command will trigger, use the
``--dry-run`` flag. If you are happy with the number of jobs returned by the
dry run, issue the ``teuthology-suite`` command again without ``--dry-run`` and
with ``-p`` and an appropriate number as an argument. 

To skip the priority check, use ``--force-priority``. Be considerate of the needs of other developers to run tests, and use ``--force-priority`` only in emergencies. 

Suites Inventory
----------------

The ``suites`` directory of the `ceph/qa sub-directory`_ contains all the
integration tests for all the Ceph components.

.. list-table:: **Suites**

  * - **Component**
    - **Function**

  * - `ceph-deploy <https://github.com/ceph/ceph/tree/master/qa/suites/ceph-deploy>`_
    - install a Ceph cluster with ``ceph-deploy`` (`ceph-deploy man page`_)

  * - `dummy <https://github.com/ceph/ceph/tree/master/qa/suites/dummy>`_
    - get a machine, do nothing and return success (commonly used to verify
      that the integration testing infrastructure works as expected)

  * - `fs <https://github.com/ceph/ceph/tree/master/qa/suites/fs>`_
    - test CephFS mounted using kernel and FUSE clients, also with multiple MDSs.

  * - `krbd <https://github.com/ceph/ceph/tree/master/qa/suites/krbd>`_
    - test the RBD kernel module

  * - `powercycle <https://github.com/ceph/ceph/tree/master/qa/suites/powercycle>`_
    - verify the Ceph cluster behaves when machines are powered off and on
      again

  * - `rados <https://github.com/ceph/ceph/tree/master/qa/suites/rados>`_
    - run Ceph clusters including OSDs and MONs, under various conditions of
      stress

  * - `rbd <https://github.com/ceph/ceph/tree/master/qa/suites/rbd>`_
    - run RBD tests using actual Ceph clusters, with and without qemu

  * - `rgw <https://github.com/ceph/ceph/tree/master/qa/suites/rgw>`_
    - run RGW tests using actual Ceph clusters

  * - `smoke <https://github.com/ceph/ceph/tree/master/qa/suites/smoke>`_
    - run tests that exercise the Ceph API with an actual Ceph cluster

  * - `teuthology <https://github.com/ceph/ceph/tree/master/qa/suites/teuthology>`_ 
    - verify that teuthology can run integration tests, with and without OpenStack

  * - `upgrade <https://github.com/ceph/ceph/tree/master/qa/suites/upgrade>`_
    - for various versions of Ceph, verify that upgrades can happen without disrupting an ongoing workload (`Upgrade Testing`_)

teuthology-describe
-------------------

``teuthology-describe`` was added to the `teuthology framework`_ to facilitate
documentation and better understanding of integration tests.

Tests can be documented by embedding ``meta:`` annotations in the yaml files
used to define the tests. The results can be seen in the `teuthology-describe
usecases`_

Since this is a new feature, many yaml files have yet to be annotated.
Developers are encouraged to improve the coverage and the quality of the
documentation. 

How to run integration tests 
----------------------------

Typically, the `Sepia lab`_ is used to run integration tests. But as a new Ceph
developer, you will probably not have access to the `Sepia lab`_.  You might
however be able to run some integration tests in an environment separate from
the `Sepia lab`_ . Ask members from the relevant team how to do this. 

One way to run your own integration tests is to set up a teuthology cluster on
bare metal. Setting up a teuthology cluster on bare metal is a complex task.
Here are `some notes
<https://docs.ceph.com/projects/teuthology/en/latest/LAB_SETUP.html>`_ to get
you started if you decide that you are interested in undertaking the complex
task of setting up a teuthology cluster on bare metal.

Running integration tests on your code contributions and publishing the results
allows reviewers to verify that changes to the code base do not cause
regressions, and allows reviewers to analyze test failures when they occur.

Every teuthology cluster, whether bare-metal or cloud-provisioned, has a
so-called "teuthology machine" from which tests suites are triggered using the
``teuthology-suite`` command.

A detailed and up-to-date description of each `teuthology-suite`_ option is
available by running the following command on the teuthology machine:

.. prompt:: bash $

   teuthology-suite --help

.. _teuthology-suite: https://docs.ceph.com/projects/teuthology/en/latest/commands/teuthology-suite.html

How integration tests are defined
---------------------------------

Integration tests are defined by yaml files found in the ``suites``
subdirectory of the `ceph/qa sub-directory`_ and implemented by python
code found in the ``tasks`` subdirectory. Some tests ("standalone tests")
are defined in a single yaml file, while other tests are defined by a
directory tree containing yaml files that are combined, at runtime, into a
larger yaml file.


.. _reading-standalone-test:

Reading a standalone test
-------------------------

Let us first examine a standalone test, or "singleton".

Here is a commented example using the integration test
`rados/singleton/all/admin-socket.yaml
<https://github.com/ceph/ceph/blob/master/qa/suites/rados/singleton/all/admin-socket.yaml>`_

.. code-block:: yaml

      roles:
      - - mon.a
        - osd.0
        - osd.1
      tasks:
      - install:
      - ceph:
      - admin_socket:
          osd.0:
            version:
            git_version:
            help:
            config show:
            config set filestore_dump_file /tmp/foo:
            perf dump:
            perf schema:

The ``roles`` array determines the composition of the cluster (how
many MONs, OSDs, etc.) on which this test is designed to run, as well
as how these roles will be distributed over the machines in the
testing cluster. In this case, there is only one element in the
top-level array: therefore, only one machine is allocated to the
test. The nested array declares that this machine shall run a MON with
id ``a`` (that is the ``mon.a`` in the list of roles) and two OSDs
(``osd.0`` and ``osd.1``).

The body of the test is in the ``tasks`` array: each element is
evaluated in order, causing the corresponding python file found in the
``tasks`` subdirectory of the `teuthology repository`_ or
`ceph/qa sub-directory`_ to be run. "Running" in this case means calling
the ``task()`` function defined in that file.

In this case, the `install
<https://github.com/ceph/teuthology/blob/master/teuthology/task/install/__init__.py>`_
task comes first. It installs the Ceph packages on each machine (as
defined by the ``roles`` array). A full description of the ``install``
task is `found in the python file
<https://github.com/ceph/teuthology/blob/master/teuthology/task/install/__init__.py>`_
(search for "def task").

The ``ceph`` task, which is documented `here
<https://github.com/ceph/ceph/blob/master/qa/tasks/ceph.py>`__ (again,
search for "def task"), starts OSDs and MONs (and possibly MDSs as well)
as required by the ``roles`` array. In this example, it will start one MON
(``mon.a``) and two OSDs (``osd.0`` and ``osd.1``), all on the same
machine. Control moves to the next task when the Ceph cluster reaches
``HEALTH_OK`` state.

The next task is ``admin_socket`` (`source code
<https://github.com/ceph/ceph/blob/master/qa/tasks/admin_socket.py>`_).
The parameter of the ``admin_socket`` task (and any other task) is a
structure which is interpreted as documented in the task. In this example
the parameter is a set of commands to be sent to the admin socket of
``osd.0``. The task verifies that each of them returns on success (i.e.
exit code zero).

This test can be run with

.. prompt:: bash $

   teuthology-suite --machine-type smithi --suite rados/singleton/all/admin-socket.yaml fs/ext4.yaml

Test descriptions
-----------------

Each test has a "test description", which is similar to a directory path,
but not the same. In the case of a standalone test, like the one in
`Reading a standalone test`_, the test description is identical to the
relative path (starting from the ``suites/`` directory of the
`ceph/qa sub-directory`_) of the yaml file defining the test.

Much more commonly, tests are defined not by a single yaml file, but by a
`directory tree of yaml files`. At runtime, the tree is walked and all yaml
files (facets) are combined into larger yaml "programs" that define the
tests. A full listing of the yaml defining the test is included at the
beginning of every test log.

In these cases, the description of each test consists of the
subdirectory under `suites/
<https://github.com/ceph/ceph/tree/master/qa/suites>`_ containing the
yaml facets, followed by an expression in curly braces (``{}``) consisting of
a list of yaml facets in order of concatenation. For instance the
test description::

  ceph-deploy/basic/{distros/centos_7.0.yaml tasks/ceph-deploy.yaml}

signifies the concatenation of two files:

* ceph-deploy/basic/distros/centos_7.0.yaml
* ceph-deploy/basic/tasks/ceph-deploy.yaml

How tests are built from directories
------------------------------------

As noted in the previous section, most tests are not defined in a single
yaml file, but rather as a `combination` of files collected from a
directory tree within the ``suites/`` subdirectory of the `ceph/qa sub-directory`_.

The set of all tests defined by a given subdirectory of ``suites/`` is
called an "integration test suite", or a "teuthology suite".

Combination of yaml facets is controlled by special files (``%`` and
``+``) that are placed within the directory tree and can be thought of as
operators.  The ``%`` file is the "convolution" operator and ``+``
signifies concatenation.

Convolution operator
^^^^^^^^^^^^^^^^^^^^

The convolution operator, implemented as a (typically empty) file called ``%``,
tells teuthology to construct a test matrix from yaml facets found in
subdirectories below the directory containing the operator.

For example, the `ceph-deploy suite
<https://github.com/ceph/ceph/tree/master/qa/suites/ceph-deploy/>`_ is
defined by the ``suites/ceph-deploy/`` tree, which consists of the files and
subdirectories in the following structure

.. code-block:: none

  qa/suites/ceph-deploy
  ├── %
  ├── distros
  │   ├── centos_latest.yaml
  │   └── ubuntu_latest.yaml
  └── tasks
      ├── ceph-admin-commands.yaml
      └── rbd_import_export.yaml

This is interpreted as a 2x1 matrix consisting of two tests:

1. ceph-deploy/basic/{distros/centos_7.0.yaml tasks/ceph-deploy.yaml}
2. ceph-deploy/basic/{distros/ubuntu_16.04.yaml tasks/ceph-deploy.yaml}

i.e. the concatenation of centos_7.0.yaml and ceph-deploy.yaml and
the concatenation of ubuntu_16.04.yaml and ceph-deploy.yaml, respectively.
In human terms, this means that the task found in ``ceph-deploy.yaml`` is
intended to run on both CentOS 7.0 and Ubuntu 16.04.

Without the file percent, the ``ceph-deploy`` tree would be interpreted as
three standalone tests:

* ceph-deploy/basic/distros/centos_7.0.yaml
* ceph-deploy/basic/distros/ubuntu_16.04.yaml
* ceph-deploy/basic/tasks/ceph-deploy.yaml

(which would of course be wrong in this case).

Referring to the `ceph/qa sub-directory`_, you will notice that the
``centos_7.0.yaml`` and ``ubuntu_16.04.yaml`` files in the
``suites/ceph-deploy/basic/distros/`` directory are implemented as symlinks.
By using symlinks instead of copying, a single file can appear in multiple
suites. This eases the maintenance of the test framework as a whole.

All the tests generated from the ``suites/ceph-deploy/`` directory tree
(also known as the "ceph-deploy suite") can be run with

.. prompt:: bash $

   teuthology-suite --machine-type smithi --suite ceph-deploy

An individual test from the `ceph-deploy suite`_ can be run by adding the
``--filter`` option

.. prompt:: bash $

   teuthology-suite \
      --machine-type smithi \
      --suite ceph-deploy/basic \
      --filter 'ceph-deploy/basic/{distros/ubuntu_16.04.yaml tasks/ceph-deploy.yaml}'

.. note:: To run a standalone test like the one in `Reading a standalone
   test`_, ``--suite`` alone is sufficient. If you want to run a single
   test from a suite that is defined as a directory tree, ``--suite`` must
   be combined with ``--filter``. This is because the ``--suite`` option
   understands POSIX relative paths only.

Nested Subsets
^^^^^^^^^^^^^^

Suites can get quite large with the combinatorial explosion of yaml
configurations. At the time of writing, the ``rados``` suite is more than
100,000 jobs. For this reason, scheduling often uses the ``--subset`` option to
only run a subset of the jobs (see also: :ref:`subset`). However, this applies
only at the top-level of the suite being run (e.g. ``fs``). That may
incidentally inflate the ratio of jobs for some larger sub-suites (like
``fs:workload``) vs.  smaller but critical suites (like ``fs:volumes``).

It is therefore attractive to automatically subset some sub-suites which are
never run fully. This is done by providing an integer divisor for the ``%``
convolution operator file instead of leaving it empty. That divisor
automatically subsets the resulting matrix. For example, if the convolution
file ``%`` contains ``2``, the matrix will be divided into two using the same
logic as the ``--subset`` mechanism.

Note the numerator is not specified as with the ``--subset`` option as there is
no meaningful way to express this when there could be several layers of
nesting.  Instead, a random subset is selected (1 of 2 in our example). The
choice is based off the random seed (``--seed``) used for the scheduling.
Remember that seed is saved in the results so that a ``--rerun`` of failed
tests will still preserve the correct numerator (subset of subsets).

You can disable nested subsets using the ``--no-nested-subset`` argument to
``teuthology-suite``.

Concatenation operator
^^^^^^^^^^^^^^^^^^^^^^

For even greater flexibility in sharing yaml files between suites, the
special file plus (``+``) can be used to concatenate files within a
directory. For instance, consider the `suites/rbd/thrash
<https://github.com/ceph/ceph/tree/master/qa/suites/rbd/thrash>`_
tree

.. code-block:: none

  qa/suites/rbd/thrash
  ├── %
  ├── clusters
  │   ├── +
  │   ├── fixed-2.yaml
  │   └── openstack.yaml
  └── workloads
      ├── rbd_api_tests_copy_on_read.yaml
      ├── rbd_api_tests.yaml
      └── rbd_fsx_rate_limit.yaml

This creates two tests:

* rbd/thrash/{clusters/fixed-2.yaml clusters/openstack.yaml workloads/rbd_api_tests_copy_on_read.yaml}
* rbd/thrash/{clusters/fixed-2.yaml clusters/openstack.yaml workloads/rbd_api_tests.yaml}

Because the ``clusters/`` subdirectory contains the special file plus
(``+``), all the other files in that subdirectory (``fixed-2.yaml`` and
``openstack.yaml`` in this case) are concatenated together
and treated as a single file. Without the special file plus, they would
have been convolved with the files from the workloads directory to create
a 2x2 matrix:

* rbd/thrash/{clusters/openstack.yaml workloads/rbd_api_tests_copy_on_read.yaml}
* rbd/thrash/{clusters/openstack.yaml workloads/rbd_api_tests.yaml}
* rbd/thrash/{clusters/fixed-2.yaml workloads/rbd_api_tests_copy_on_read.yaml}
* rbd/thrash/{clusters/fixed-2.yaml workloads/rbd_api_tests.yaml}

The ``clusters/fixed-2.yaml`` file is shared among many suites to
define the following ``roles``

.. code-block:: yaml

  roles:
  - [mon.a, mon.c, osd.0, osd.1, osd.2, client.0]
  - [mon.b, osd.3, osd.4, osd.5, client.1]

The ``rbd/thrash`` suite as defined above, consisting of two tests,
can be run with

.. prompt:: bash $

   teuthology-suite --machine-type smithi --suite rbd/thrash

A single test from the rbd/thrash suite can be run by adding the
``--filter`` option

.. prompt:: bash $

   teuthology-suite \
      --machine-type smithi \
      --suite rbd/thrash \
      --filter 'rbd/thrash/{clusters/fixed-2.yaml clusters/openstack.yaml workloads/rbd_api_tests_copy_on_read.yaml}'

.. _upgrade-testing:

Upgrade Testing
^^^^^^^^^^^^^^^

Using the upgrade suite we are able to verify that upgrades from earlier releases can complete
successfully without disrupting any ongoing workload.
Each Release branch upgrade directory includes 2-x upgrade testing.
Meaning, we are able to test the upgrade from 2 preceding releases to the current one.
The upgrade sequence is done in `parallel <https://github.com/ceph/teuthology/blob/main/teuthology/task/parallel.py>`_
with other given workloads.

For instance, the upgrade test directory from the Quincy release branch is as follows:

.. code-block:: none

  .
  ├── octopus-x
  └── pacific-x

It is possible to test upgrades from Octopus (2-x) or from Pacific (1-x) to Quincy (x).
A simple upgrade test consists the following order:

.. code-block:: none

  ├── 0-start.yaml
  ├── 1-tasks.yaml
  ├── upgrade-sequence.yaml
  └── workload

After starting the cluster with the older release we begin running the given ``workload``
and the ``upgrade-sequnce`` in parallel.

.. code-block:: yaml

  - print: "**** done start parallel"
  - parallel:
      - workload
      - upgrade-sequence
  - print: "**** done end parallel"

While the ``workload`` directory consists regular yaml files just as in any other suite,
the ``upgrade-sequnce`` is resposible for running the upgrade and awaitng its completion:

.. code-block:: yaml

  - print: "**** done start upgrade, wait"
  ...
    mon.a:
      - ceph orch upgrade start --image quay.ceph.io/ceph-ci/ceph:$sha1
      - while ceph orch upgrade status | jq '.in_progress' | grep true ; do ceph orch ps ; ceph versions ; sleep 30 ; done\
  ...
  - print: "**** done end upgrade, wait..."


It is also possible to upgrade in stages while running workloads in between those:

.. code-block:: none

  ├── %
  ├── 0-cluster
  ├── 1-ceph-install
  ├── 2-partial-upgrade
  ├── 3-thrash
  ├── 4-workload
  ├── 5-finish-upgrade.yaml
  ├── 6-quincy.yaml
  └── 8-final-workload

After starting a cluster we upgrade only 2/3 of the cluster (``2-partial-upgrade``).
The next stage is running thrash tests and given workload tests. Later on, continuing to upgrade the
rest of the cluster (``5-finish-upgrade.yaml``).
The last stage is requiring the updated release (``ceph require-osd-release quincy``,
``ceph osd set-require-min-compat-client quincy``) and running the ``final-workload``.

Position Independent Linking
----------------------------

Under the ``qa/suites`` directory are ``.qa`` symbolic links in every
directory. Each link is recursive by always linking to ``../.qa/``. The final
terminating link is in the ``qa/`` directory itself as ``qa/.qa -> .``. This
layout of symbolic links allows a suite to be easily copied or moved without
breaking a number of symbolic links. For example::

    qa/suites/fs/upgrade/nofs/centos_latest.yaml -> .qa/distros/supported/centos_latest.yaml

If we copy the ``nofs`` suite somewhere else, add a parent directory above
``nofs``, or move the ``centos_latest.yaml`` fragment into a sub-directory, the
link will not break. Compare to::

    qa/suites/fs/upgrade/nofs/centos_latest.yaml -> ../../../../distros/supported/centos_latest.yaml

If the link is moved, it is very likely it will break because the number of
parent directories to reach the ``distros`` directory may change.

When adding new directories or suites, it is recommended to also remember
adding ``.qa`` symbolic links. A trivial find command may do this for you:

.. prompt:: bash $

   find qa/suites/ -type d -execdir ln -sfT ../.qa/ {}/.qa \;


Filtering tests by their description
------------------------------------

When a few jobs fail and need to be run again, the ``--filter`` option
can be used to select tests with a matching description. For instance, if the
``rados`` suite fails the `all/peer.yaml <https://github.com/ceph/ceph/blob/master/qa/suites/rados/singleton/all/peer.yaml>`_ test, the following will only
run the tests that contain this file

.. prompt:: bash $

   teuthology-suite --machine-type smithi --suite rados --filter all/peer.yaml

The ``--filter-out`` option does the opposite (it matches tests that do `not`
contain a given string), and can be combined with the ``--filter`` option.

Both ``--filter`` and ``--filter-out`` take a comma-separated list of strings
(which means the comma character is implicitly forbidden in filenames found in
the `ceph/qa sub-directory`_). For instance

.. prompt:: bash $

  teuthology-suite --machine-type smithi --suite rados --filter all/peer.yaml,all/rest-api.yaml

will run tests that contain either
`all/peer.yaml <https://github.com/ceph/ceph/blob/master/qa/suites/rados/singleton/all/peer.yaml>`_
or
`all/rest-api.yaml <https://github.com/ceph/ceph/blob/master/qa/suites/rados/singleton/all/rest-api.yaml>`_

Each string is looked up anywhere in the test description and has to
be an exact match: they are not regular expressions.


.. _subset:

Reducing the number of tests
----------------------------

The ``rados`` suite generates tens or even hundreds of thousands of tests out
of a few hundred files. This happens because teuthology constructs test
matrices from subdirectories wherever it encounters a file named ``%``. For
instance, all tests in the `rados/basic suite
<https://github.com/ceph/ceph/tree/master/qa/suites/rados/basic>`_ run with
different messenger types: ``simple``, ``async`` and ``random``, because they
are combined (via the special file ``%``) with the `msgr directory
<https://github.com/ceph/ceph/tree/master/qa/suites/rados/basic/msgr>`_

All integration tests are required to be run before a Ceph release is
published. When merely verifying whether a contribution can be merged without
risking a trivial regression, it is enough to run a subset. The ``--subset``
option can be used to reduce the number of tests that are triggered. For
instance

.. prompt:: bash $

   teuthology-suite --machine-type smithi --suite rados --subset 0/4000

will run as few tests as possible. The tradeoff in this case is that
not all combinations of test variations will together,
but no matter how small a ratio is provided in the ``--subset``,
teuthology will still ensure that all files in the suite are in at
least one test. Understanding the actual logic that drives this
requires reading the teuthology source code.

Note: some suites are now using a **nested subset** feature that automatically
applies a subset to a carefully chosen set of YAML configurations. You may
disable this behavior (for some custom filtering, perhaps) using the
``--no-nested-subset`` option.

The ``--limit`` option only runs the first ``N`` tests in the suite:
this is rarely useful, however, because there is no way to control which
test will be first.

.. _ceph/qa sub-directory: https://github.com/ceph/ceph/tree/master/qa
.. _Sepia Lab: https://wiki.sepia.ceph.com/doku.php
.. _teuthology repository: https://github.com/ceph/teuthology
.. _teuthology framework: https://github.com/ceph/teuthology
.. _teuthology-describe usecases: https://gist.github.com/jdurgin/09711d5923b583f60afc
.. _ceph-deploy man page: ../../../../man/8/ceph-deploy
