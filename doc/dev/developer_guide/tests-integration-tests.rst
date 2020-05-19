Testing - Integration Tests
===========================

Ceph has two types of tests: `make check`_ tests and integration tests.
When a test requires multiple machines, root access or lasts for a
longer time (for example, to simulate a realistic Ceph deployment), it
is deemed to be an integration test. Integration tests are organized into
"suites", which are defined in the `ceph/qa sub-directory`_ and run with
the ``teuthology-suite`` command.

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
<https://github.com/ceph/ceph/tree/master/qa/distros/supported>`_ (as
of December 2017 the list consisted of "CentOS 7.2" and "Ubuntu 16.04").  It
expects to be provided pre-built Ceph packages for these platforms.
Teuthology deploys these platforms on machines (bare-metal or
cloud-provisioned), installs the packages on them, and deploys Ceph
clusters on them - all as called for by the test.

The Nightlies
-------------

A number of integration tests are run on a regular basis in the `Sepia
lab`_ against the official Ceph repositories (on the ``master`` development
branch and the stable branches). Traditionally, these tests are called "the
nightlies" because the Ceph core developers used to live and work in
the same time zone and from their perspective the tests were run overnight.

The results of the nightlies are published at http://pulpito.ceph.com/. The
developer nick shows in the
test results URL and in the first column of the Pulpito dashboard.  The
results are also reported on the `ceph-qa mailing list
<https://ceph.com/irc/>`_ for analysis.

Testing Priority
----------------

The ``teuthology-suite`` command includes an almost mandatory option ``-p <N>``
which specifies the priority of the jobs submitted to the queue. The lower
the value of ``N``, the higher the priority. The option is almost mandatory
because the default is ``1000`` which matches the priority of the nightlies.
Nightlies are often half-finished and cancelled due to the volume of testing
done so your jobs may never finish. Therefore, it is common to select a
priority less than 1000.

Any priority may be selected when submitting jobs. But, in order to be
sensitive to the workings of other developers that also need to do testing,
the following recommendations should be followed:

* **Priority < 10:** Use this if the sky is falling and some group of tests
  must be run ASAP.

* **10 <= Priority < 50:** Use this if your tests are urgent and blocking
  other important development.

* **50 <= Priority < 75:** Use this if you are testing a particular
  feature/fix and running fewer than about 25 jobs. This range can also be
  used for urgent release testing.

* **75 <= Priority < 100:** Tech Leads will regularly schedule integration
  tests with this priority to verify pull requests against master.

* **100 <= Priority < 150:** This priority is to be used for QE validation of
  point releases.

* **150 <= Priority < 200:** Use this priority for 100 jobs or fewer of a
  particular feature/fix that you'd like results on in a day or so.

* **200 <= Priority < 1000:** Use this priority for large test runs that can
  be done over the course of a week.

In case you don't know how many jobs would be triggered by
``teuthology-suite`` command, use ``--dry-run`` to get a count first and then
issue ``teuthology-suite`` command again, this time without ``--dry-run`` and
with ``-p`` and an appropriate number as an argument to it.

Suites Inventory
----------------

The ``suites`` directory of the `ceph/qa sub-directory`_ contains
all the integration tests, for all the Ceph components.

`ceph-deploy <https://github.com/ceph/ceph/tree/master/qa/suites/ceph-deploy>`_
  install a Ceph cluster with ``ceph-deploy`` (:ref:`ceph-deploy man page <ceph-deploy>`)

`dummy <https://github.com/ceph/ceph/tree/master/qa/suites/dummy>`_
  get a machine, do nothing and return success (commonly used to
  verify the `integration testing`_ infrastructure works as expected)

`fs <https://github.com/ceph/ceph/tree/master/qa/suites/fs>`_
  test CephFS mounted using FUSE

`kcephfs <https://github.com/ceph/ceph/tree/master/qa/suites/kcephfs>`_
  test CephFS mounted using kernel

`krbd <https://github.com/ceph/ceph/tree/master/qa/suites/krbd>`_
  test the RBD kernel module

`multimds <https://github.com/ceph/ceph/tree/master/qa/suites/multimds>`_
  test CephFS with multiple MDSs

`powercycle <https://github.com/ceph/ceph/tree/master/qa/suites/powercycle>`_
  verify the Ceph cluster behaves when machines are powered off
  and on again

`rados <https://github.com/ceph/ceph/tree/master/qa/suites/rados>`_
  run Ceph clusters including OSDs and MONs, under various conditions of
  stress

`rbd <https://github.com/ceph/ceph/tree/master/qa/suites/rbd>`_
  run RBD tests using actual Ceph clusters, with and without qemu

`rgw <https://github.com/ceph/ceph/tree/master/qa/suites/rgw>`_
  run RGW tests using actual Ceph clusters

`smoke <https://github.com/ceph/ceph/tree/master/qa/suites/smoke>`_
  run tests that exercise the Ceph API with an actual Ceph cluster

`teuthology <https://github.com/ceph/ceph/tree/master/qa/suites/teuthology>`_
  verify that teuthology can run integration tests, with and without OpenStack

`upgrade <https://github.com/ceph/ceph/tree/master/qa/suites/upgrade>`_
  for various versions of Ceph, verify that upgrades can happen
  without disrupting an ongoing workload

.. _`ceph-deploy man page`: ../../man/8/ceph-deploy

teuthology-describe-tests
-------------------------

In February 2016, a new feature called ``teuthology-describe-tests`` was
added to the `teuthology framework`_ to facilitate documentation and better
understanding of integration tests (`feature announcement
<http://article.gmane.org/gmane.comp.file-systems.ceph.devel/29287>`_).

The upshot is that tests can be documented by embedding ``meta:``
annotations in the yaml files used to define the tests. The results can be
seen in the `ceph-qa-suite wiki
<http://tracker.ceph.com/projects/ceph-qa-suite/wiki/>`_.

Since this is a new feature, many yaml files have yet to be annotated.
Developers are encouraged to improve the documentation, in terms of both
coverage and quality.

How integration tests are run
-----------------------------

Given that - as a new Ceph developer - you will typically not have access
to the `Sepia lab`_, you may rightly ask how you can run the integration
tests in your own environment.

One option is to set up a teuthology cluster on bare metal. Though this is
a non-trivial task, it `is` possible. Here are `some notes
<http://docs.ceph.com/teuthology/docs/LAB_SETUP.html>`_ to get you started
if you decide to go this route.

If you have access to an OpenStack tenant, you have another option: the
`teuthology framework`_ has an OpenStack backend, which is documented `here
<https://github.com/dachary/teuthology/tree/openstack#openstack-backend>`__.
This OpenStack backend can build packages from a given git commit or
branch, provision VMs, install the packages and run integration tests
on those VMs. This process is controlled using a tool called
``ceph-workbench ceph-qa-suite``. This tool also automates publishing of
test results at http://teuthology-logs.public.ceph.com.

Running integration tests on your code contributions and publishing the
results allows reviewers to verify that changes to the code base do not
cause regressions, or to analyze test failures when they do occur.

Every teuthology cluster, whether bare-metal or cloud-provisioned, has a
so-called "teuthology machine" from which tests suites are triggered using the
``teuthology-suite`` command.

A detailed and up-to-date description of each `teuthology-suite`_ option is
available by running the following command on the teuthology machine::

   $ teuthology-suite --help

.. _teuthology-suite: http://docs.ceph.com/teuthology/docs/teuthology.suite.html

How integration tests are defined
---------------------------------

Integration tests are defined by yaml files found in the ``suites``
subdirectory of the `ceph/qa sub-directory`_ and implemented by python
code found in the ``tasks`` subdirectory. Some tests ("standalone tests")
are defined in a single yaml file, while other tests are defined by a
directory tree containing yaml files that are combined, at runtime, into a
larger yaml file.

Reading a standalone test
-------------------------

Let us first examine a standalone test, or "singleton".

Here is a commented example using the integration test
`rados/singleton/all/admin-socket.yaml
<https://github.com/ceph/ceph/blob/master/qa/suites/rados/singleton/all/admin-socket.yaml>`_
::

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

This test can be run with::

    $ teuthology-suite --machine-type smithi --suite rados/singleton/all/admin-socket.yaml fs/ext4.yaml

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

The convolution operator, implemented as an empty file called ``%``, tells
teuthology to construct a test matrix from yaml facets found in
subdirectories below the directory containing the operator.

For example, the `ceph-deploy suite
<https://github.com/ceph/ceph/tree/jewel/qa/suites/ceph-deploy/>`_ is
defined by the ``suites/ceph-deploy/`` tree, which consists of the files and
subdirectories in the following structure::

  directory: ceph-deploy/basic
      file: %
      directory: distros
         file: centos_7.0.yaml
         file: ubuntu_16.04.yaml
      directory: tasks
         file: ceph-deploy.yaml

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
(also known as the "ceph-deploy suite") can be run with::

  $ teuthology-suite --machine-type smithi --suite ceph-deploy

An individual test from the `ceph-deploy suite`_ can be run by adding the
``--filter`` option::

  $ teuthology-suite \
      --machine-type smithi \
      --suite ceph-deploy/basic \
      --filter 'ceph-deploy/basic/{distros/ubuntu_16.04.yaml tasks/ceph-deploy.yaml}'

.. note:: To run a standalone test like the one in `Reading a standalone
   test`_, ``--suite`` alone is sufficient. If you want to run a single
   test from a suite that is defined as a directory tree, ``--suite`` must
   be combined with ``--filter``. This is because the ``--suite`` option
   understands POSIX relative paths only.

Concatenation operator
^^^^^^^^^^^^^^^^^^^^^^

For even greater flexibility in sharing yaml files between suites, the
special file plus (``+``) can be used to concatenate files within a
directory. For instance, consider the `suites/rbd/thrash
<https://github.com/ceph/ceph/tree/master/qa/suites/rbd/thrash>`_
tree::

  directory: rbd/thrash
    file: %
    directory: clusters
      file: +
      file: fixed-2.yaml
      file: openstack.yaml
    directory: workloads
      file: rbd_api_tests_copy_on_read.yaml
      file: rbd_api_tests.yaml

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
define the following ``roles``::

  roles:
  - [mon.a, mon.c, osd.0, osd.1, osd.2, client.0]
  - [mon.b, osd.3, osd.4, osd.5, client.1]

The ``rbd/thrash`` suite as defined above, consisting of two tests,
can be run with::

  $ teuthology-suite --machine-type smithi --suite rbd/thrash

A single test from the rbd/thrash suite can be run by adding the
``--filter`` option::

  $ teuthology-suite \
      --machine-type smithi \
      --suite rbd/thrash \
      --filter 'rbd/thrash/{clusters/fixed-2.yaml clusters/openstack.yaml workloads/rbd_api_tests_copy_on_read.yaml}'

Filtering tests by their description
------------------------------------

When a few jobs fail and need to be run again, the ``--filter`` option
can be used to select tests with a matching description. For instance, if the
``rados`` suite fails the `all/peer.yaml <https://github.com/ceph/ceph/blob/master/qa/suites/rados/singleton/all/peer.yaml>`_ test, the following will only
run the tests that contain this file::

  teuthology-suite --machine-type smithi --suite rados --filter all/peer.yaml

The ``--filter-out`` option does the opposite (it matches tests that do `not`
contain a given string), and can be combined with the ``--filter`` option.

Both ``--filter`` and ``--filter-out`` take a comma-separated list of strings
(which means the comma character is implicitly forbidden in filenames found in
the `ceph/qa sub-directory`_). For instance::

  teuthology-suite --machine-type smithi --suite rados --filter all/peer.yaml,all/rest-api.yaml

will run tests that contain either
`all/peer.yaml <https://github.com/ceph/ceph/blob/master/qa/suites/rados/singleton/all/peer.yaml>`_
or
`all/rest-api.yaml <https://github.com/ceph/ceph/blob/master/qa/suites/rados/singleton/all/rest-api.yaml>`_

Each string is looked up anywhere in the test description and has to
be an exact match: they are not regular expressions.

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
instance::

  teuthology-suite --machine-type smithi --suite rados --subset 0/4000

will run as few tests as possible. The tradeoff in this case is that
not all combinations of test variations will together,
but no matter how small a ratio is provided in the ``--subset``,
teuthology will still ensure that all files in the suite are in at
least one test. Understanding the actual logic that drives this
requires reading the teuthology source code.

The ``--limit`` option only runs the first ``N`` tests in the suite:
this is rarely useful, however, because there is no way to control which
test will be first.

.. _ceph/qa sub-directory: https://github.com/ceph/ceph/tree/master/qa
.. _Integration testing: testing-integration-tests
.. _make check:
.. _Sepia Lab: https://wiki.sepia.ceph.com/doku.php
.. _teuthology repository: https://github.com/ceph/teuthology
.. _teuthology framework: https://github.com/ceph/teuthology
