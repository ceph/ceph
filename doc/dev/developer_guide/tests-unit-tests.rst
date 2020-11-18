Testing - unit tests
====================

Ceph has two types of tests: 

#. unit tests (also called ``make check`` tests)
#. integration tests. 
   
What are here called ``make check`` tests are not, strictly speaking, "unit
tests". They are tests that can be easily run on a single-build machine
after Ceph has been compiled from source. Such ``make check`` tests do
not require packages or a multi-machine cluster.

Integration tests, however, require packages and multi-machine clusters.

.. _make-check:

What does "make check" mean?
----------------------------

After Ceph has been compiled, its code can be run through a battery of
tests that cover various aspects of Ceph. For historical reasons, this
battery of tests is often referred to as ``make check`` even though the
actual command used to run the tests is now ``ctest``. In order to be
included in this battery of tests, a test must:

* bind ports that do not conflict with other tests
* not require root access
* not require more than one machine to run
* complete within a few minutes

For the sake of simplicity, this class of tests is referred to as "make
check tests" or "unit tests". This is meant to distinguish these tests from
the more complex "integration tests" that are run via the `teuthology
framework`_.

While it is possible to run ``ctest`` directly, it can be tricky to
correctly set up your environment for it. Fortunately, a script is provided
to make it easier to run the unit tests on your code. This script can be
run from the top-level directory of the Ceph source tree by running the
following command:

.. prompt:: bash $

   ./run-make-check.sh

You will need a minimum of 8GB of RAM and 32GB of free disk space for this
command to complete successfully on x86_64 (other architectures may have
different constraints). Depending on your hardware, it can take from 20
minutes to three hours to complete.

How unit tests are declared
---------------------------

Unit tests are declared in the ``CMakeLists.txt`` file, which is found
in the ``./src`` directory. The ``add_ceph_test`` and 
``add_ceph_unittest`` CMake functions are used to declare unit tests.
``add_ceph_test`` and ``add_ceph_unittest`` are themselves defined in
``./cmake/modules/AddCephTest.cmake``. 

Some unit tests are scripts and other unit tests are binaries that are
compiled during the build process.  

* ``add_ceph_test`` function - used to declare unit test scripts 
* ``add_ceph_unittest`` function - used for unit test binaries

Unit testing of CLI tools
-------------------------

Some of the CLI tools are tested using special files ending with the extension
``.t`` and stored under ``./src/test/cli``. These tests are run using a tool
called `cram`_ via a shell script ``./src/test/run-cli-tests``.  `cram`_ tests
that are not suitable for ``make check`` may also be run by teuthology using
the `cram task`_.

.. _`cram`: https://bitheap.org/cram/
.. _`cram task`: https://github.com/ceph/ceph/blob/master/qa/tasks/cram.py

Tox based testing of python modules
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most python modules can be found under ``./src/pybind/``.

Many modules use **tox** to run their unit tests.
**tox** itself is a generic virtualenv management and test command line tool.

To find out quickly if tox can be run you can either just try to run ``tox``
or find out if a ``tox.ini`` exists.

Currently the following modules use tox:

- Cephadm (``./src/pybind/mgr/cephadm``)
- Insights (``./src/pybind/mgr/insights``)
- Manager core (``./src/pybind/mgr``)
- Dashboard (``./src/pybind/mgr/dashboard``)
- Python common (``./src/python-common/tox.ini``)


Most tox configuration support multiple environments and tasks. You can see
which environments and tasks are supported by looking into the ``tox.ini``
file to see what ``envlist`` is assigned.
To run **tox**, just execute ``tox`` in the directory where ``tox.ini`` lies.
Without any specified environments ``-e $env1,$env2``, all environments will
be run. Jenkins will run ``tox`` by executing ``run_tox.sh`` which lies under
``./src/script``.

Here some examples from ceph dashboard on how to specify different
environments and run options::

  ## Run Python 2+3 tests+lint commands:
  $ tox -e py27,py3,lint,check

  ## Run Python 3 tests+lint commands:
  $ tox -e py3,lint,check

  ## To run it like Jenkins would do
  $ ../../../script/run_tox.sh --tox-env py27,py3,lint,check
  $ ../../../script/run_tox.sh --tox-env py3,lint,check

Manager core unit tests
"""""""""""""""""""""""

Currently only doctests_ inside ``mgr_util.py`` are run.

To add more files that should be tested inside the core of the manager add
them at the end of the line that includes ``mgr_util.py`` inside ``tox.ini``.

.. _doctests: https://docs.python.org/3/library/doctest.html

Unit test caveats
-----------------

1. Unlike the various Ceph daemons and ``ceph-fuse``, the unit tests
   are linked against the default memory allocator (glibc) unless explicitly
   linked against something else. This enables tools like valgrind to be used
   in the tests.

.. _make check:
.. _teuthology framework: https://github.com/ceph/teuthology
