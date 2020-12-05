Testing - unit tests
====================

The Ceph GitHub repository has two types of tests: unit tests (also called
``make check`` tests) and integration tests. Strictly speaking, the
``make check`` tests are not "unit tests", but rather tests that can be run
easily on a single build machine after compiling Ceph from source, whereas
integration tests require package installation and multi-machine clusters to
run.

.. _make-check:

What does "make check" mean?
----------------------------

After compiling Ceph, the code can be run through a battery of tests
For historical reasons, this is
often referred to as ``make check`` even though the actual command used to run
the tests is now ``ctest``. For inclusion in this group of tests, a test
must:


* bind ports that do not conflict with other tests
* not require root access
* not require more than one machine to run
* complete within a few minutes

For the sake of simplicity, this class of tests is referred to as "make
check tests" or "unit tests". This is meant to distinguish these tests from
the more complex "integration tests" that are run via the `teuthology
framework`_.

While it is possible to run ``ctest`` directly, it can be tricky to correctly
set up your environment. Fortunately, a script is provided to make it easier
run the unit tests on your code. It can be run from the top-level directory of
the Ceph source tree by invoking:

  .. prompt:: bash $

     ./run-make-check.sh

You will need a minimum of 8GB of RAM and 32GB of free drive space for this
command to complete successfully on x86_64; other architectures may have
different requirements. Depending on your hardware, it can take from twenty
minutes to three hours to complete, but it's worth the wait.


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

To find out quickly if **tox** can be run you can either just try to run ``tox``
or check for the existence of a ``tox.ini`` file.

Currently the following modules use **tox**:

- Cephadm (``./src/pybind/mgr/cephadm``)
- Insights (``./src/pybind/mgr/insights``)
- Manager core (``./src/pybind/mgr``)
- Dashboard (``./src/pybind/mgr/dashboard``)
- Python common (``./src/python-common/tox.ini``)


Most **tox** configurations support multiple environments and tasks. You can see
which are supported by examining the ``envlist`` assignment within ``tox.ini``
To run **tox**, just execute ``tox`` in the directory where ``tox.ini`` is found.
If no environments are specified with e.g. ``-e $env1,$env2``, all environments
will be run. Jenkins will run ``tox`` by executing ``run_tox.sh`` which is under
``./src/script``.

Here some examples from the Ceph Dashboard on how to specify
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

To add test additional files inside the core of the manager, add
them at the end of the line that includes ``mgr_util.py`` within ``tox.ini``.

.. _doctests: https://docs.python.org/3/library/doctest.html

Unit test caveats
-----------------

1. Unlike the various Ceph daemons and ``ceph-fuse``, unit tests
   are linked against the default memory allocator (glibc) unless explicitly
   linked against something else. This enables tools like **valgrind** to be used
   in the tests.

.. _make check:
.. _teuthology framework: https://github.com/ceph/teuthology
