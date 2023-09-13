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

After compiling Ceph, the code can be run through a battery of tests. For
historical reasons, this is often referred to as ``make check`` even though
the actual command used to run the tests is now ``ctest``. To be included in
this group of tests, a test must:

* bind ports that do not conflict with other tests
* not require root access
* not require more than one machine to run
* complete within a few minutes

For the sake of simplicity, this class of tests is referred to as "make
check tests" or "unit tests". This is meant to distinguish these tests from
the more complex "integration tests" that are run via the `teuthology
framework`_.

While it is possible to run ``ctest`` directly, it can be tricky to correctly
set up your environment for it. Fortunately, there is a script that makes it
easy to run the unit tests on your code. This script can be run from the
top-level directory of the Ceph source tree by invoking:

  .. prompt:: bash $

     ./run-make-check.sh

You will need a minimum of 8GB of RAM and 32GB of free drive space for this
command to complete successfully on x86_64 architectures; other architectures
may have different requirements. Depending on your hardware, it can take from
twenty minutes to three hours to complete.


How unit tests are declared
---------------------------

Unit tests are declared in the ``CMakeLists.txt`` file, which is found in the
``./src`` directory. The ``add_ceph_test`` and ``add_ceph_unittest`` CMake
functions are used to declare unit tests.  ``add_ceph_test`` and
``add_ceph_unittest`` are themselves defined in
``./cmake/modules/AddCephTest.cmake``. 

Some unit tests are scripts and other unit tests are binaries that are
compiled during the build process.  

* ``add_ceph_test`` function - used to declare unit test scripts 
* ``add_ceph_unittest`` function - used for unit test binaries

Unit testing of CLI tools
-------------------------
Some of the CLI tools are tested using special files ending with the extension
``.t`` and stored under ``./src/test/cli``. These tests are run using a tool
called `cram`_ via a shell script called ``./src/test/run-cli-tests``.
`cram`_ tests that are not suitable for ``make check`` can also be run by
teuthology using the `cram task`_.

.. _`cram`: https://bitheap.org/cram/
.. _`cram task`: https://github.com/ceph/ceph/blob/master/qa/tasks/cram.py

Tox-based testing of Python modules
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Some of the Python modules in Ceph use `tox <https://tox.readthedocs.io/en/latest/>`_ 
to run their unit tests.

Most of these Python modules can be found in the directory ``./src/pybind/``.

Currently (December 2020) the following modules use **tox**:

* Cephadm (``./src/cephadm/tox.ini``)
* Ceph Manager Python API (``./src/pybind/mgr``)

  * ``./src/pybind/mgr/tox.ini``
    
  * ``./src/pybind/mgr/dashboard/tox.ini``

  * ``./src/pybind/tox.ini``

* Dashboard (``./src/pybind/mgr/dashboard``)
* Python common (``./src/python-common/tox.ini``)
* CephFS (``./src/tools/cephfs/tox.ini``)
* ceph-volume

  * ``./src/ceph-volume/tox.ini``

  * ``./src/ceph-volume/plugin/zfs/tox.ini``

  * ``./src/ceph-volume/ceph_volume/tests/functional/batch/tox.ini``

  * ``./src/ceph-volume/ceph_volume/tests/functional/simple/tox.ini``

  * ``./src/ceph-volume/ceph_volume/tests/functional/lvm/tox.ini``

Configuring Tox environments and tasks 
""""""""""""""""""""""""""""""""""""""
Most tox configurations support multiple environments and tasks. 

The list of environments and tasks that are supported is in the ``tox.ini``
file, under ``envlist``. For example, here are the first three lines of
``./src/cephadm/tox.ini``::

   [tox]
   envlist = py3, mypy
   skipsdist=true

In this example, the ``Python 3`` and ``mypy`` environments are specified.

The list of environments can be retrieved with the following command:

  .. prompt:: bash $

     tox --list

Or:

  .. prompt:: bash $

     tox -l

Running Tox
"""""""""""
To run **tox**, just execute ``tox`` in the directory containing
``tox.ini``.  If you do not specify any environments (for example, ``-e
$env1,$env2``), then ``tox`` will run all environments. Jenkins will run
``tox`` by executing ``./src/script/run_tox.sh``.

Here are some examples from Ceph Dashboard that show how to specify different
environments and run options::

  ## Run Python 2+3 tests+lint commands:
  $ tox -e py27,py3,lint,check

  ## Run Python 3 tests+lint commands:
  $ tox -e py3,lint,check

  ## To run it as Jenkins would:  
  $ ../../../script/run_tox.sh --tox-env py3,lint,check

Manager core unit tests
"""""""""""""""""""""""

Currently only doctests_ inside ``mgr_util.py`` are run.

To add more files to be tested inside the core of the manager, open the
``tox.ini`` file and add the files to be tested  at the end of the line that
includes ``mgr_util.py``.

.. _doctests: https://docs.python.org/3/library/doctest.html

Unit test caveats
-----------------

#. Unlike the various Ceph daemons and ``ceph-fuse``, the unit tests are
   linked against the default memory allocator (glibc) unless they are
   explicitly linked against something else. This enables tools such as
   **valgrind** to be used in the tests.

#. Google Test unit testing library hides the client output from the shell.
   In order to debug the client after setting the desired debug level
   (e.g ``ceph config set client debug_rbd 20``), the debug log file can
   be found at ``build/out/client.admin.<pid>.log``.
   This can also be handy when examining teuthology failed unit test
   jobs, the job's debug level can be set at the relevant yaml file.

.. _make check:
.. _teuthology framework: https://github.com/ceph/teuthology
