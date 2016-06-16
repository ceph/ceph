===================================================
`Teuthology` -- The Ceph integration test framework
===================================================

``teuthology`` is an automation framework for `Ceph
<https://github.com/ceph/ceph>`__, written in `Python
<https://www.python.org/>`__. It is used to run the vast majority of its tests
and was developed because the unique requirements of testing such a highly
distributed system with active kernel development meant that no other framework
existed that could do its job.

The name '`teuthology <http://en.wikipedia.org/wiki/Teuthology>`__' refers to the
study of cephalopods.


Overview
========

The general mode of operation of ``teuthology`` is to remotely orchestrate
operations on remote hosts over SSH, as implemented by `Paramiko
<http://www.lag.net/paramiko/>`__. A typical `job` consists of multiple nested
`tasks`, each of which perform operations on a remote host over the network.

When testing, it is common to group many `jobs` together to form a `test run`.

If you are new to teuthology and simply want to run existing tests, check out
:ref:`intro_testers`


Provided Utilities
==================
* ``teuthology`` - Run individual jobs
* ``teuthology-coverage`` - Analyze code coverage via lcov
* ``teuthology-kill`` - Kill running jobs or entire runs
* ``teuthology-lock`` - Lock, unlock, and update status of machines
* ``teuthology-ls`` - List job results by examining an archive directory
* ``teuthology-openstack`` - Use OpenStack backend (wrapper around ``teuthology-suite``)
* ``teuthology-nuke`` - Attempt to return a machine to a pristine state
* ``teuthology-queue`` - List, or delete, jobs in the queue
* ``teuthology-report`` - Submit test results to a web service (we use `paddles <https://github.com/ceph/paddles/>`__)
* ``teuthology-results`` - Examing a finished run and email results
* ``teuthology-schedule`` - Schedule a single job
* ``teuthology-suite`` - Schedule a full run based on a suite (see `suites` in `ceph-qa-suite <https://github.com/ceph/ceph-qa-suite>`__)
* ``teuthology-updatekeys`` - Update SSH host keys for a mchine
* ``teuthology-worker`` - Worker daemon to monitor the queue and execute jobs

For a description of the distinct services that utilities interact with see
:ref:`components`.

Installation
============

See :ref:`installation_and_setup`.


Infrastructure
==============

The examples in this document are based on the lab machine configuration used
by the Red Hat Ceph development and quality assurance teams 
(see :ref:`lab_setup`). Other instances of a Ceph Lab being used in a
development or testing environment may differ from these examples.


Detailed test configuration
===========================

See :ref:`detailed_test_config`.


Virtual Machine Support
=======================

For OpenStack support, see :ref:`openstack-backend`

For 'vps' support using `downburst <https://github.com/ceph/downburst>`__, see
:ref:`downburst_vms`


Test Suites
===========

Most of the current teuthology test suite execution scripts automatically
download their tests from the master branch of the appropriate github
repository.  People who want to run experimental test suites usually modify the
download method in the ``teuthology/task`` script to use some other branch or
repository. This should be generalized in later teuthology releases.
Teuthology QA suites can be found in ``src/ceph-qa-suite``. Make sure that this
directory exists in your source tree before running the test suites.

Each suite name is determined by the name of the directory in ``ceph-qa-suite``
that contains that suite. The directory contains subdirectories and yaml files,
which, when assembled, produce valid tests that can be run. The test suite
application generates combinations of these files and thus ends up running a
set of tests based off the data in the directory for the suite.

To run a suite, enter::

    teuthology-suite -s <suite> [-c <ceph>] [-k <kernel>] [-e email] [-f flavor] [-t <teuth>] [-m <mtype>]

where:

* ``suite``: the name of the suite (the directory in ceph-qa-suite).
* ``ceph``: ceph branch to be used.
* ``kernel``: version of the kernel to be used.
* ``email``: email address to send the results to.
* ``flavor``: the kernel flavor to run against
* ``teuth``: version of teuthology to run
* ``mtype``: machine type of the run
* ``templates``: template file used for further modifying the suite (optional)

For example, consider::

     teuthology-suite -s rbd -c wip-fix -k cuttlefish -e bob.smith@foo.com -f basic -t cuttlefish -m plana

The above command runs the rbd suite using the wip-fix branch of ceph, the
cuttlefish kernel, with a 'basic' kernel flavor, and the teuthology
cuttlefish branch will be used.  It will run on plana machines and send an email
to bob.smith@foo.com when it's completed. For more details on
``teuthology-suite``, please consult the output of ``teuthology-suite --help``.

In order for a queued task to be run, a teuthworker thread on
``teuthology.front.sepia.ceph.com`` needs to remove the task from the queue.
On ``teuthology.front.sepia.ceph.com``, run ``ps aux | grep teuthology-worker``
to view currently running tasks. If no processes are reading from the test
version that you are running, additonal teuthworker tasks need to be started.
To start these tasks:

* copy your build tree to ``/home/teuthworker`` on ``teuthology.front.sepia.ceph.com``.
* Give it a unique name (in this example, xxx)
* start up some number of worker threads (as many as machines you are testing with, there are 60 running for the default queue)::

    /home/virtualenv/bin/python
    /var/lib/teuthworker/xxx/virtualenv/bin/teuthworker
    /var/lib/teuthworker/archive --tube xxx
    --log-dir /var/lib/teuthworker/archive/worker_logs

    Note: The threads on teuthology.front.sepia.ceph.com are started via
    ~/teuthworker/start.sh.  You can use that file as a model for your
    own threads, or add to this file if you want your threads to be
    more permanent.

Once the suite completes, an email message is sent to the users specified, and
a large amount of information is left on ``teuthology.front.sepia.ceph.com`` in
``/var/lib/teuthworker/archive``.

This is symbolically linked to /a for convenience. A new directory is created
whose name consists of a concatenation of the date and time that the suite was
started, the name of the suite, the ceph branch tested, the kernel used, and
the flavor. For every test run there is a directory whose name is the pid
number of the pid of that test.  Each of these directory contains a copy of the
``teuthology.log`` for that process.  Other information from the suite is
stored in files in the directory, and task-specific yaml files and other logs
are saved in the subdirectories.

These logs are also publically available at
``http://qa-proxy.ceph.com/teuthology/``.
