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


VIRTUAL MACHINE SUPPORT
=======================

For OpenStack support, see :ref:`openstack-backend`

Teuthology also supports virtual machines, which can function like
physical machines but differ in the following ways:

VPSHOST:
--------
The following description is based on the Red Hat lab used by the Ceph
development and quality assurance teams.

The teuthology database of available machines contains a vpshost field.
For physical machines, this value is null. For virtual machines, this entry
is the name of the physical machine that that virtual machine resides on.

There are fixed "slots" for virtual machines that appear in the teuthology
database.  These slots have a machine type of vps and can be locked like
any other machine.  The existence of a vpshost field is how teuthology
knows whether or not a database entry represents a physical or a virtual
machine.

In order to get the right virtual machine associations, the following needs
to be set in ~/.config/libvirt/libvirt.conf or for some older versions
of libvirt (like ubuntu precise) in ~/.libvirt/libvirt.conf::

    uri_aliases = [
        'mira001=qemu+ssh://ubuntu@mira001.front.sepia.ceph.com/system?no_tty=1',
        'mira003=qemu+ssh://ubuntu@mira003.front.sepia.ceph.com/system?no_tty=1',
        'mira004=qemu+ssh://ubuntu@mira004.front.sepia.ceph.com/system?no_tty=1',
        'mira006=qemu+ssh://ubuntu@mira006.front.sepia.ceph.com/system?no_tty=1',
        'mira007=qemu+ssh://ubuntu@mira007.front.sepia.ceph.com/system?no_tty=1',
        'mira008=qemu+ssh://ubuntu@mira008.front.sepia.ceph.com/system?no_tty=1',
        'mira009=qemu+ssh://ubuntu@mira009.front.sepia.ceph.com/system?no_tty=1',
        'mira010=qemu+ssh://ubuntu@mira010.front.sepia.ceph.com/system?no_tty=1',
        'mira011=qemu+ssh://ubuntu@mira011.front.sepia.ceph.com/system?no_tty=1',
        'mira013=qemu+ssh://ubuntu@mira013.front.sepia.ceph.com/system?no_tty=1',
        'mira014=qemu+ssh://ubuntu@mira014.front.sepia.ceph.com/system?no_tty=1',
        'mira015=qemu+ssh://ubuntu@mira015.front.sepia.ceph.com/system?no_tty=1',
        'mira017=qemu+ssh://ubuntu@mira017.front.sepia.ceph.com/system?no_tty=1',
        'mira018=qemu+ssh://ubuntu@mira018.front.sepia.ceph.com/system?no_tty=1',
        'mira020=qemu+ssh://ubuntu@mira020.front.sepia.ceph.com/system?no_tty=1',
        'mira024=qemu+ssh://ubuntu@mira024.front.sepia.ceph.com/system?no_tty=1',
        'mira029=qemu+ssh://ubuntu@mira029.front.sepia.ceph.com/system?no_tty=1',
        'mira036=qemu+ssh://ubuntu@mira036.front.sepia.ceph.com/system?no_tty=1',
        'mira043=qemu+ssh://ubuntu@mira043.front.sepia.ceph.com/system?no_tty=1',
        'mira044=qemu+ssh://ubuntu@mira044.front.sepia.ceph.com/system?no_tty=1',
        'mira074=qemu+ssh://ubuntu@mira074.front.sepia.ceph.com/system?no_tty=1',
        'mira079=qemu+ssh://ubuntu@mira079.front.sepia.ceph.com/system?no_tty=1',
        'mira081=qemu+ssh://ubuntu@mira081.front.sepia.ceph.com/system?no_tty=1',
        'mira091=qemu+ssh://ubuntu@mira091.front.sepia.ceph.com/system?no_tty=1',
        'mira098=qemu+ssh://ubuntu@mira098.front.sepia.ceph.com/system?no_tty=1',
        'vercoi01=qemu+ssh://ubuntu@vercoi01.front.sepia.ceph.com/system?no_tty=1',
        'vercoi02=qemu+ssh://ubuntu@vercoi02.front.sepia.ceph.com/system?no_tty=1',
        'vercoi03=qemu+ssh://ubuntu@vercoi03.front.sepia.ceph.com/system?no_tty=1',
        'vercoi04=qemu+ssh://ubuntu@vercoi04.front.sepia.ceph.com/system?no_tty=1',
        'vercoi05=qemu+ssh://ubuntu@vercoi05.front.sepia.ceph.com/system?no_tty=1',
        'vercoi06=qemu+ssh://ubuntu@vercoi06.front.sepia.ceph.com/system?no_tty=1',
        'vercoi07=qemu+ssh://ubuntu@vercoi07.front.sepia.ceph.com/system?no_tty=1',
        'vercoi08=qemu+ssh://ubuntu@vercoi08.front.sepia.ceph.com/system?no_tty=1',
        'senta01=qemu+ssh://ubuntu@senta01.front.sepia.ceph.com/system?no_tty=1',
        'senta02=qemu+ssh://ubuntu@senta02.front.sepia.ceph.com/system?no_tty=1',
        'senta03=qemu+ssh://ubuntu@senta03.front.sepia.ceph.com/system?no_tty=1',
        'senta04=qemu+ssh://ubuntu@senta04.front.sepia.ceph.com/system?no_tty=1',
    ]

DOWNBURST:
----------

When a virtual machine is locked, downburst is run on that machine to install a
new image.  This allows the user to set different virtual OSes to be installed
on the newly created virtual machine.  Currently the default virtual machine is
ubuntu (precise).  A different vm installation can be set using the
``--os-type`` and ``--os-version`` options in ``teuthology.lock``.

When a virtual machine is unlocked, downburst destroys the image on the
machine.

Temporary yaml files are used to downburst a virtual machine.  A typical
yaml file will look like this::

    downburst:
      cpus: 1
      disk-size: 30G
      distro: centos
      networks:
      - {source: front}
      ram: 4G

These values are used by downburst to create the virtual machine.

When locking a file, a downburst meta-data yaml file can be specified by using
the downburst-conf parameter on the command line.

To find the downburst executable, teuthology first checks the PATH environment
variable.  If not defined, teuthology next checks for
src/downburst/virtualenv/bin/downburst executables in the user's home
directory, /home/ubuntu, and /home/teuthology.  This can all be overridden if
the user specifies a downburst field in the user's .teuthology.yaml file.

HOST KEYS:
----------

Because teuthology reinstalls a new machine, a new hostkey is generated.  After
locking, once a connection is established to the new machine,
``teuthology-lock`` with the ``--list`` or ``--list-targets`` options will
display the new keys.  When vps machines are locked using the ``--lock-many``
option, a message is displayed indicating that ``--list-targets`` should be run
later.

ASSUMPTIONS:
------------

It is assumed that downburst is on the user's ``$PATH``.


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
