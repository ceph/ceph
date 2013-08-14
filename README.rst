==================================================
 `Teuthology` -- The Ceph integration test runner
==================================================

The Ceph project needs automated tests. Because Ceph is a highly
distributed system, and has active kernel development, its testing
requirements are quite different from e.g. typical LAMP web
applications. Nothing out there seemed to handle our requirements,
so we wrote our own framework, called `Teuthology`.


Overview
========

Teuthology runs a given set of Python functions (`tasks`), with an SSH
connection to every host participating in the test. The SSH connection
uses `Paramiko <http://www.lag.net/paramiko/>`__, a native Python
client for the SSH2 protocol, and this allows us to e.g. run multiple
commands inside a single SSH connection, to speed up test
execution. Tests can use `gevent <http://www.gevent.org/>`__ to
perform actions concurrently or in the background.


Build
=====

Teuthology uses several Python packages that are not in the standard
library. To make the dependencies easier to get right, we use a
`virtualenv` to manage them. To get started, ensure you have the
``virtualenv`` and ``pip`` programs installed; e.g. on Debian/Ubuntu::

	sudo apt-get install python-dev python-virtualenv python-pip libevent-dev

and then run::

	./bootstrap

You can run Teuthology's internal unit tests with::

	./virtualenv/bin/nosetests


Test configuration
==================

An integration test run takes three items of configuration:

- ``targets``: what hosts to run on; this is a dictionary mapping
  hosts to ssh host keys, like:
  "username@hostname.example.com: ssh-rsa long_hostkey_here"
- ``roles``: how to use the hosts; this is a list of lists, where each
  entry lists all the roles to be run on a single host; for example, a
  single entry might say ``[mon.1, osd.1]``
- ``tasks``: how to set up the cluster and what tests to run on it;
  see below for examples

The format for this configuration is `YAML <http://yaml.org/>`__, a
structured data format that is still human-readable and editable.

For example, a full config for a test run that sets up a three-machine
cluster, mounts Ceph via ``ceph-fuse``, and leaves you at an interactive
Python prompt for manual exploration (and enabling you to SSH in to
the nodes & use the live cluster ad hoc), might look like this::

	roles:
	- [mon.0, mds.0, osd.0]
	- [mon.1, osd.1]
	- [mon.2, client.0]
	targets:
	  ubuntu@host07.example.com: ssh-rsa host07_ssh_key
	  ubuntu@host08.example.com: ssh-rsa host08_ssh_key
	  ubuntu@host09.example.com: ssh-rsa host09_ssh_key
	tasks:
	- install:
	- ceph:
	- ceph-fuse: [client.0]
	- interactive:

The number of entries under ``roles`` and ``targets`` must match.

Note the colon after every task name in the ``tasks`` section.

The ``install`` task needs to precede all other tasks.

You need to be able to SSH in to the listed targets without
passphrases, and the remote user needs to have passphraseless `sudo`
access. Note that the ssh keys at the end of the ``targets``
entries are the public ssh keys for the hosts.
On Ubuntu, these are located at /etc/ssh/ssh_host_rsa_key.pub

If you'd save the above file as ``example.yaml``, you could run
teuthology on it by saying::

	./virtualenv/bin/teuthology example.yaml

You can also pass the ``-v`` option, for more verbose execution. See
``teuthology --help`` for more.


Multiple config files
---------------------

You can pass multiple files as arguments to ``teuthology``. Each one
will be read as a config file, and their contents will be merged. This
allows you to e.g. share definitions of what a "simple 3 node cluster"
is. The source tree comes with ``roles/3-simple.yaml``, so we could
skip the ``roles`` section in the above ``example.yaml`` and then
run::

	./virtualenv/bin/teuthology roles/3-simple.yaml example.yaml


Reserving target machines
-------------------------

Before locking machines will work, you must create a .teuthology.yaml
file in your home directory that sets a lock_server, i.e.::

	lock_server: http://host.example.com:8080/lock

Teuthology automatically locks nodes for you if you specify the
``--lock`` option. Without this option, you must specify machines to
run on in a ``targets.yaml`` file, and lock them using
teuthology-lock.

Note that the default owner of a machine is ``USER@HOST``.
You can override this with the ``--owner`` option when running
teuthology or teuthology-lock.

With teuthology-lock, you can also add a description, so you can
remember which tests you were running on them. This can be done when
locking or unlocking machines, or as a separate action with the
``--update`` option. To lock 3 machines and set a description, run::

	./virtualenv/bin/teuthology-lock --lock-many 3 --desc 'test foo'

If machines become unusable for some reason, you can mark them down::

	./virtualenv/bin/teuthology-lock --update --status down machine1 machine2

To see the status of all machines, use the ``--list`` option. This can
be restricted to particular machines as well::

	./virtualenv/bin/teuthology-lock --list machine1 machine2


Tasks
=====

A task is a Python module in the ``teuthology.task`` package, with a
callable named ``task``. It gets the following arguments:

- ``ctx``: a context that is available through the lifetime of the
  test run, and has useful attributes such as ``cluster``, letting the
  task access the remote hosts. Tasks can also store their internal
  state here. (TODO beware namespace collisions.)
- ``config``: the data structure after the colon in the config file,
  e.g. for the above ``ceph-fuse`` example, it would be a list like
  ``["client.0"]``.

Tasks can be simple functions, called once in the order they are
listed in ``tasks``. But sometimes, it makes sense for a task to be
able to clean up after itself; for example, unmounting the filesystem
after a test run. A task callable that returns a Python `context
manager
<http://docs.python.org/library/stdtypes.html#typecontextmanager>`__
will have the manager added to a stack, and the stack will be unwound
at the end of the run. This means the cleanup actions are run in
reverse order, both on success and failure. A nice way of writing
context managers is the ``contextlib.contextmanager`` decorator; look
for that string in the existing tasks to see examples, and note where
they use ``yield``.

Further details on some of the more complex tasks such as install or workunit
can be obtained via python help. For example::

    >>> import teuthology.task.workunit
    >>> help(teuthology.task.workunit)

displays a page of more documentation and more concrete examples.

Some of the more important / commonly used tasks include:

* ``chef``: Run the chef task.
* ``install``: by default, the install task goes to gitbuilder and installs the results of the latest build. You can, however, add additional parameters to the test configuration to cause it to install any branch, SHA, archive or URL. The following are valid parameters.

- ``branch``: specify a branch (bobtail, cuttlefish...)
- ``flavor``: specify a flavor (next, unstable...). Flavors can be thought of as
  subsets of branches.  Sometimes (unstable, for example) they may have
  a predefined meaning.
- ``project``: specify a project (ceph, samba...)
- ``sha1``: install the build with this sha1 value.
- ``tag``: specify a tag/identifying text for this build (v47.2, v48.1...)
* ``ceph``: Bring up Ceph

* ``overrides``: override behavior. Typically, this includes sub-tasks being overridden. Sub-tasks can nest further information.  For example, overrides of install tasks are project specific, so the following section of a yaml file would cause all ceph installation to default into using the cuttlefish branch::

    overrides:
        install:
            ceph:
                branch: cuttlefish

* ``workunit``: workunits are a way of grouping tasks and behavior on targets.
* ``sequential``: group the sub-tasks into a unit where the sub-tasks run sequentially as listed.
* ``parallel``: group the sub-tasks into a unit where the sub-task all run in parallel.

Sequential and parallel tasks can be nested.  Tasks run sequentially if not specified.

The above list is a very incomplete description of the tasks available on
teuthology. The teuthology/task subdirectory contains all the python files
that implement tasks.
Many of these tasks are used to run shell scripts that are defined in the
ceph/ceph-qa-suite.

Troubleshooting
===============

Sometimes when a bug triggers, instead of automatic cleanup, you want
to explore the system as is. Adding a top-level::

	interactive-on-error: true

as a config file for ``teuthology`` will make that possible. With that
option, any *task* that fails, will have the ``interactive`` task
called after it. This means that before any cleanup happens, you get a
chance to inspect the system -- both through Teuthology and via extra
SSH connections -- and the cleanup completes only when you choose so.
Just exit the interactive Python session to continue the cleanup.

Note that this only catches exceptions *between* the tasks. If a task
calls multiple subtasks, e.g. with ``contextutil.nested``, those
cleanups *will* be performed. Later on, we can let tasks communicate
the subtasks they wish to invoke to the top-level runner, avoiding
this issue.

Test Sandbox Directory
======================

Teuthology currently places most test files and mount points in a sandbox
directory, defaulting to ``/tmp/cephtest/{rundir}``.  The ``{rundir}`` is the
name of the run (as given by ``--name``) or if no name is specified,
``user@host-timestamp`` is used.  To change the location of the sandbox
directory, the following options can be specified in
``$HOME/.teuthology.yaml``::

	base_test_dir: <directory>

The ``base_test_dir`` option will set the base directory to use for the individual
run directories.  If not specified, this defaults to: ``/tmp/cephtest``.

	test_path: <directory>

The ``test_path`` option will set the complete path to use for the test directory.
This allows for the old behavior, where ``/tmp/cephtest`` was used as the sandbox
directory.


VIRTUAL MACHINE SUPPORT
=======================

Teuthology also supports virtual machines, which can function like
physical machines but differ in the following ways:

VPSHOST:
--------

A new entry, vpshost, has been added to the teuthology database of
available machines.  For physical machines, this value is null. For
virtual machines, this entry is the name of the physical machine that
that virtual machine resides on.

There are fixed "slots" for virtual machines that appear in the teuthology
database.  These slots have a machine type of vps and can be locked like
any other machine.  The existence of a vpshost field is how teuthology
knows whether or not a database entry represents a physical or a virtual
machine.

The following needs to be set in ~/.libvirt/libvirt.conf in order to get the
right virtual machine associations for the Inktank lab::

    uri_aliases = [
       'mira001=qemu+ssh://ubuntu@mira001.front.sepia.ceph.com/system?no_tty',
       'mira003=qemu+ssh://ubuntu@mira003.front.sepia.ceph.com/system?no_tty',
       'mira004=qemu+ssh://ubuntu@mira004.front.sepia.ceph.com/system?no_tty',
       'mira006=qemu+ssh://ubuntu@mira006.front.sepia.ceph.com/system?no_tty',
       'mira007=qemu+ssh://ubuntu@mira007.front.sepia.ceph.com/system?no_tty',
       'mira008=qemu+ssh://ubuntu@mira008.front.sepia.ceph.com/system?no_tty',
       'mira009=qemu+ssh://ubuntu@mira009.front.sepia.ceph.com/system?no_tty',
       'mira010=qemu+ssh://ubuntu@mira010.front.sepia.ceph.com/system?no_tty',
       'mira011=qemu+ssh://ubuntu@mira011.front.sepia.ceph.com/system?no_tty',
       'mira013=qemu+ssh://ubuntu@mira013.front.sepia.ceph.com/system?no_tty',
       'mira014=qemu+ssh://ubuntu@mira014.front.sepia.ceph.com/system?no_tty',
       'mira015=qemu+ssh://ubuntu@mira015.front.sepia.ceph.com/system?no_tty',
       'mira017=qemu+ssh://ubuntu@mira017.front.sepia.ceph.com/system?no_tty',
       'mira018=qemu+ssh://ubuntu@mira018.front.sepia.ceph.com/system?no_tty',
       'mira020=qemu+ssh://ubuntu@mira020.front.sepia.ceph.com/system?no_tty',
       'vercoi01=qemu+ssh://ubuntu@vercoi01.front.sepia.ceph.com/system?no_tty',
       'vercoi02=qemu+ssh://ubuntu@vercoi02.front.sepia.ceph.com/system?no_tty',
       'vercoi03=qemu+ssh://ubuntu@vercoi03.front.sepia.ceph.com/system?no_tty',
       'vercoi04=qemu+ssh://ubuntu@vercoi04.front.sepia.ceph.com/system?no_tty',
       'vercoi05=qemu+ssh://ubuntu@vercoi05.front.sepia.ceph.com/system?no_tty',
       'vercoi06=qemu+ssh://ubuntu@vercoi06.front.sepia.ceph.com/system?no_tty',
       'vercoi07=qemu+ssh://ubuntu@vercoi07.front.sepia.ceph.com/system?no_tty',
       'vercoi08=qemu+ssh://ubuntu@vercoi08.front.sepia.ceph.com/system?no_tty',
       'senta01=qemu+ssh://ubuntu@senta01.front.sepia.ceph.com/system?no_tty',
       'senta02=qemu+ssh://ubuntu@senta02.front.sepia.ceph.com/system?no_tty',
       'senta03=qemu+ssh://ubuntu@senta03.front.sepia.ceph.com/system?no_tty',
       'senta04=qemu+ssh://ubuntu@senta04.front.sepia.ceph.com/system?no_tty',
       ]

DOWNBURST:
----------

When a virtual machine is locked, downburst is run on that machine to
install a new image.  This allows the user to set different virtual
OSes to be installed on the newly created virtual machine.  Currently
the default virtual machine is ubuntu (precise).  A different vm installation
can be set using the ``--os-type`` option in ``teuthology.lock``.

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

HOST KEYS:
----------

Because teuthology reinstalls a new machine, a new hostkey is generated.  After
locking, once a connection is established to the new machine,
``teuthology-lock`` with the ``--list`` or ``--list-targets`` options will
display the new keys.  When vps machines are locked using the ``--lock-many``
option, a message is displayed indicating that ``--list-targets`` should be run
later.

CEPH-QA-CHEF:
-------------

Once teuthology starts after a new vm is installed, teuthology
checks for the existence of ``/ceph-qa-ready``.  If this file is not
present, ``ceph-qa-chef`` is run when teuthology first comes up.

ASSUMPTIONS:
------------

It is assumed that downburst is on the user's ``$PATH``.


Test Suites
===========

Most of the current teuthology test suite execution scripts automatically
download their tests from the master branch of the appropriate github
repository.  People who want to run experimental test suites usually modify
the download method in the ``teuthology/task`` script to use some other branch
or repository. This should be generalized in later teuthology releases.
Teuthology QA suites can be found in ``src/ceph-qa-suite``. Make sure that this
directory exists in your source tree before running the test suites.

Each suite name is determined by the name of the directory in ``ceph-qa-suite``
that contains that suite. The directory contains subdirectories and yaml files,
which, when assembled, produce valid tests that can be run. The test suite
application generates combinations of these files and thus ends up running
a set of tests based off the data in the directory for the suite.

To run a suite, enter::

   ./schedule_suite.sh <suite> <ceph> <kernel> <email> <flavor> <teuth> <mtype> <template>

where:

* ``suite``: the name of the suite (the directory in ceph-qa-suite).
* ``ceph``: ceph branch to be used.
* ``kernel``: version of the kernel to be used.
* ``email``: email address to send the results to.
* ``flavor``: flavor of the test
* ``teuth``: version of teuthology to run
* ``mtype``: machine type of the run
* ``templates``: template file used for further modifying the suite (optional)

For example, consider::

     schedule_suite.sh rbd wip-fix cuttlefish bob.smith@foo.com master cuttlefish plana

The above command runs the rbd suite using wip-fix as the ceph branch,
a straight cuttlefish kernel, and the master flavor of cuttlefish teuthology.
It will run on plana machines.

In order for a queued task to be run, a teuthworker thread on
``teuthology.front.sepia.ceph.com`` needs to remove the task from the queue.
On ``teuthology.front.sepia.ceph.com``, run ``ps aux | grep teuthology-worker``
to view currently running tasks. If no processes are reading from the test
version that you are running, additonal teuthworker tasks need to be started.
To start these tasks:
* copy your build tree to ``/home/teuthworker`` on ``teuthology.front.sepia.ceph.com``.
* Give it a unique name (in this example, xxx)
* start up some number of worker threads (as many as machines you are testing
with, there are 60 running for the default queue)::

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
number of the pid of that test.  Each of these directory contains a copy of
the ``teuthology.log`` for that process.  Other information from the suite is
stored in files in the directory, and task-specific yaml files and other logs
are saved in the subdirectories.

