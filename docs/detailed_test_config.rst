.. _detailed_test_config:

===========================
Detailed Test Configuration
===========================

Test configuration
==================

An integration test run takes three items of configuration:

- ``targets``: what hosts to run on; this is a dictionary mapping
  hosts to ssh host keys, like:
  "username@hostname.example.com: ssh-rsa long_hostkey_here"
  It is possible to configure your installation so that if the targets line
  and host keys are omitted and teuthology is run with the --lock option,
  then teuthology will grab machines from a pool of available
  test machines.
- ``roles``: how to use the hosts; this is a list of lists, where each
  entry lists all the roles to be run on a single host. For example, a
  single entry might say ``[mon.1, osd.1]``.
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

Note the colon after every task name in the ``tasks`` section. Also note the
dashes before each task. This is the YAML syntax for an ordered list and
specifies the order in which tasks are executed.

The ``install`` task needs to precede all other tasks.

The listed targets need resolvable hostnames. If you do not have a DNS server
running, you can add entries to ``/etc/hosts``. You also need to be able to SSH
in to the listed targets without passphrases, and the remote user needs to have
passwordless `sudo` access. Note that the ssh keys at the end of the
``targets`` entries are the public ssh keys for the hosts.  These are
located in /etc/ssh/ssh_host_rsa_key.pub

If you saved the above file as ``example.yaml``, you could run
teuthology on it like this::

    ./virtualenv/bin/teuthology example.yaml

You could also pass the ``-v`` option for more verbose execution. See
``teuthology --help`` for more options.


Multiple config files
---------------------

You can pass multiple files as arguments to teuthology. Each one
will be read as a config file, and their contents will be merged. This
allows you to share definitions of what a "simple 3 node cluster"
is. The source tree comes with ``roles/3-simple.yaml``, so we could
skip the ``roles`` section in the above ``example.yaml`` and then
run::

    ./virtualenv/bin/teuthology roles/3-simple.yaml example.yaml


Reserving target machines
-------------------------

Teuthology automatically locks nodes for you if you specify the
``--lock`` option. Without this option, you must specify machines to
run on in a ``targets.yaml`` file, and lock them using
teuthology-lock.

Note that the default owner of a machine is of the form: USER@HOST where USER
is the user who issued the lock command and host is the machine on which the
lock command was run.

You can override this with the ``--owner`` option when running
teuthology or teuthology-lock.

With teuthology-lock you can also add a description, so you can
remember which tests you were running. This can be done when
locking or unlocking machines, or as a separate action with the
``--update`` option. To lock 3 machines and set a description, run::

    ./virtualenv/bin/teuthology-lock --lock-many 3 --desc 'test foo'

If machines become unusable for some reason, you can mark them down::

    ./virtualenv/bin/teuthology-lock --update --status down machine1 machine2

To see the status of all machines, use the ``--list`` option. This can
be restricted to particular machines as well::

    ./virtualenv/bin/teuthology-lock --list machine1 machine2


Choosing machines for a job
---------------------------

It is possible to run jobs against machines of one or more  ``machine_type``
values. It is also possible to tell ``teuthology`` to only select those
machines which match the following criteria specified in the job's YAML:

* ``os_type`` (e.g. 'rhel', 'ubuntu')
* ``os_version`` (e.g. '7.0', '14.04')
* ``arch`` (e.g. 'x86_64')


Tasks
=====

A task is a Python module in the ``teuthology.task`` package, with a
callable named ``task``. It gets the following arguments:

- ``ctx``: a context that is available through the lifetime of the
  test run, and has useful attributes such as ``cluster``, letting the
  task access the remote hosts. Tasks can also store their internal
  state here. (TODO beware of namespace collisions.)
- ``config``: the data structure after the colon in the config file,
  e.g. for the above ``ceph-fuse`` example, it would be a list like
  ``["client.0"]``.

Tasks can be simple functions, called once in the order they are
listed in ``tasks``. But sometimes it makes sense for a task to be
able to clean up after itself: for example, unmounting the filesystem
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

* ``ansible``: Run the ansible task.
* ``install``: by default, the install task goes to gitbuilder and installs the
  results of the latest build. You can, however, add additional parameters to
  the test configuration to cause it to install any branch, SHA, archive or
  URL. The following are valid parameters.

  - ``branch``: specify a branch (firefly, giant...)

  - ``flavor``: specify a flavor (next, unstable...). Flavors can be thought of
    as subsets of branches.  Sometimes (unstable, for example) they may have a
    predefined meaning.

  - ``project``: specify a project (ceph, samba...)

  - ``sha1``: install the build with this sha1 value.

  - ``tag``: specify a tag/identifying text for this build (v47.2, v48.1...)

* ``ceph``: Bring up Ceph

* ``overrides``: override behavior. Typically, this includes sub-tasks being
  overridden. Overrides technically is not a task (there is no 'def task' in
  an overrides.py file), but from a user's standpoint can be described as
  behaving like one.
  Sub-tasks can nest further information.  For example, overrides
  of install tasks are project specific, so the following section of a yaml
  file would cause all ceph installations to default to using the jewel
  branch::

    overrides:
      install:
        ceph:
          branch: jewel

* ``workunit``: workunits are a way of grouping tasks and behavior on targets.
* ``sequential``: group the sub-tasks into a unit where the sub-tasks run
  sequentially as listed.
* ``parallel``: group the sub-tasks into a unit where the sub-tasks all run in
  parallel.

Sequential and parallel tasks can be nested.  Tasks run sequentially unless
specified otherwise.

The above list is a very incomplete description of the tasks available on
teuthology. The teuthology/task subdirectory contains the teuthology-specific 
python files that implement tasks.

Extra tasks used by teuthology can be found in ceph-qa-suite/tasks.  These
tasks are not needed for teuthology to run, but do test specific independent
features.  A user who wants to define a test for a new feature can implement
new tasks in this directory.

Many of these tasks are used to run python scripts that are defined in the
ceph/ceph-qa-suite.

If machines were locked as part of the run (with the --lock switch),
teuthology normally leaves them locked when there is any task failure
for investigation of the machine state.  When developing new teuthology
tasks, sometimes this behavior is not useful.  The ``unlock_on_failure``
global option can be set to true to make the unlocking happen unconditionally.

Troubleshooting
===============

Sometimes when a bug triggers, instead of automatic cleanup, you want
to explore the system as is. Adding a top-level::

    interactive-on-error: true

as a config file for teuthology will make that possible. With that
option, any *task* that fails, will have the ``interactive`` task
called after it. This means that before any cleanup happens, you get a
chance to inspect the system -- both through Teuthology and via extra
SSH connections -- and the cleanup completes only when you choose so.
Just exit the interactive Python session to continue the cleanup.

Interactive task facilities
===========================

The ``interactive`` task presents a prompt for you to interact with the
teuthology configuration.  The ``ctx`` variable is available to explore,
and a ``pprint.PrettyPrinter().pprint`` object is added for convenience as
``pp``, so you can do things like pp(dict-of-interest) to see a formatted
view of the dict.

This is also useful to pause the execution of the test between two tasks,
either to perform ad hoc operations, or to examine the state of the cluster.
Hit ``control-D`` to continue when done.

You need to nest ``interactive`` underneath ``tasks`` in your config. You
can have has many ``interactive`` tasks as needed in your task list.

An example::

    tasks:
    - ceph:
    - interactive:

Test Sandbox Directory
======================

Teuthology currently places most test files and mount points in a
sandbox directory, defaulting to ``/home/$USER/cephtest``.  To change
the location of the sandbox directory, the following option can be
specified in ``$HOME/.teuthology.yaml``::

    test_path: <directory>
