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

	sudo apt-get install python-virtualenv python-pip

and then run::

	./bootstrap

You can run Teuthology's (and Orchestra's) internal unit tests with::

	./virtualenv/bin/nosetests orchestra teuthology


Test configuration
==================

An integration test run takes three items of configuration:

- ``targets``: what hosts to run on; this is a list of entries like
  "username@hostname.example.com"
- ``roles``: how to use the hosts; this is a list of lists, where each
  entry lists all the roles to be run on a single host; for example, a
  single entry might say ``[mon.1, osd.1]``
- ``tasks``: how to set up the cluster and what tests to run on it;
  see below for examples

The format for this configuration is `YAML <http://yaml.org/>`__, a
structured data format that is still human-readable and editable.

For example, a full config for a test run that sets up a three-machine
cluster, mounts Ceph via ``cfuse``, and leaves you at an interactive
Python prompt for manual exploration (and enabling you to SSH in to
the nodes & use the live cluster ad hoc), might look like this::

	roles:
	- [mon.0, mds.0, osd.0]
	- [mon.1, osd.1]
	- [mon.2, client.0]
	targets:
	- ubuntu@host07.example.com
	- ubuntu@host08.example.com
	- ubuntu@host09.example.com
	tasks:
	- ceph:
	- cfuse: [client.0]
	- interactive:

The number of entries under ``roles`` and ``targets`` must match.

Note the colon after every task name in the ``tasks`` section.

You need to be able to SSH in to the listed targets without
passphrases, and the remote user needs to have passphraseless `sudo`
access.

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

Right now there is no automatic machine allocation and locking
support.

For the `sepia` cluster, use the Autotest web UI, lock the hosts you
intend to use, and write a ``targets.yaml`` file yourself.

Later, a utility will be written to create a similar yaml with as many
hosts as you request, while taking care of the locking. (TODO)


Tasks
=====

A task is a Python module in the ``teuthology.task`` package, with a
callable named ``task``. It gets the following arguments:

- ``ctx``: a context that is available through the lifetime of the
  test run, and has useful attributes such as ``cluster``, letting the
  task access the remote hosts. Tasks can also store their internal
  state here. (TODO beware namespace collisions.)
- ``config``: the data structure after the colon in the config file,
  e.g. for the above ``cfuse`` example, it would be a list like
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
